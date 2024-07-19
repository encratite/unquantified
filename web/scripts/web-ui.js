import { UnquantifiedMode } from "./highlight-rules.js";
import {
	ScriptingEngine,
	SecondsPerDay,
	TimeParameter,
	TimeFrame,
	Offset,
	Symbol,
	SymbolArray,
	String,
	Keyword
} from "./engine.js";

const LocalStorageKey = "unquantified";

export class WebUi {
	constructor() {
		this.content = null;
		this.editor = null;
		this.editorContainer = null;
		this.engine = null;
	}

	async initialize() {
		this.content = document.getElementById("content");
		this.createEditor();
		const callHandlers = {
			candle: this.plotCandlestick.bind(this),
			plot: this.plotLine.bind(this),
			correlation: this.correlation.bind(this),
			timezone: this.timezone.bind(this)
		};
		this.engine = new ScriptingEngine(callHandlers);
		await this.engine.initialize();
		const data = this.getLocalStorageData();
		if (data.lastScript != null) {
			this.editor.setValue(data.lastScript, 1);
		}
		if (data.timezone != null) {
			this.engine.setTimezone(data.timezone);
		}
		if (data.variables != null) {
			this.engine.deserializeVariables(data.variables);
		}
	}

	invoke(url, request) {
		const json = JSON.stringify(request);
		const options = {
			method: "POST",
			headers: {
				"Content-Type": "application/json"
			},
			body: json
		};
		return new Promise((resolve, reject) => {
			fetch(url, options)
				.then(response => response.json())
				.then(json => {
					if (json.error == null) {
						resolve(json);
					}
					else {
						reject(json.error);
					}
				})
				.catch(reject);
		});
	}

	async getHistory(symbols, from, to, timeFrame) {
		const request = {
			symbols: symbols,
			from: from,
			to: to,
			timeFrame: timeFrame
		};
		const response = await this.invoke("/history", request);
		return response;
	}

	async getCorrelation(symbols, from, to) {
		const request = {
			symbols: symbols,
			from: from,
			to: to
		};
		const response = await this.invoke("/correlation", request);
		return response;
	}

	getTime(timeString) {
		const options = {
			setZone: true
		};
		const dateTime = luxon.DateTime.fromISO(timeString, options);
		return dateTime;
	}

	getDateFormat(dateTime, short) {
		const formatString = short ? "dd LLL yyyy" : "dd LLL yyyy HH:mm:ss";
		return dateTime.toFormat(formatString);
	}

	append(element) {
		this.content.appendChild(element);
	}

	createChart(history, isCandlestick, timeFrame) {
		const container = document.createElement("div");
		container.className = "plot";
		const canvas = document.createElement("canvas");
		container.appendChild(canvas);
		const button = document.createElement("button");
		button.textContent = "Reset zoom";
		container.appendChild(button);
		this.append(container);
		const context = canvas.getContext("2d");
		if (context === null) {
			throw new Error("Failed to create 2D context");
		}
		const titleCallback = tooltipItems => {
			const context = tooltipItems[0];
			const dateTime = context.raw.time;
			const short = timeFrame.value === SecondsPerDay;
			const title = this.getDateFormat(dateTime, short);
			return title;
		};
		const options = {
			options: {
				maintainAspectRatio: false,
				plugins: {
					zoom: {
						pan: {
							enabled: true
						},
						zoom: {
							drag: {
								enabled: true
							},
							mode: "x",
						},
					},
					legend: {
						position: "bottom"
					},
					tooltip: {
						callbacks: {
							title: titleCallback
						}
					}
				},
				transitions: {
					zoom: {
						animation: {
							duration: 0
						}
					}
				}
			}
		}
		if (isCandlestick) {
			options.type = "candlestick";
			options.data = {
				datasets: this.getCandlestickDatasets(history)
			};
		}
		else {
			const datasets = this.getLineDatasets(history);
			options.type = "line";
			options.data = {
				datasets: datasets
			};
			const innerOptions = options.options;
			innerOptions.scales = {
				x: {
					type: "timeseries",
					offset: true,
					ticks: {
						major: {
							enabled: true,
						},
						source: "data",
						maxRotation: 0,
						autoSkip: true,
						autoSkipPadding: 75,
						sampleSize: 100
					}
				},
				y: {
					type: "linear"
				}
			};
			innerOptions.pointStyle = false;
			innerOptions.borderJoinStyle = "bevel";
			innerOptions.pointHitRadius = 3;
			const multiMode = datasets.length > 1;
			if (multiMode) {
				innerOptions.scales.y.ticks = {
					callback: value => {
						return `${value.toFixed(1)}%`;
					}
				};
				const labelCallback = context => {
					return `${context.dataset.label}: ${context.raw.c} (${context.raw.y.toFixed(1)}%)`;
				};
				innerOptions.plugins.tooltip.callbacks.label = labelCallback;
			}
		}
		const chart = new Chart(context, options);
		button.onclick = _ => chart.resetZoom();
		$(container).resizable();
	}

	getCandlestickDatasets(history) {
		const tickers = Object.keys(history.tickers);
		if (tickers.length != 1) {
			throw new Error("Invalid ticker count");
		}
		const ticker = tickers[0];
		const records = history.tickers[ticker];
		const data = records.map(ohlc => {
			const dateTime = this.getTime(ohlc.time);
			return {
				x: dateTime.valueOf(),
				o: ohlc.open,
				h: ohlc.high,
				l: ohlc.low,
				c: ohlc.close,
				time: dateTime
			};
		});
		const datasets = [
			{
				label: ticker,
				data: data
			}
		];
		return datasets;
	}

	getLineDatasets(history) {
		const tickers = Object.keys(history.tickers);
		const multiMode = tickers.length > 1;
		const datasets = tickers
			.map(symbol => {
				const records = history.tickers[symbol];
				let firstClose = null;
				if (records.length > 0) {
					firstClose = records[0].close;
				}
				const data = records.map(ohlc => {
					let value;
					if (multiMode) {
						value = ohlc.close / firstClose * 100.0;
					}
					else {
						value = ohlc.close;
					}
					const dateTime = this.getTime(ohlc.time);
					return {
						x: dateTime.valueOf(),
						y: value,
						c: ohlc.close,
						time: dateTime
					};
				});
				return {
					label: symbol,
					data: data
				};
			});
		return datasets;
	}

	createEditor() {
		const container = document.createElement("div");
		container.className = "editor";
		container.onkeydown = this.onEditorKeyDown.bind(this);
		this.append(container);
		const editor = ace.edit(container);
		editor.setOptions({
			fontSize: "14px",
			useWorker: false,
			autoScrollEditorIntoView: true
		});
		editor.setShowPrintMargin(false);
		editor.setHighlightActiveLine(false);
		const mode = new UnquantifiedMode();
		editor.session.setMode(mode);
		editor.session.setUseWrapMode(true);
		editor.renderer.setShowGutter(false);
		const resizeEditor = () => {
			const screenLength = editor.getSession().getScreenLength();
			const scrollbarWidth = editor.renderer.scrollBar.getWidth();
			const newHeight = screenLength * editor.renderer.lineHeight + scrollbarWidth;
			container.style.height = newHeight.toString() + "px";
			editor.resize();
		};
		resizeEditor();
		editor.getSession().on("change", resizeEditor);
		editor.focus();
		editor.navigateFileEnd();
		this.editor = editor;
		this.editorContainer = container;
	}

	async onEditorKeyDown(event) {
		if (event.key === "Enter" && event.shiftKey) {
			event.preventDefault();
			const script = this.editor.getValue();	
			this.enableEditor(false);
			try {
				await this.engine.run(script);
				const data = this.getLocalStorageData();
				data.lastScript = script;
				data.variables = this.engine.serializeVariables();
				this.setLocalStorageData(data);
				this.disableHighlight();
				this.editorContainer.classList.add("read-only");
				this.createEditor();
				window.scrollTo(0, document.body.scrollHeight);
			}
			catch (error) {
				toastr.error(error, "Script Error");
				this.enableEditor(true);
			}
		}
	}

	getLocalStorageData() {
		const json = localStorage.getItem(LocalStorageKey);
		if (json == null) {
			return {};
		}
		const data = JSON.parse(json);
		return data;
	}

	setLocalStorageData(data) {
		const json = JSON.stringify(data);
		localStorage.setItem(LocalStorageKey, json);
	}

	enableEditor(enable) {
		const editor = this.editor;
		editor.session.setUseWorker(enable);
		editor.setReadOnly(!enable);
	}

	disableHighlight() {
		const editor = this.editor;
		editor.renderer.$cursorLayer.element.style.display = "none";
		editor.$highlightTagPending = true;
		editor.$highlightPending = true;
		const session = editor.session;
		session.removeMarker(session.$tagHighlight);
		session.$tagHighlight = null;
		session.removeMarker(session.$bracketHighlight);
		session.$bracketHighlight = null;
	}

	renderCorrelationMatrix(correlation, separators) {
		const container = document.createElement("div");
		container.className = "correlation";
		this.append(container);
		const table = this.createElement("table", container);
		const createRow = () => this.createElement("tr", table);
		const createCell = (text, row) => {
			const element = this.createElement("td", row);
			if (text != null) {
				element.textContent = text;
			}
			return element;
		};
		const firstRow = createRow();
		createCell(null, firstRow);
		const symbols = correlation.symbols;
		const setSeparatorStyle = (cell, i, top) => {
			if (separators != null && separators[i] === true) {
				cell.className = top ? "separator-top" : "separator-left";
			}
		};
		for (let i = 0; i < symbols.length; i++) {
			const symbol = symbols[i];
			const cell = createCell(symbol, firstRow);
			setSeparatorStyle(cell, i, true);
		}
		const chromaScale = chroma.scale("RdYlBu").padding([0, 0.07]);
		const correlationMin = -1;
		const correlationMax = 1;
		for (let i = 0; i < symbols.length; i++) {
			const symbol = symbols[i];
			const data = correlation.correlation[i];
			const row = createRow();
			const cell = createCell(symbol, row);
			setSeparatorStyle(cell, i, false);
			for (let j = 0; j < symbols.length; j++) {
				const coefficient = data[j];
				const cell = createCell(coefficient.toFixed(2), row);
				const scale = (coefficient - correlationMin) / (correlationMax - correlationMin);
				if (scale < 0 || scale > 1) {
					throw new Error("Invalid scale in correlation matrix");
				}
				const color = chromaScale(scale).hex();
				cell.style.backgroundColor = color;
			}
		}
		const from = this.getTime(correlation.from);
		const to = this.getTime(correlation.to);
		const fromToLabel = this.createElement("div", container);
		fromToLabel.textContent = `Showing data from ${this.getDateFormat(from, true)} to ${this.getDateFormat(to, true)}`;
	}

	createElement(tag, parent) {
		const element = document.createElement(tag);
		if (parent != null) {
			parent.appendChild(element);
		}
		return element;
	}

	async plot(callArguments, isCandlestick) {
		this.validateArgumentCount(callArguments, 1, 4);
		const symbolArgument = callArguments[0];
		const from = callArguments[1] || new TimeParameter(Keyword.First);
		const to = callArguments[2] || new TimeParameter(Keyword.Last);
		const timeFrame = callArguments[3] || new TimeFrame(SecondsPerDay);
		let symbols;
		if (isCandlestick) {
			if (!(symbolArgument instanceof Symbol)) {
				throw new Error("Invalid symbol data type");
			}
			symbols = [symbolArgument.getJsonValue()];
		}
		else {
			if (symbolArgument instanceof Symbol) {
				symbols = [symbolArgument.getJsonValue()];
			}
			else if (symbolArgument instanceof SymbolArray) {
				this.validateSymbols(symbolArgument);
				symbols = symbolArgument.getJsonValue();
			}
			else {
				throw new Error("Invalid symbol data type");
			}
		}
		this.validateFromTo(from, to);
		this.validateTimeFrame(timeFrame);
		const history = await this.getHistory(symbols, from.getJsonValue(), to.getJsonValue(), timeFrame.getJsonValue());
		this.createChart(history, isCandlestick, timeFrame);
	}

	async plotCandlestick(callArguments) {
		await this.plot(callArguments, true);
	}

	async plotLine(callArguments) {
		await this.plot(callArguments, false);
	}

	async correlation(callArguments) {
		this.validateArgumentCount(callArguments, 1, 3);
		const tickers = callArguments[0];
		const from = callArguments[1] || new TimeParameter(Keyword.First);
		const to = callArguments[2] || new TimeParameter(Keyword.Last);
		this.validateSymbols(tickers);
		this.validateFromTo(from, to);
		const response = await this.getCorrelation(tickers.getJsonValue(), from.getJsonValue(), to.getJsonValue());
		let separators = null;
		if (tickers instanceof SymbolArray) {
			separators = tickers.value.map(x => x.separator);
		}
		this.renderCorrelationMatrix(response.correlation, separators);
	}

	async timezone(callArguments) {
		this.validateArgumentCount(callArguments, 1, 1);
		const timezoneArgument = callArguments[0];
		if (!(timezoneArgument instanceof String)) {
			throw new Error("Invalid timezone data type");
		}
		const timezone = timezoneArgument.value;
		const data = this.getLocalStorageData();
		data.timezone = timezone;
		this.engine.setTimezone(timezone);
	}

	validateArgumentCount(callArguments, min, max) {
		if (callArguments.length < min || callArguments.length > max) {
			throw new Error("Invalid number of arguments");
		}
	}

	validateSymbols(tickers) {
		if (tickers instanceof SymbolArray) {
			for (const t of tickers.value) {
				if (!(t instanceof Symbol)) {
					throw new Error("Encountered an invalid data type in a ticker array");
				}
			}
		}
		else if (!(tickers instanceof Symbol)) {
			throw new Error("Invalid ticker data type");
		}
	}

	validateFromTo(from, to) {
		let dateTimeCount = 0;
		let offsetCount = 0;
		const validateDateTime = x => {
			if (x instanceof TimeParameter) {
				dateTimeCount++;
			}
		};
		const validateOffset = x => {
			if (x instanceof Offset) {
				offsetCount++;
			}
		};
		validateDateTime(from);
		validateDateTime(to);
		validateOffset(from);
		validateOffset(to);
		if (
			dateTimeCount + offsetCount < 2 ||
			dateTimeCount == 0
		) {
			throw new Error("Invalid from/to parameter types");
		}
	}

	validateTimeFrame(timeFrame) {
		if (
			!(timeFrame instanceof TimeFrame) ||
			timeFrame.value < 1 ||
			timeFrame.value > SecondsPerDay ||
			!Number.isInteger(timeFrame.value)
		) {
			throw new Error("Invalid time frame specified");
		}
	}
}