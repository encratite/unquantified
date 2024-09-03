import { UnquantifiedMode } from "./highlight-rules.js";
import {
	ScriptingEngine,
	TimeParameter,
	TimeFrame,
	Offset,
	Symbol,
	SymbolArray,
	String,
	Parameters,
	SECONDS_PER_DAY,
	Keyword
} from "./engine.js";

const LOCAL_STORAGE_KEY = "unquantified";

const ChartMode = {
	LINE: "line",
	CANDLESTICK: "candlestick",
	EQUITY_CURVE: "equityCurve"
};

export class WebUi {
	constructor() {
		this.content = null;
		this.editor = null;
		this.editorContainer = null;
		this.engine = null;
		this.history = [];
		this.historyIndex = null;
	}

	async initialize() {
		this.content = document.getElementById("content");
		this.createEditor();
		const callHandlers = {
			candle: this.plotCandlestick.bind(this),
			plot: this.plotLine.bind(this),
			correlation: this.correlation.bind(this),
			backtest: this.backtest.bind(this)
		};
		this.engine = new ScriptingEngine(callHandlers);
		await this.engine.initialize();
		const data = this.getLocalStorageData();
		if (data.lastScript != null) {
			this.editor.setValue(data.lastScript, 1);
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
						resolve(json.result);
					} else {
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

	async runBacktest(strategy, symbols, from, to, parameters, timeFrame) {
		const request = {
			strategy: strategy,
			symbols: symbols,
			from: from,
			to: to,
			parameters: parameters,
			timeFrame: timeFrame
		};
		const response = await this.invoke("/backtest", request);
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

	createChart(data, mode) {
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
		const options = this.getBaseChartOptions();
		if (mode === ChartMode.CANDLESTICK) {
			let datasets = this.getCandlestickDatasets(data);
			this.setTitleCallback(datasets, options, true);
			options.type = "candlestick";
			options.data = {
				datasets: datasets
			};
		} else if (mode === ChartMode.LINE) {
			const datasets = this.getLineDatasets(data);
			this.setChartOptions(datasets, options, true);
		} else if (mode === ChartMode.EQUITY_CURVE) {
			const datasets = this.getEquityCurveDaily(data.equityCurveDaily);
			this.setChartOptions(datasets, options, true);
			const form = document.createElement("form");
			form.className = "equity-curve";
			container.insertBefore(form, container.firstChild);
			const onChangeDaily = () => this.updateEquityCurve(chart, data, true);
			const onChangeTrades = () => this.updateEquityCurve(chart, data, false);
			this.createRadioButton("Equity curve (daily)", form, true, onChangeDaily);
			this.createRadioButton("Equity curve (by trade)", form, false, onChangeTrades);
		} else {
			throw new Error("Unknown chart mode specified");
		}
		const chart = new Chart(context, options);
		button.onclick = _ => chart.resetZoom();
		$(container).resizable();
	}

	updateEquityCurve(chart, data, daily) {
		let datasets;
		if (daily === true) {
			datasets = this.getEquityCurveDaily(data.equityCurveDaily);
		} else {
			datasets = this.getEquityCurveTrades(data.equityCurveTrades);
		}
		this.setChartOptions(datasets, chart.options, daily);
		chart.data.datasets = datasets;
		chart.update();
	}

	createRadioButton(labelText, container, checked, onChange) {
		const label = document.createElement("label");
		container.appendChild(label);
		const input = document.createElement("input");
		input.type = "radio";
		input.name = "mode";
		input.checked = checked;
		input.onchange = onChange;
		label.appendChild(input);
		const text = document.createTextNode(labelText);
		label.appendChild(text);
	}

	getCandlestickDatasets(tickerMap) {
		const tickers = Object.keys(tickerMap);
		if (tickers.length != 1) {
			throw new Error("Invalid ticker count");
		}
		const ticker = tickers[0];
		const records = tickerMap[ticker];
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

	getLineDatasets(tickerMap) {
		const tickers = Object.keys(tickerMap);
		const multiMode = tickers.length > 1;
		const datasets = tickers
			.map(symbol => {
				const records = tickerMap[symbol];
				let firstClose = null;
				if (records.length > 0) {
					firstClose = records[0].close;
				}
				const data = records.map(ohlc => {
					let value;
					if (multiMode) {
						value = ohlc.close / firstClose * 100.0;
					} else {
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

	getEquityCurveDaily(equityCurveDaily) {
		const data = equityCurveDaily.map(record => {
			const dateTime = this.getTime(record.date);
			return {
				x: dateTime.valueOf(),
				y: record.accountValue,
				time: dateTime
			};
		});
		const datasets = [
			{
				label: "Equity curve (daily)",
				data: data
			}
		];
		return datasets;
	}

	getEquityCurveTrades(equityCurveByTrade) {
		let x = 1;
		const data = equityCurveByTrade.map(y => {
			return {
				x: x++,
				y: y
			};
		});
		const datasets = [
			{
				label: "Equity curve (by trade)",
				data: data
			}
		];
		return datasets;
	}

	getShortFormat(datasets) {
		for (let i = 0; i < datasets.length; i++) {
			const dataset = datasets[i];
			const data = dataset.data;
			for (let j = 0; j < data.length; j++) {
				const record = data[j];
				const time = record.time;
				if (
					time.hour !== 0 ||
					time.minute !== 0 ||
					time.second !== 0
				) {
					return false;
				}
			}
		}
		return true;
	}

	setTitleCallback(datasets, options, timeSeries) {
		let shortFormat = null;
		if (timeSeries === true) {
			shortFormat = this.getShortFormat(datasets);
		}
		options.options.plugins.tooltip.callbacks.title = tooltipItems => {
			const context = tooltipItems[0];
			let title;
			if (timeSeries === true) {
				const dateTime = context.raw.time;
				title = this.getDateFormat(dateTime, shortFormat);
			} else {
				title = `Trade #${context.raw.x}`;
			}
			return title;
		};
	}

	getBaseChartOptions() {
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
						callbacks: {}
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
		};
		return options;
	}

	setChartOptions(datasets, options, timeSeries) {
		options.type = "line";
		options.data = {
			datasets: datasets
		};
		options.options = {
			plugins: {
				tooltip: {
					callbacks: {}
				}
			}
		};
		const innerOptions = options.options;
		innerOptions.scales = {
			x: {
				type: timeSeries === true ? "timeseries" : "line",
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
		this.setTitleCallback(datasets, options, timeSeries);
	}

	createEditor() {
		const container = document.createElement("div");
		container.className = "editor";
		container.onkeydown = this.onEditorKeyDown.bind(this);
		container.onkeyup = this.onEditorKeyUp.bind(this);
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
		this.enableHistory = true;
	}

	async onEditorKeyDown(event) {
		if (event.key === "Enter" && !event.shiftKey) {
			event.preventDefault();
			const script = this.editor.getValue();	
			this.enableEditor(false);
			try {
				await this.engine.run(script);
				const data = this.getLocalStorageData();
				data.lastScript = script;
				this.history.push(script);
				data.variables = this.engine.serializeVariables();
				this.setLocalStorageData(data);
				this.disableHighlight();
				this.editorContainer.classList.add("read-only");
				this.createEditor();
				window.scrollTo(0, document.body.scrollHeight);
			}
			catch (error) {
				toastr.error(error, "Script Error");
				console.error(error);
				this.enableEditor(true);
			}
		}
	}

	onEditorKeyUp(event) {
		const arrowUp = event.key === "ArrowUp";
		const arrowDown = event.key === "ArrowDown";
		const historyLast = this.history.length - 1;
		const incrementIndex = direction => {
			this.historyIndex += direction;
			this.historyIndex = Math.max(this.historyIndex, 0);
			this.historyIndex = Math.min(this.historyIndex, historyLast);
		};
		if (this.enableHistory && arrowUp) {
			if (this.historyIndex === null) {
				this.historyIndex = historyLast;
			} else {
				incrementIndex(-1);
			}
			this.showHistory(event);
		} else if (this.enableHistory && arrowDown && this.historyIndex !== null) {
			incrementIndex(1);
			this.showHistory(event);
		} else if (event.key !== "Enter") {
			this.enableHistory = false;
			this.historyIndex = null;
		}
	}

	showHistory(event) {
		const history = this.history[this.historyIndex];
		this.editor.setValue(history, 1);
		event.preventDefault();
	}

	getLocalStorageData() {
		const json = localStorage.getItem(LOCAL_STORAGE_KEY);
		if (json == null) {
			return {};
		}
		const data = JSON.parse(json);
		return data;
	}

	setLocalStorageData(data) {
		const json = JSON.stringify(data);
		localStorage.setItem(LOCAL_STORAGE_KEY, json);
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
			const data = correlation.result[i];
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
		const from = callArguments[1] || new TimeParameter(Keyword.FIRST);
		const to = callArguments[2] || new TimeParameter(Keyword.LAST);
		const timeFrame = callArguments[3] || new TimeFrame(SECONDS_PER_DAY);
		let symbols;
		const invalidSymbol = "Invalid symbol argument type";
		let mode;
		if (isCandlestick) {
			if (!(symbolArgument instanceof Symbol)) {
				throw new Error(invalidSymbol);
			}
			symbols = [symbolArgument.getJsonValue()];
			mode = ChartMode.CANDLESTICK;
		} else {
			symbols = this.getSymbols(symbolArgument);
			mode = ChartMode.LINE;
		}
		this.validateFromTo(from, to);
		this.validateTimeFrame(timeFrame);
		const history = await this.getHistory(symbols, from.getJsonValue(), to.getJsonValue(), timeFrame.getJsonValue());
		this.createChart(history, mode);
	}

	async plotCandlestick(callArguments) {
		await this.plot(callArguments, true);
	}

	async plotLine(callArguments) {
		await this.plot(callArguments, false);
	}

	async correlation(callArguments) {
		this.validateArgumentCount(callArguments, 1, 3);
		const symbols = callArguments[0];
		const from = callArguments[1] || new TimeParameter(Keyword.FIRST);
		const to = callArguments[2] || new TimeParameter(Keyword.LAST);
		this.validateSymbols(symbols);
		this.validateFromTo(from, to);
		const correlation = await this.getCorrelation(symbols.getJsonValue(), from.getJsonValue(), to.getJsonValue());
		let separators = null;
		if (symbols instanceof SymbolArray) {
			separators = symbols.value.map(x => x.separator);
		}
		this.renderCorrelationMatrix(correlation, separators);
	}

	async backtest(callArguments) {
		this.validateArgumentCount(callArguments, 4, 6);
		const strategy = callArguments[0];
		const symbolArgument = callArguments[1];
		const from = callArguments[2];
		const to = callArguments[3];
		const parameters = callArguments[4] || new Parameters([]);
		const timeFrame = callArguments[5] || new String("daily");
		if (!(strategy instanceof String)) {
			throw new Error("Invalid strategy argument type");
		}
		this.validateSymbols(symbolArgument);
		this.validateFromTo(from, to);
		if (!(parameters instanceof Parameters)) {
			throw new Error("Invalid parameters argument type");
		}
		if (!(timeFrame instanceof String)) {
			throw new Error("Invalid time frame argument type");
		}
		const symbols = this.getSymbols(symbolArgument);
		const backtestResult = await this.runBacktest(
			strategy.getJsonValue(),
			symbols,
			from.getJsonValue(),
			to.getJsonValue(),
			parameters.getJsonValue(),
			timeFrame.getJsonValue(),
		);
		this.createChart(backtestResult, ChartMode.EQUITY_CURVE);
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
		} else if (!(tickers instanceof Symbol)) {
			throw new Error("Invalid symbol argument type");
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
			timeFrame.value > SECONDS_PER_DAY ||
			!Number.isInteger(timeFrame.value)
		) {
			throw new Error("Invalid time frame specified");
		}
	}

	getSymbols(symbolArgument) {
		if (symbolArgument instanceof Symbol) {
			return [symbolArgument.getJsonValue()];
		} else if (symbolArgument instanceof SymbolArray) {
			this.validateSymbols(symbolArgument);
			return symbolArgument.getJsonValue();
		} else {
			throw new Error(invalidSymbol);
		}
	}
}