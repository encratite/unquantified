import { UnquantifiedMode } from "./highlight-rules.js";
import {
	ScriptingEngine,
	DateTime,
	Offset,
	TimeFrame,
	Ticker,
	Array,
	SecondsPerDay,
	Keyword
} from "./engine.js";

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
			plotCandlestick: this.plotCandlestick.bind(this),
			plotLine: this.plotLine.bind(this),
			correlation: this.correlation.bind(this),
			winRatio: this.winRatio.bind(this),
			walkForward: this.walkForward.bind(this),
		};
		this.engine = new ScriptingEngine(callHandlers);
		await this.engine.initialize();
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
				.then(resolve)
				.catch(reject);
		});
	}

	async getHistory(tickers, from, to, timeFrame) {
		const request = {
			tickers: tickers,
			from: from,
			to: to,
			timeFrame: timeFrame
		};
		const response = await this.invoke("/history", request);
		return response;
	}

	async getCorrelation(tickers, from, to) {
		const request = {
			tickers: tickers,
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

	winRatioTest(records) {
		let tradesWon = 0;
		let tradesLost = 0;
		let gains = 0;
		let losses = 0;
		for (let i = 0; i < records.length - 1; i++) {
			let ohlc1 = records[i];
			let ohlc2 = records[i + 1];
			let difference = ohlc2.close - ohlc1.close;
			if (difference > 0) {
				gains += difference;
				tradesWon++;
			}
			else {
				losses -= difference;
				tradesLost++;
			}
		}
		const winRatio = tradesWon / (tradesWon + tradesLost);
		const profitRatio = gains / losses;
		console.log(`Win ratio: ${(winRatio * 100).toFixed(1)}%`);
		console.log(`Profit ratio: ${profitRatio.toFixed(2)}`);
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
			const formatString = timeFrame.value === SecondsPerDay ? "dd LLL yyyy" : "dd LLL yyyy HH:mm:ss";
			const title = dateTime.toFormat(formatString);
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
			.map(ticker => {
				const records = history.tickers[ticker];
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
					label: ticker,
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

	renderCorrelationMatrix(correlation) {
		const table = document.createElement("table");
		table.className = "correlation";
		this.append(table);
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
		const tickers = correlation.tickers;
		tickers.forEach(ticker => {
			createCell(ticker, firstRow);
		});
		const chromaScale = chroma.scale("RdYlBu").padding([0, 0.07]);
		const correlationMin = -1;
		const correlationMax = 1;
		for (let i = 0; i < tickers.length; i++) {
			const ticker = tickers[i];
			const data = correlation.correlation[i];
			const row = createRow();
			createCell(ticker, row);
			for (let j = 0; j < tickers.length; j++) {
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
	}

	createElement(tag, parent) {
		const element = document.createElement(tag);
		if (parent != null) {
			parent.appendChild(element);
		}
		return element;
	}

	async plot(callArguments, isCandlestick) {
		this.validateArgumentCount(callArguments, 3, 4);
		const tickerArgument = callArguments[0];
		const from = callArguments[1];
		const to = callArguments[2];
		const timeFrame = callArguments[3] || new TimeFrame(SecondsPerDay);
		let tickers;
		if (isCandlestick) {
			if (!(tickerArgument instanceof Ticker)) {
				throw new Error("Invalid ticker data type");
			}
			tickers = [tickerArgument.getJsonValue()];
		}
		else {
			if (tickerArgument instanceof Ticker) {
				tickers = [tickerArgument.getJsonValue()];
			}
			else if (tickerArgument instanceof Array) {
				this.validateTickers(tickerArgument);
				tickers = tickerArgument.getJsonValue();
			}
			else {
				throw new Error("Invalid ticker data type");
			}
		}
		this.validateFromTo(from, to);
		this.validateTimeFrame(timeFrame);
		const history = await this.getHistory(tickers, from.getJsonValue(), to.getJsonValue(), timeFrame.getJsonValue());
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
		const tickerArgument = callArguments[0];
		const from = callArguments[1] || new DateTime(Keyword.First);
		const to = callArguments[2] || new DateTime(Keyword.Last);
		this.validateTickers(tickerArgument);
		this.validateFromTo(from, to);
		const response = await this.getCorrelation(tickerArgument.getJsonValue(), from.getJsonValue(), to.getJsonValue());
		this.renderCorrelationMatrix(response.correlation);
	}

	async winRatio(callArguments) {
		console.log("winRatio", callArguments);
	}

	async walkForward(callArguments) {
		console.log("walkForward", callArguments);
	}

	validateArgumentCount(callArguments, min, max) {
		if (callArguments.length < min || callArguments.length > max) {
			throw new Error("Invalid number of arguments");
		}
	}

	validateTickers(tickers) {
		if (tickers instanceof Array) {
			for (const t of tickers.value) {
				if (!(t instanceof Ticker)) {
					throw new Error("Encountered an invalid data type in a ticker array");
				}
			}
		}
		else if (!(tickers instanceof Ticker)) {
			throw new Error("Invalid ticker data type");
		}
	}

	validateFromTo(from, to) {
		let dateTimeCount = 0;
		let offsetCount = 0;
		const validateDateTime = x => {
			if (x instanceof DateTime) {
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