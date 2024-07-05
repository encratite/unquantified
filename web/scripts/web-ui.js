import { UnquantifiedMode } from "./highlight-rules.js";
import {
	ScriptingEngine,
	DateTime,
	Offset,
	TimeFrame,
	Ticker,
	Array,
	SecondsPerDay
} from "./engine.js";

export class WebUi {
	constructor() {
		const callHandlers = {
			plotCandlestick: this.plotCandlestick.bind(this),
			plotLine: this.plotLine.bind(this),
			correlation: this.correlation.bind(this),
			winRatio: this.winRatio.bind(this),
			walkForward: this.walkForward.bind(this),
		};
		this.engine = new ScriptingEngine(callHandlers);
		this.content = null;
		this.editor = null;
		this.editorContainer = null;
	}

	initialize() {
		this.content = document.getElementById("content");
		this.createEditor();
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

	getTime(timeString) {
		const time = luxon.DateTime.fromISO(timeString);
		return time.valueOf();
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

	createChart(history, isCandlestick) {
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
		let type;
		let datasets;
		if (isCandlestick) {
			type = "candlestick";
			datasets = this.getCandlestickDatasets(history);
		}
		else {
			type = "line";
			datasets = this.getLineDatasets(history);
		}
		const options = {
			type: type,
			data: {
				datasets: datasets
			},
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
			return {
				x: this.getTime(ohlc.time),
				o: ohlc.open,
				h: ohlc.high,
				l: ohlc.low,
				c: ohlc.close
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
		const datasets = Object.keys(history.tickers)
			.map(ticker => {
				const records = history.tickers[ticker];
				const data = records.map(ohlc => {
					return {
						x: this.getTime(ohlc.time),
						y: ohlc.close
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
			useWorker: false
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

	async plot(callArguments, isCandlestick) {
		if (callArguments.length < 3 || callArguments.length > 4) {
			throw new Error("Invalid number of arguments");
		}
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
				for (const t of tickerArgument) {
					if (!(t instanceof Ticker)) {
						throw new Error("Encountered an invalid data type in a ticker array");
					}
				}
				tickers = tickerArgument.getJsonValue();
			}
			else {
				throw new Error("Invalid ticker data type");
			}
		}
		this.checkFromTo(from, to);
		this.checkTimeFrame(timeFrame);
		const history = await this.getHistory(tickers, from.getJsonValue(), to.getJsonValue(), timeFrame.getJsonValue());
		this.createChart(history, isCandlestick);
	}

	async plotCandlestick(callArguments) {
		this.plot(callArguments, true);
	}

	async plotLine(callArguments) {
		this.plot(callArguments, false);
	}

	async correlation(callArguments) {
		console.log("correlation", callArguments);
	}

	async winRatio(callArguments) {
		console.log("winRatio", callArguments);
	}

	async walkForward(callArguments) {
		console.log("walkForward", callArguments);
	}

	checkFromTo(from, to) {
		let dateTimeCount = 0;
		let offsetCount = 0;
		const dateTimeCheck = x => {
			if (x instanceof DateTime) {
				dateTimeCount++;
			}
		};
		const offsetCheck = x => {
			if (x instanceof Offset) {
				offsetCount++;
			}
		};
		dateTimeCheck(from);
		dateTimeCheck(to);
		offsetCheck(from);
		offsetCheck(to);
		if (
			dateTimeCount + offsetCount < 2 ||
			dateTimeCount == 0
		) {
			throw new Error("Invalid from/to parameter types");
		}
	}

	checkTimeFrame(timeFrame) {
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