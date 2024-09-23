import chroma from "https://cdn.jsdelivr.net/npm/chroma-js@3.1.1/index.min.js";
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
const NOT_AVAILABLE_SYMBOL = "-";
// At what point should the max drawdown be highlighted in red?
const MAX_DRAWDOWN_WARNING = -0.2;
const RATIO_DIGITS = 2;

const ChartMode = {
	LINE: "line",
	CANDLESTICK: "candlestick",
	EQUITY_CURVE: "equityCurve"
};

const DailyStatsMode = {
	EQUITY_CURVE_DAILY: "equityCurveDaily",
	EQUITY_CURVE_TRADES: "equityCurveTrades",
	MARGIN: "margin"
};

function createElement(tag, container, properties) {
	const element = document.createElement(tag);
	if (container != null) {
		container.appendChild(element);
	}
	if (properties != null) {
		for (const name in properties) {
			element[name] = properties[name];
		}
	}
	return element;
}

function zeroToNull(number) {
	return number != 0 ? number : null;
}

function numericSpan(content) {
	if (content instanceof Node) {
		content.classList.add("numeric");
		return content;
	} else {
		return createElement("span", null, {
			className: "numeric",
			textContent: content
		});
	}
}

function createTable(rows, container, title) {
	const table = createElement("table", container);
	if (title != null) {
		const row = createElement("tr", table);
		createElement("td", row, {
			textContent: title,
			colSpan: 2
		});
	}
	rows.forEach(columns => {
		const row = createElement("tr", table);
		columns.forEach(column => {
			const cell = createElement("td", row);
			if (typeof column === "string") {
				cell.textContent = column;
			} else {
				cell.appendChild(column);
			}
		});
	});
	return table;
}

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

	createBacktestTables(result) {
		const container = createElement("div", this.content, {
			className: "results"
		});
		const generalStatsContainer = createElement("div", container, {
			className: "general"
		});
		const tradesContainer = createElement("div", container, {
			className: "trades"
		});
		const eventsContainer = createElement("div", container, {
			className: "events"
		});
		const bestResult = result.bestResult;
		const generalTableLeft = [
			["Net profit", this.formatCurrency(bestResult.profit)],
			["Annual average profit", this.formatCurrency(bestResult.annualAverageProfit)],
			["Starting capital", this.formatCurrency(bestResult.startingCash)],
			["Total return", this.formatPercentage(bestResult.totalReturn)],
			["Compound annual growth rate", this.formatPercentage(bestResult.compoundAnnualGrowthRate)],
			["Interest accumulated", this.formatCurrency(bestResult.interest)],
		];
		const maxDrawdownEnableColor = bestResult.maxDrawdown < MAX_DRAWDOWN_WARNING;
		const generalTableRight = [
			["Sharpe ratio", this.formatNumber(bestResult.sharpeRatio, RATIO_DIGITS, true)],
			["Sortino ratio", this.formatNumber(bestResult.sortinoRatio, RATIO_DIGITS, true)],
			["Calmar ratio", this.formatNumber(bestResult.calmarRatio, RATIO_DIGITS, true)],
			["Max drawdown", this.formatPercentage(zeroToNull(bestResult.maxDrawdown), false, maxDrawdownEnableColor)],
			["Fees paid", this.formatCurrency(bestResult.fees)],
			["Fees per profit", this.formatPercentage(zeroToNull(bestResult.feesPercent), false, false)],
		];
		const createTradesTable = (title, tradeResults) => {
			const rows = [
				["Trades", this.formatInt(zeroToNull(tradeResults.trades))],
				["Profit", this.formatCurrency(zeroToNull(tradeResults.profit))],
				["Profit per trade", this.formatCurrency(tradeResults.profitPerTrade)],
				["Win rate", this.formatPercentage(tradeResults.winRate, false, false)],
				["Profit factor", this.formatNumber(tradeResults.profitFactor)],
				["Bars in trade", this.formatNumber(tradeResults.barsInTrade, 1)]
			];
			createTable(rows, tradesContainer, title);
		};
		createTable(generalTableLeft, generalStatsContainer);
		createTable(generalTableRight, generalStatsContainer);
		createTradesTable("All trades", bestResult.allTrades);
		createTradesTable("Long trades only", bestResult.longTrades);
		createTradesTable("Short trades only", bestResult.shortTrades);
		this.createEventTable(bestResult, eventsContainer);
		this.createParametersTable(result, container);
	}

	createEventTable(bestResult, eventsContainer) {
		let eventRows = bestResult.events.map(event => {
			const dateTime = luxon.DateTime.fromISO(event.time);
			const short =
				dateTime.hour === 0 &&
				dateTime.minute === 0 &&
				dateTime.second === 0;
			const dateTimeString = this.getDateFormat(dateTime, short);
			const eventTypeData = this.getEventTypeData(event.eventType);
			const description = eventTypeData[0];
			const className = eventTypeData[1];
			const descriptionSpan = createElement("span", null, {
				textContent: description
			});
			if (className != null) {
				descriptionSpan.className = className;
			}
			return [
				dateTimeString,
				descriptionSpan,
				event.message
			];
		});
		eventRows = [
			["Time", "Event", "Description"]
		].concat(eventRows);
		createTable(eventRows, eventsContainer);
	}

	createParametersTable(result, container) {
		// Only render the performance overview in case of multiple strategy parameters having been evaluated
		if (result.results.length <= 1) {
			return;
		}
		const parameterContainer = createElement("div", container, {
			className: "parameters"
		});
		const firstRow = result.results[0];
		let headers = firstRow.parameters
			.filter(parameter => this.isExpandedParameter(parameter, result.bestParameters))
			.map(parameter => this.getParameterName(parameter.name));
		headers = headers.concat([
			"Trades",
			"Total return",
			"CAGR",
			"Sharpe",
			"Sortino",
			"Drawdown"
		]);
		let parameterRows = result.results.map(simplifiedResult => {
			const maxDrawdownEnableColor = simplifiedResult.maxDrawdown < MAX_DRAWDOWN_WARNING;
			let output = simplifiedResult.parameters
				.filter(parameter => this.isExpandedParameter(parameter, result.bestParameters))
				.map(parameter => this.getParameterContent(parameter));
			const numericCells = [
				this.formatInt(zeroToNull(simplifiedResult.trades)),
				this.formatPercentage(simplifiedResult.totalReturn),
				this.formatPercentage(simplifiedResult.compoundAnnualGrowthRate),
				this.formatNumber(simplifiedResult.sharpeRatio, RATIO_DIGITS, true),
				this.formatNumber(simplifiedResult.sortinoRatio, RATIO_DIGITS, true),
				this.formatPercentage(zeroToNull(simplifiedResult.maxDrawdown), false, maxDrawdownEnableColor)
			].map(numericSpan);
			output = output.concat(numericCells);
			return output;
		});
		parameterRows = [
			headers
		].concat(parameterRows);
		const table = createTable(parameterRows, parameterContainer);
		const tableHeaders = table.firstChild;
		const secondRow = table.querySelectorAll("tr:nth-child(2) td");
		const className = "numeric";
		for (let i = 0; i < secondRow.length; i++) {
			const span = secondRow[i].firstChild;
			if (
				span != null &&
				span instanceof HTMLSpanElement &&
				span.className === className
			) {
				tableHeaders.childNodes[i].className = className;
			}
		}
		createElement("div", container, {
			className: "statistics",
			textContent: `Evaluated ${result.results.length} combinations in ${result.stopwatch} s`
		});
	}

	isExpandedParameter(parameter, bestParameters) {
		const baseParameter = bestParameters.find(x => x.name === parameter.name);
		if (baseParameter == null) {
			throw new Error("Unable to find a matching base parameter");
		}
		const isMulti = baseParameter.limit != null || baseParameter.values != null;
		return baseParameter.name !== "contracts" && isMulti;
	}

	getParameterName(name) {
		name = name.replace(/([A-Z])/g, " $1");
		name = name.toLowerCase();
		name = name[0].toUpperCase() + name.substring(1);
		return name;
	}

	getParameterContent(parameter) {
		if (parameter.value != null) {
			return numericSpan(parameter.value.toString());
		} else if (parameter.values != null) {
			if (parameter.values.length != 1) {
				return `[${parameter.values.join(", ")}]`;
			} else {
				return numericSpan(parameter.values[0].toString());
			}
		} else if (parameter.boolValue != null) {
			return parameter.boolValue.toString();
		} else if (parameter.stringValue != null) {
			return parameter.stringValue;
		} else {
			return "(null)";
		}
	}

	createChart(data, mode) {
		const container = createElement("div", this.content, {
			className: "plot"
		});
		const canvas = createElement("canvas", container);
		const button = createElement("button", container, {
			textContent: "Reset zoom"
		});
		const context = canvas.getContext("2d");
		if (context === null) {
			throw new Error("Failed to create 2D context");
		}
		const options = this.getBaseChartOptions();
		const initializeChart = () => {
			const chart = new Chart(context, options);
			button.onclick = _ => chart.resetZoom();
		};
		if (mode === ChartMode.CANDLESTICK) {
			let datasets = this.getCandlestickDatasets(data);
			this.setTitleCallback(datasets, options, true);
			options.type = "candlestick";
			options.data = {
				datasets: datasets
			};
			initializeChart();
		} else if (mode === ChartMode.LINE) {
			const datasets = this.getLineDatasets(data);
			this.setChartOptions(datasets, options, true, true, false);
			initializeChart();
		} else if (mode === ChartMode.EQUITY_CURVE) {
			const form = createElement("form", null, {
				className: "equity-curve"
			});
			container.insertBefore(form, container.firstChild);
			const chartContext = new ChartContext(context, button, data);
			const onChangeDaily = () => this.updateDailyStats(chartContext, DailyStatsMode.EQUITY_CURVE_DAILY);
			const onChangeTrades = () => this.updateDailyStats(chartContext, DailyStatsMode.EQUITY_CURVE_TRADES);
			const onChangeMargin = () => this.updateDailyStats(chartContext, DailyStatsMode.MARGIN);
			this.createRadioButton("Equity curve (daily)", form, true, onChangeDaily);
			this.createRadioButton("Equity curve (trades)", form, false, onChangeTrades);
			this.createRadioButton("Margin used", form, false, onChangeMargin);
			this.updateDailyStats(chartContext, DailyStatsMode.EQUITY_CURVE_DAILY);
		} else {
			throw new Error("Unknown chart mode specified");
		}
		$(container).resizable();
	}

	updateDailyStats(chartContext, mode) {
		let datasets;
		if (mode === DailyStatsMode.EQUITY_CURVE_DAILY) {
			datasets = this.getEquityCurveDaily(chartContext.data.equityCurveDaily);
		} else if (mode === DailyStatsMode.EQUITY_CURVE_TRADES) {
			datasets = this.getEquityCurveTrades(chartContext.data.equityCurveTrades);
		} else if (mode === DailyStatsMode.MARGIN) {
			datasets = this.getMarginDatasets(chartContext.data.equityCurveDaily);
		} else {
			throw new Error("Unknown mode in updateDailyStats");
		}
		const options = this.getBaseChartOptions();
		const timeSeries = mode !== DailyStatsMode.EQUITY_CURVE_TRADES;
		this.setChartOptions(datasets, options, timeSeries, false, true);
		chartContext.render(options);
	}

	createRadioButton(labelText, container, checked, onChange) {
		const label = createElement("label", container);
		createElement("input", label, {
			type: "radio",
			name: "mode",
			checked: checked,
			onchange: onChange
		});
		createElement("span", label, {
			textContent: labelText
		});
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

	getProfitPercentage(equityCurve, first) {
		const percentage = equityCurve.accountValue / first.accountValue - 1.0;
		return percentage;
	}

	getEquityCurveDaily(equityCurveDaily) {
		const getData = drawdown => {
			const data = equityCurveDaily.map(record => {
				const dateTime = this.getTime(record.date);
				const equityCurve = record.equityCurve;
				let output = {
					x: dateTime.valueOf(),
					time: dateTime
				};
				if (drawdown === true) {
					output.y = equityCurve.drawdown;
					output.percentage = equityCurve.drawdownPercent;
				} else {
					output.y = equityCurve.accountValue;
					output.percentage = this.getProfitPercentage(equityCurve, equityCurveDaily[0].equityCurve);
				}
				return output;
			});
			return data;
		};
		const datasets = this.getEquityCurveDatasets(getData);
		return datasets;
	}

	getEquityCurveTrades(equityCurveByTrade) {
		const getData = drawdown => {
			let x = 0;
			const data = equityCurveByTrade.map(record => {
				let output = {
					x: x++,
				};
				if (drawdown === true) {
					output.y = record.drawdown;
					output.percentage = record.drawdownPercent;
				}
				else {
					output.y = record.accountValue;
					output.percentage = this.getProfitPercentage(record, equityCurveByTrade[0]);
				}
				return output;
			});
			return data;
		};
		const datasets = this.getEquityCurveDatasets(getData);
		return datasets;
	}

	getEquityCurveDatasets(getData) {
		const equityCurve = getData(false);
		const drawdown = getData(true);
		const datasets = [
			{
				label: "Equity curve",
				data: equityCurve,
				borderColor: "#4bc0c0",
				backgroundColor: "#4bc0c0a0",
				fill: "origin"
			},
			{
				label: "Drawdown",
				data: drawdown,
				borderColor: "#ff6384",
				backgroundColor: "#ff6384a0",
				fill: "origin"
			}
		];
		return datasets;
	}

	getMarginDatasets(equityCurveDaily) {
		const getData = overnight => equityCurveDaily.map(record => {
			const dateTime = this.getTime(record.date);
			const value = overnight ? record.overnightMargin : record.maintenanceMargin;
			const percentage = value / record.equityCurve.accountValue;
			return {
				x: dateTime.valueOf(),
				y: value,
				time: dateTime,
				percentage: percentage
			};
		});
		let maintenanceMarginData = getData(false);
		let overnightMarginData = getData(true);
		const datasets = [
			{
				label: "Maintenance margin",
				data: maintenanceMarginData
			},
			{
				label: "Overnight margin",
				data: overnightMarginData
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
				const x = context.raw.x;
				if (x > 0) {
					title = `Trade #${x}`;
				} else {
					title = "Start";
				}
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

	setChartOptions(datasets, options, timeSeries, lineMode, currency) {
		options.type = "line";
		options.data = {
			datasets: datasets
		};
		const innerOptions = options.options;
		innerOptions.scales = {
			x: {
				type: timeSeries === true ? "timeseries" : "linear",
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
		if (lineMode === false) {
			innerOptions.scales.y.grid = {
				color: context => {
					if (context.tick.value === 0) {
						return "#000";
					} else {
						return "rgba(0, 0, 0, 0.1)";
					}
				}
			};
		}
		if (currency === true) {
			innerOptions.scales.y.ticks = {
				callback: (value, _, __) => {
					return this.formatCurrency(value, false, false);
				}
			};
		}
		if (timeSeries === false) {
			const ticks = innerOptions.scales.x.ticks;
			ticks.stepSize = 1;
			ticks.callback = value => {
				return Number.isInteger(value) ? value : null;
			};
		}
		innerOptions.pointStyle = false;
		innerOptions.borderJoinStyle = "bevel";
		innerOptions.pointHitRadius = 3;
		const multiMode = datasets.length > 1;
		if (lineMode === true && multiMode) {
			innerOptions.scales.y.ticks = {
				callback: value => {
					return `${value.toFixed(1)}%`;
				}
			};
			const labelCallback = context => {
				const raw = context.raw;
				// Should be using formatCurrency with a currency parameter here
				const formatted = this.formatNumber(raw.c);
				const percentage = this.formatPercentage(raw.y, false, false)
				return `${context.dataset.label}: ${formatted} (${percentage})`;
			};
			innerOptions.plugins.tooltip.callbacks.label = labelCallback;
		} else if (lineMode === false) {
			const labelCallback = context => {
				const raw = context.raw;
				if (raw.percentage != null) {
					const formatted = this.formatCurrency(raw.y, false, false);
					const percentage = this.formatPercentage(raw.percentage, false, false);
					return `${context.dataset.label}: ${formatted} (${percentage})`;
				}
				else {
					const formatted = this.formatCurrency(raw.y, false, false);
					return `${context.dataset.label}: ${formatted}`;
				}
			};
			innerOptions.plugins.tooltip.callbacks.label = labelCallback;
		}
		this.setTitleCallback(datasets, options, timeSeries);
	}

	formatInt(number) {
		if (number === null) {
			return NOT_AVAILABLE_SYMBOL;
		}
		const formatted = number.toLocaleString("en-US");
		return formatted;
	}

	formatNumber(number, digits, enableColor) {
		if (number === null) {
			return NOT_AVAILABLE_SYMBOL;
		}
		digits = digits || 2;
		const text = number.toLocaleString("en-US", {
			minimumFractionDigits: digits,
			maximumFractionDigits: digits
		});
		if (number < 0) {
			return this.getNegativeSpan(text, enableColor);
		} else {
			return text;
		}
	}

	formatPercentage(number, plusPrefix, enableColor) {
		if (number === null) {
			return NOT_AVAILABLE_SYMBOL;
		}
		const percentage = 100.0 * number;
		const digits = 1;
		const formatted = percentage.toLocaleString("en-US", {
			minimumFractionDigits: digits,
			maximumFractionDigits: digits
		});
		if (number > 0 && plusPrefix === true) {
			return `+${formatted}%`;
		} else {
			const text = `${formatted}%`;
			if (number < 0) {
				return this.getNegativeSpan(text, enableColor);
			} else {
				return text;
			}
		}
	};

	formatCurrency(number, useParentheses, enableColor) {
		if (number === null) {
			return NOT_AVAILABLE_SYMBOL;
		}
		if (useParentheses === undefined) {
			useParentheses = true;
		}
		const formatted = this.formatNumber(Math.abs(number));
		if (number >= 0) {
			return `$${formatted}`;
		} else {
			let text;
			if (useParentheses === true) {
				text = `($${formatted})`;
			} else {
				text = `-$${formatted}`;
			}
			return this.getNegativeSpan(text, enableColor);
		}
	}

	getNegativeSpan(text, enableColor) {
		if (enableColor === undefined || enableColor === true) {
			const span = createElement("span", null, {
				className: "negative",
				textContent: text
			});
			return span;
		} else {
			return text;
		}
	}

	createEditor() {
		const container = createElement("div", this.content, {
			className: "editor",
			onkeydown: this.onEditorKeyDown.bind(this),
			onkeyup: this.onEditorKeyUp.bind(this)
		});
		const editor = ace.edit(container);
		editor.setOptions({
			fontSize: "0.9em",
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
		const container = createElement("div", this.content, {
			className: "correlation"
		});
		const table = createElement("table", container);
		const createRow = () => createElement("tr", table);
		const createCell = (text, row) => {
			const element = createElement("td", row);
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
		const fromToLabel = createElement("div", container);
		fromToLabel.textContent = `Showing data from ${this.getDateFormat(from, true)} to ${this.getDateFormat(to, true)}`;
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
		this.createBacktestTables(backtestResult);
		this.createChart(backtestResult.bestResult, ChartMode.EQUITY_CURVE);
		console.log(backtestResult);
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

	getEventTypeData(eventType) {
		const definitions = {
			openPosition: ["Opened position", null],
			closePosition: ["Closed position", null],
			rollover: ["Rollover", null],
			marginCall: ["Margin call", "error"],
			warning: ["Warning", "warning"],
			error: ["Error", "error"]
		};
		const output = definitions[eventType];
		if (output == null) {
			throw new Error("Unknown event type");
		}
		return output;
	}
}

class ChartContext {
	constructor(context, button, data) {
		this.context = context;
		this.button = button;
		this.data = data;
		this.chart = null;
	}

	render(options) {
		if (this.chart != null) {
			this.chart.destroy();
		}
		this.chart = new Chart(this.context, options);
		this.button.onclick = _ => this.chart.resetZoom();
	}
}