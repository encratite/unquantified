import "./engine.js";
import ScriptingEngine from "./engine.js";

function loadData() {
	const request = {
		tickers: [
			"ES"
		],
		from: "2000-01-01T00:00:00+02:00",
		to: "2024-06-01T00:00:00+02:00",
		timeFrame: 1440
	};
	const json = JSON.stringify(request);
	const url = "/history";
	const options = {
		method: "POST",
		headers: {
			"Content-Type": "application/json"
		},
		body: json
	};
	fetch(url, options)
		.then(response => response.json())
		.then(historyResponse => drawChart(historyResponse))
		// .then(historyResponse => winRatioTest(historyResponse.tickers["ES"]))
		.catch(error => console.error(error));
}

function getTime(timeString) {
	const time = luxon.DateTime.fromISO(timeString);
	return time.valueOf();
}

function winRatioTest(records) {
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

function drawChart(historyResponse) {
	const chartContainer = document.createElement("div");
	chartContainer.classList.add("plot");
	const canvas = document.createElement("canvas");
	chartContainer.appendChild(canvas);
	const button = document.createElement("button");
	button.textContent = "Reset zoom";
	chartContainer.appendChild(button);
	const content = document.getElementById("content");
	content.appendChild(chartContainer);
	const context = canvas.getContext("2d");
	if (context === null) {
		throw new Error("Failed to create 2D context");
	}
	const ticker = Object.keys(historyResponse.tickers)[0];
	const records = historyResponse.tickers[ticker];
	const barData = records.map(ohlc => {
		return {
			x: getTime(ohlc.time),
			o: ohlc.open,
			h: ohlc.high,
			l: ohlc.low,
			c: ohlc.close
		};
	});
	const datasets = [
		{
			label: ticker,
			data: barData
		}
	];
	const options = {
		type: "candlestick",
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
	$(chartContainer).resizable();
}

function initializeEditor() {
	const container = document.getElementById("editor");
	const editor = ace.edit(container);
	editor.setOptions({
		fontSize: "14px",
		useWorker: false
	});
	editor.setShowPrintMargin(false);
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
	return editor;
}

async function plotCandlestick(callArguments) {
	console.log("plotCandlestick", callArguments);
}

async function plotLine(callArguments) {
	console.log("plotLine", callArguments);
}

async function correlation(callArguments) {
	console.log("correlation", callArguments);
}

async function winRatio(callArguments) {
	console.log("winRatio", callArguments);
}

async function walkForward(callArguments) {
	console.log("walkForward", callArguments);
}

document.addEventListener("DOMContentLoaded", _ => {
	const editor = initializeEditor();
	// loadData();
	const script = editor.getValue();
	const callHandlers = {
		plotCandlestick: plotCandlestick,
		plotLine: plotLine,
		correlation: correlation,
		winRatio: winRatio,
		walkForward: walkForward,
	};
	const engine = new ScriptingEngine(callHandlers);
	engine.run(script)
		.then(() => console.log("Done executing script"))
		.catch(error => console.error(`Scripting error: ${error}`));
});