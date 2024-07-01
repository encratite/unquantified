/*
import * as luxon from "./luxon/luxon.js";
import { Chart, ChartConfiguration, ChartTypeRegistry } from "./chart.js/chart.js";
*/

function loadData() {
	const request = {
		tickers: [
			"ES"
		],
		from: "2023-11-01T00:00:00+02:00",
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
		.catch(error => console.error(error));
}

function getTime(timeString) {
	const time = luxon.DateTime.fromISO(timeString);
	return time.valueOf();
}

function drawChart(historyResponse) {
	const div = document.createElement("div");
	div.style.width = "1500px";
	const canvas = document.createElement("canvas");
	div.appendChild(canvas);
	document.body.appendChild(div);
	const button = document.createElement("button");
	button.className = "resetZoom";
	button.textContent = "Reset zoom";
	div.appendChild(button);
	const context = canvas.getContext("2d");
	if (context === null) {
		throw new Error("Failed to create 2D context");
	}
	context.canvas.width = 1000;
	context.canvas.height = 500;
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
	const options: ChartConfiguration<keyof ChartTypeRegistry, any, unknown> = {
		type: "candlestick",
		data: {
			datasets: datasets
		},
		options: {
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
	};
	const chart = new Chart(context, options);
	button.onclick = event => chart.resetZoom();
}

function initializeEditor() {
	const container = document.getElementById("editor");
	const editor = ace.edit(container);
	editor.setTheme("ace/theme/monokai");
	editor.session.setMode("ace/mode/javascript");
}

document.addEventListener("DOMContentLoaded", event => {
	initializeEditor();
	// loadData();
});