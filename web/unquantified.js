function loadData() {
	const request = {
		tickers: [
			"ES"
		],
		from: "2024-01-01T00:00:00+02:00",
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
					}
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
	editor.setTheme("ace/theme/monokai");
	editor.session.setMode("ace/mode/javascript");
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
}

document.addEventListener("DOMContentLoaded", _ => {
	initializeEditor();
	loadData();
});