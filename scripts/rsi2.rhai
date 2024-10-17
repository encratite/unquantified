/*
RSI 2 long-only strategy with three different regime filters that can be selected using the "mode" parameter.

1. SMA filter (mode "sma")
- Filter: close > sma(smaPeriod)
- SMA period is set using the "smaPeriod" parameter

2. ADX filter (mode "adx")
- Filter: adx(20) > adxThreshold
- ADX threshold is set using the "adxThreshold" parameter

3. ATR filter (mode "atr")
- Filter: atr(20) > atrThreshold
- ATR threshold is set using the "atrThreshold" parameter
*/

const MODE_SMA = "sma";
const MODE_ADX = "adx";
const MODE_ATR = "atr";
const ADX_PERIOD = 20;
const ATR_PERIOD = 20;

let low_threshold = parameter("lowThreshold", 10);
let high_threshold = parameter("highThreshold", 70);
let sma_period = parameter("smaPeriod", 200);
let adx_threshold = parameter("adxThreshold", 20);
let atr_threshold = parameter("atrThreshold", 30);
let mode = parameter("mode", MODE_SMA);

let previous_signal = CLOSE;

fn next(symbol) {
	let rsi = rsi(2);
	let close = close();
	let sma = sma(sma_period);
	let adx = adx(ADX_PERIOD);
	let atr = atr(ATR_PERIOD);
	if rsi == () || close == () || sma == () || adx == () || atr == () {
		// Buffers haven't been filled yet
		return CLOSE;
	}
	let filter = if mode == MODE_SMA {
		close > sma
	} else if mode == MODE_ADX {
		adx > adx_threshold
	} else if mode == MODE_ATR {
		atr > atr_threshold
	} else {
		throw `Unknown mode "${mode}"`;
	};
	let signal = if filter {
		if rsi < low_threshold {
			LONG
		} else if rsi > high_threshold {
			CLOSE
		} else {
			previous_signal
		}
	} else {
		CLOSE
	};
	previous_signal = signal;
	signal
}