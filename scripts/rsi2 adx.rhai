// RSI(2) long/short strategy with ADX and SMA regime filters

const RSI_PERIOD = 2;
const ADX_PERIOD = 14;
const ADX_THRESHOLD = 20;

let rsi_long_entry = parameter("rsiLongEntry", 20);
let rsi_long_exit = parameter("rsiLongExit", 75);
let rsi_short_entry = parameter("rsiShortEntry", 85);
let rsi_short_exit = parameter("rsiShortExit", 25);
let sma_period = parameter("smaPeriod", 300);

let previous_signal = CLOSE;

fn next() {
	let rsi = rsi(RSI_PERIOD);
	let adx = adx(ADX_PERIOD);
	let close = close();
	let sma = sma(sma_period);
	if rsi == () || adx == () || close == () || sma == () {
		// Buffers haven't been filled yet
		return CLOSE;
	}
	let signal = if rsi < rsi_long_entry && adx > ADX_THRESHOLD && close > sma {
		LONG
	} else if rsi > rsi_short_entry && adx > ADX_THRESHOLD && close < sma {
		SHORT
	} else {
		previous_signal
	};
	if previous_signal == LONG && rsi > rsi_long_exit {
		signal = CLOSE;
	} else if previous_signal == SHORT && rsi < rsi_short_exit {
		signal = CLOSE;
	}
	previous_signal = signal;
	signal
}