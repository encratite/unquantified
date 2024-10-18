// RSI(2) long/short strategy with ADX and SMA regime filters

const RSI_PERIOD = 2;
const ADX_PERIOD = 14;

let adx_threshold = parameter("adxThreshold", 20);
let sma_period = parameter("smaPeriod", 250);
let rsi_long_entry = parameter("rsiLongEntry", 20);
let rsi_long_exit = parameter("rsiLongExit", 75);
let rsi_short_entry = parameter("rsiShortEntry", 85);
let rsi_short_exit = parameter("rsiShortExit", 25);

let previous_signal = CLOSE;

fn next() {
	let sma = sma(sma_period);
	let adx = adx(ADX_PERIOD);
	let rsi = rsi(RSI_PERIOD);
	let close = close();
	if sma == () || adx == () || rsi == () {
		// Buffers haven't been filled yet
		return CLOSE;
	}
	let filter = adx > adx_threshold;
	let signal = if filter && close > sma && rsi < rsi_long_entry {
		LONG
	} else if filter && close < sma && rsi > rsi_short_entry {
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