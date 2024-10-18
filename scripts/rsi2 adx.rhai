// RSI(2) long/short strategy with ADX and SMA regime filters

const ROC_PERIOD = 10;
const RSI_PERIOD = 2;
const ADX_PERIOD = 14;
const ADX_THRESHOLD = 20;

let exclude_month = parameter("excludeMonth", 4);
let roc_limit = parameter("rocLimit", 10);
let sma_period = parameter("smaPeriod", 300);
let rsi_long_entry = parameter("rsiLongEntry", 20);
let rsi_long_exit = parameter("rsiLongExit", 75);
let rsi_short_entry = parameter("rsiShortEntry", 85);
let rsi_short_exit = parameter("rsiShortExit", 25);

let previous_signal = CLOSE;

fn next() {
	let roc = roc(ROC_PERIOD);
	let sma = sma(sma_period);
	let adx = adx(ADX_PERIOD);
	let rsi = rsi(RSI_PERIOD);
	let close = close();
	if roc == () || sma == () || adx == () || rsi == () {
		// Buffers haven't been filled yet
		return CLOSE;
	}
	let filter = month() != exclude_month && roc < roc_limit && adx > ADX_THRESHOLD;
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