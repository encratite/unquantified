// RSI(2) long/short strategy with an ADX filter and an SMA regime filter

const RSI_PERIOD = 2;
const ADX_PERIOD = 14;

let adx_threshold = parameter("adxThreshold", 20);
let sma_period = parameter("smaPeriod", 250);
let rsi_long_entry = parameter("rsiLongEntry", 20);
let rsi_long_exit = parameter("rsiLongExit", 75);
let rsi_short_entry = parameter("rsiShortEntry", 85);
let rsi_short_exit = parameter("rsiShortExit", 25);

fn next() {
	let sma = sma(sma_period);
	let adx = adx(ADX_PERIOD);
	let rsi = rsi(RSI_PERIOD);
	let close = close();
	let previous_signal = previous();
	let exit_long = previous_signal == LONG && rsi > rsi_long_exit;
	let exit_short = previous_signal == SHORT && rsi < rsi_short_exit;
	let filter = adx > adx_threshold;
	if exit_long || exit_short {
		CLOSE
	} else {
		if filter && close > sma && rsi < rsi_long_entry {
			LONG
		} else if filter && close < sma && rsi > rsi_short_entry {
			SHORT
		} else {
			HOLD
		}
	}
}