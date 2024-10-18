// Momentum reversal long/short strategy

let long_offset1 = parameter("longOffset1", 20);
let long_offset2 = parameter("longOffset2", 40);
let short_offset1 = parameter("shortOffset1", 25);
let short_offset2 = parameter("shortOffset2", 45);

// Parameter "validation"
long_offset1 = min(long_offset1, long_offset2);
short_offset1 = min(short_offset1, short_offset2);

let previous_signal = CLOSE;

fn next() {
	let close = close();
	let long_close1 = close(long_offset1);
	let long_close2 = close(long_offset2);
	let short_close1 = close(short_offset1);
	let short_close2 = close(short_offset2);
	if close == () || long_close1 == () || long_close2 == () || short_close1 == () || short_close2 == () {
		// Buffers haven't been filled yet
		return CLOSE;
	}
	let long_signal = close > close(long_offset1) && close < close(long_offset2);
	let short_signal = close < close(short_offset1) && close > close(short_offset2);
	let signal = if long_signal && !short_signal {
		LONG
	} else if short_signal && !long_signal {
		SHORT
	} else {
		previous_signal
	};
	previous_signal = signal;
	signal
}