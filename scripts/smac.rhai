// Basic long-only SMA crossover strategy with regime filter (close > SMA(n))

let fast_period = parameter("fastPeriod", 2);
let slow_period = parameter("slowPeriod", 10);
let filter_period = parameter("filterPeriod", 200);

fn next() {
	let fast_ma = sma(fast_period);
	let slow_ma = sma(slow_period);
	let filter_ma = sma(filter_period);
	let close = close();
	if fast_ma > slow_ma && close > filter_ma {
		LONG
	} else {
		CLOSE
	}
}