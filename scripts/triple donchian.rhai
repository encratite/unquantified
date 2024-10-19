// Triple Donchian channel narrowing long/short strategy

const DONCHIAN_PERIOD3 = 3;
const ADX_PERIOD = 14;
const ADX_THRESHOLD = 20;

let donchian_period1 = parameter("donchianPeriod1", 70);
let donchian_period2 = parameter("donchianPeriod2", 25);
let holding_time = parameter("holdingTime", 20);

if donchian_period1 <= donchian_period2 || donchian_period2 <= DONCHIAN_PERIOD3 {
	throw "Invalid parameters";
}

fn next() {
	let close = close();
	let donchian1 = donchian(donchian_period1);
	let donchian2 = donchian(donchian_period2);
	let donchian3 = donchian(DONCHIAN_PERIOD3);
	let adx = adx(ADX_PERIOD);
	if holding_time() >= holding_time {
		return CLOSE;
	}
	let narrowing1 = donchian1.upper > donchian2.upper && donchian1.lower < donchian2.lower;
	let narrowing2 = donchian2.upper > donchian3.upper && donchian2.lower < donchian3.lower;
	let adx_filter = adx > ADX_THRESHOLD;
	let signal = if narrowing1 && narrowing2 && adx_filter {
		if close > donchian2.upper {
			LONG
		} else if close < donchian2.lower {
			SHORT
		} else {
			HOLD
		}
	} else {
		HOLD
	};
	signal
}