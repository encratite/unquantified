#[derive(PartialEq, Clone)]
pub struct IndicatorId {
	name: &'static str,
	period1: usize,
	period2: usize,
	period3: usize,
	multiplier: f64
}

impl IndicatorId {
	pub fn from_period(name: &'static str, period: usize) -> Self {
		Self {
			name,
			period1: period,
			period2: 0,
			period3: 0,
			multiplier: 0.0
		}
	}

	pub fn from_signal_fast_slow(name: &'static str, signal_period: usize, fast_period: usize, slow_period: usize) -> Self {
		Self {
			name,
			period1: signal_period,
			period2: fast_period,
			period3: slow_period,
			multiplier: 0.0
		}
	}

	pub fn from_period_multiplier(name: &'static str, period: usize, multiplier: f64) -> Self {
		Self {
			name,
			period1: period,
			period2: 0,
			period3: 0,
			multiplier
		}
	}
}