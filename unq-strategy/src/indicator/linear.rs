use std::collections::VecDeque;
use unq_common::ohlc::OhlcRecord;
use crate::id::IndicatorId;
use crate::indicator::moving_average::MovingAverage;
use crate::technical::*;

#[derive(Clone)]
pub struct LinearMovingAverage(MovingAverage);

impl LinearMovingAverage {
	pub const ID: &'static str = "p-lma";
	pub const CROSSOVER_ID: &'static str = "lmac";

	pub fn new(fast_period: usize, slow_period: Option<usize>) -> anyhow::Result<Self> {
		let moving_average = MovingAverage::new(fast_period, slow_period, 1)?;
		let output = LinearMovingAverage(moving_average);
		Ok(output)
	}

	pub fn get_id(period: usize) -> IndicatorId {
		IndicatorId::from_period("lma", period)
	}
}

impl Indicator for LinearMovingAverage {
	fn get_description(&self) -> String {
		if let Some(slow_period) = self.0.slow_period {
			format!("LMAC({}, {})", self.0.fast_period, slow_period)
		} else {
			format!("P-LMA({})", self.0.fast_period)
		}
	}

	fn next(&mut self, record: &OhlcRecord) {
		let calculate = |period: usize, buffer: &VecDeque<f64>| -> f64 {
			let mut average = 0.0;
			let mut i = 0;
			for x in buffer.iter().take(period) {
				average += ((period - i) as f64) * x;
				i += 1;
			}
			average /= ((period * (period + 1)) as f64) / 2.0;
			average
		};
		self.0.calculate_averages(record, &calculate)
	}

	fn get_indicators(&self) -> Option<Vec<f64>> {
		self.0.get_indicators()
	}

	fn get_trade_signal(&self, _: PositionState) -> Option<TradeSignal> {
		self.0.trade_signal.clone()
	}

	fn needs_initialization(&self) -> Option<usize> {
		self.0.buffer.needs_initialization()
	}

	fn clone_box(&self) -> Box<dyn Indicator> {
		Box::new(self.clone())
	}
}