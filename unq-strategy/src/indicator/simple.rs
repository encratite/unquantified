use std::collections::VecDeque;
use anyhow::Result;
use unq_common::ohlc::OhlcRecord;
use crate::id::IndicatorId;
use crate::indicator::moving_average::MovingAverage;
use crate::technical::*;

#[derive(Clone)]
pub struct SimpleMovingAverage(MovingAverage);

impl SimpleMovingAverage {
	pub const ID: &'static str = "p-sma";
	pub const CROSSOVER_ID: &'static str = "smac";

	pub fn new(fast_period: usize, slow_period: Option<usize>) -> Result<Self> {
		let moving_average = MovingAverage::new(fast_period, slow_period, 1)?;
		let output = SimpleMovingAverage(moving_average);
		Ok(output)
	}

	pub fn get_id(period: usize) -> IndicatorId {
		IndicatorId::from_period("sma", period)
	}
}

impl Indicator for SimpleMovingAverage {
	fn get_description(&self) -> String {
		if let Some(slow_period) = self.0.slow_period {
			format!("SMAC({}, {})", self.0.fast_period, slow_period)
		} else {
			format!("P-SMA({})", self.0.fast_period)
		}
	}

	fn next(&mut self, record: &OhlcRecord) {
		let calculate = |period: usize, buffer: &VecDeque<f64>| -> f64 {
			let sum: f64 = buffer.iter().take(period).sum();
			let average = sum / (period as f64);
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