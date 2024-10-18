use std::collections::VecDeque;
use unq_common::ohlc::OhlcRecord;
use crate::id::IndicatorId;
use crate::indicator::moving_average::MovingAverage;
use crate::technical::*;

#[derive(Clone)]
pub struct ExponentialMovingAverage(MovingAverage);

impl ExponentialMovingAverage {
	pub const ID: &'static str = "p-ema";
	pub const CROSSOVER_ID: &'static str = "emac";

	pub fn new(fast_period: usize, slow_period: Option<usize>) -> anyhow::Result<Self> {
		// Increase the buffer size to twice the normal size for moving averages
		let moving_average = MovingAverage::new(fast_period, slow_period, EMA_BUFFER_SIZE_MULTIPLIER)?;
		let output = ExponentialMovingAverage(moving_average);
		Ok(output)
	}

	pub fn get_id(period: usize) -> IndicatorId {
		IndicatorId::from_period("ema", period)
	}

	fn calculate(period: usize, buffer: &VecDeque<f64>) -> f64 {
		let mut sum = 0.0;
		let mut coefficient_sum = 0.0;
		let mut i = 0;
		let lambda = 2.0 / ((period + 1) as f64);
		for x in buffer.iter().take(period) {
			let coefficient = lambda * (1.0 - lambda).powi(i);
			sum += coefficient * x;
			coefficient_sum += coefficient;
			i += 1;
		}
		// Normalize the weights to 1.0 to avoid P-EMA distortion with limited buffer size
		let average = sum / coefficient_sum;
		average
	}
}

impl Indicator for ExponentialMovingAverage {
	fn get_description(&self) -> String {
		if let Some(slow_period) = self.0.slow_period {
			format!("EMAC({}, {})", self.0.fast_period, slow_period)
		} else {
			format!("P-EMA({})", self.0.fast_period)
		}
	}

	fn next(&mut self, record: &OhlcRecord) {
		let calculate = ExponentialMovingAverage::calculate;
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