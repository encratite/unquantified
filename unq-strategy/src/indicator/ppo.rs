use rhai::Dynamic;
use unq_common::ohlc::OhlcRecord;
use crate::buffer::IndicatorBuffer;
use crate::id::IndicatorId;
use crate::technical::*;

#[derive(Clone)]
pub struct PercentagePriceOscillator {
	signal_period: usize,
	fast_period: usize,
	slow_period: usize,
	close_buffer: IndicatorBuffer,
	signal_buffer: IndicatorBuffer,
	indicators: Option<(f64, f64)>
}

impl PercentagePriceOscillator {
	pub const ID: &'static str = "ppo";

	pub fn new(signal_period: usize, fast_period: usize, slow_period: usize) -> anyhow::Result<Self> {
		validate_signal_parameters(signal_period, fast_period, slow_period)?;
		let close_buffer_size = fast_period.max(slow_period);
		let output = Self {
			signal_period,
			fast_period,
			slow_period,
			close_buffer: IndicatorBuffer::new(close_buffer_size),
			signal_buffer: IndicatorBuffer::new(signal_period),
			indicators: None
		};
		Ok(output)
	}

	pub fn get_id(signal_period: usize, fast_period: usize, slow_period: usize) -> IndicatorId {
		IndicatorId::from_signal_fast_slow(Self::ID, signal_period, fast_period, slow_period)
	}

	fn calculate(&self) -> f64 {
		let buffer = &self.close_buffer.buffer;
		let fast_ema = exponential_moving_average(buffer.iter(), self.fast_period);
		let slow_ema = exponential_moving_average(buffer.iter(), self.slow_period);
		let ppo = 100.0 * (fast_ema - slow_ema) / slow_ema;
		ppo
	}
}

impl Indicator for PercentagePriceOscillator {
	fn get_description(&self) -> String {
		format!("PPO({}, {}, {})", self.signal_period, self.fast_period, self.slow_period)
	}

	fn next(&mut self, record: &OhlcRecord) {
		self.close_buffer.add(record.close);
		if !self.close_buffer.filled() {
			return;
		}
		let ppo = self.calculate();
		self.signal_buffer.add(ppo);
		if !self.signal_buffer.filled() {
			return;
		}
		let signal = exponential_moving_average(self.signal_buffer.buffer.iter(), self.signal_period);
		self.indicators = Some((signal, ppo));
	}

	fn get_indicators(&self) -> Option<Dynamic> {
		AverageDifference::new(self.indicators)
	}

	fn get_trade_signal(&self, _: PositionState) -> Option<TradeSignal> {
		get_difference_trade_signal(&self.indicators)
	}

	fn needs_initialization(&self) -> Option<usize> {
		needs_initialization_sum(&self.close_buffer, &self.signal_buffer)
	}

	fn clone_box(&self) -> Box<dyn Indicator> {
		Box::new(self.clone())
	}
}