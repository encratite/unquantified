use unq_common::ohlc::OhlcRecord;
use crate::buffer::IndicatorBuffer;
use crate::id::IndicatorId;
use crate::technical::*;

#[derive(Clone)]
pub struct MovingAverageConvergence {
	signal_period: usize,
	fast_period: usize,
	slow_period: usize,
	close_buffer: IndicatorBuffer,
	signal_buffer: IndicatorBuffer,
	indicators: Option<(f64, f64)>
}

impl MovingAverageConvergence {
	pub const ID: &'static str = "macd";

	pub fn new(signal_period: usize, fast_period: usize, slow_period: usize) -> anyhow::Result<Self> {
		validate_signal_parameters(signal_period, fast_period, slow_period)?;
		let close_buffer_size = EMA_BUFFER_SIZE_MULTIPLIER * fast_period.max(slow_period);
		let signal_buffer_size = EMA_BUFFER_SIZE_MULTIPLIER * signal_period;
		let output = Self {
			signal_period,
			fast_period,
			slow_period,
			close_buffer: IndicatorBuffer::new(close_buffer_size),
			signal_buffer: IndicatorBuffer::new(signal_buffer_size),
			indicators: None
		};
		Ok(output)
	}

	pub fn get_id(signal_period: usize, fast_period: usize, slow_period: usize) -> IndicatorId {
		IndicatorId::from_signal_fast_slow(Self::ID, signal_period, fast_period, slow_period)
	}

	fn calculate(&self) -> (f64, f64) {
		let signal_iter = self.signal_buffer.buffer.iter();
		let signal = exponential_moving_average(signal_iter, self.signal_period);
		let close_buffer = &self.close_buffer.buffer;
		let fast_ema = exponential_moving_average(close_buffer.iter(), self.fast_period);
		let slow_ema = exponential_moving_average(close_buffer.iter(), self.slow_period);
		let macd = fast_ema - slow_ema;
		(signal, macd)
	}
}

impl Indicator for MovingAverageConvergence {
	fn get_description(&self) -> String {
		format!("MACD({}, {}, {})", self.signal_period, self.fast_period, self.slow_period)
	}

	fn next(&mut self, record: &OhlcRecord) {
		self.close_buffer.add(record.close);
		if !self.close_buffer.filled() {
			return;
		}
		let (signal, macd) = self.calculate();
		self.signal_buffer.add(macd);
		if !self.signal_buffer.filled() {
			return;
		}
		self.indicators = Some((signal, macd));
	}

	fn get_indicators(&self) -> Option<Vec<f64>> {
		get_dual_indicators(&self.indicators)
	}

	fn get_trade_signal(&self, _: PositionState) -> Option<TradeSignal> {
		get_difference_trade_signal(&self.indicators)
	}

	fn needs_initialization(&self) -> Option<usize> {
		needs_initialization(&self.close_buffer, &self.signal_buffer)
	}

	fn clone_box(&self) -> Box<dyn Indicator> {
		Box::new(self.clone())
	}
}