use unq_common::ohlc::OhlcRecord;
use crate::buffer::IndicatorBuffer;
use crate::id::IndicatorId;
use crate::indicator::atr::AverageTrueRange;
use crate::technical::*;

#[derive(Clone)]
pub struct KeltnerChannel {
	multiplier: f64,
	exit_mode: ChannelExitMode,
	close_buffer: IndicatorBuffer,
	true_range_buffer: IndicatorBuffer,
	indicators: Option<(f64, f64, f64)>
}

impl KeltnerChannel {
	pub const ID: &'static str = "keltner";

	pub fn new(period: usize, multiplier: f64, exit_mode: ChannelExitMode) -> anyhow::Result<Self> {
		validate_period(period)?;
		validate_multiplier(multiplier)?;
		let output = Self {
			multiplier,
			exit_mode,
			close_buffer: IndicatorBuffer::new(period),
			true_range_buffer: IndicatorBuffer::new(period),
			indicators: None
		};
		Ok(output)
	}

	pub fn get_id(period: usize, multiplier: f64) -> IndicatorId {
		IndicatorId::from_period_multiplier(Self::ID, period, multiplier)
	}
}

impl Indicator for KeltnerChannel {
	fn get_description(&self) -> String {
		format!("Keltner({}, {:.1}, {})", self.close_buffer.size, self.multiplier, self.exit_mode)
	}

	fn next(&mut self, record: &OhlcRecord) {
		if let Some(previous_close) = self.close_buffer.buffer.front() {
			let true_range = AverageTrueRange::get_true_range(record, *previous_close);
			self.true_range_buffer.add(true_range);
		}
		let close = record.close;
		self.close_buffer.add(close);
		if !self.close_buffer.filled() || !self.true_range_buffer.filled() {
			return;
		}
		let center = exponential_moving_average(self.close_buffer.buffer.iter(), self.close_buffer.size);
		let average_true_range = self.true_range_buffer.average();
		let multiplier_range = self.multiplier * average_true_range;
		let lower = center - multiplier_range;
		let upper = center + multiplier_range;
		self.indicators = Some((center, lower, upper));
	}

	fn get_indicators(&self) -> Option<Vec<f64>> {
		get_channel_indicators(&self.indicators)
	}

	fn get_trade_signal(&self, state: PositionState) -> Option<TradeSignal> {
		get_channel_trade_signal(&self.close_buffer, &self.indicators, &self.exit_mode, state)
	}

	fn needs_initialization(&self) -> Option<usize> {
		match self.close_buffer.needs_initialization() {
			Some(size) => Some(size + 1),
			None => None
		}
	}

	fn clone_box(&self) -> Box<dyn Indicator> {
		Box::new(self.clone())
	}
}