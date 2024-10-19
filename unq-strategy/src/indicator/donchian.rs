use rhai::Dynamic;
use unq_common::ohlc::OhlcRecord;
use crate::buffer::IndicatorBuffer;
use crate::id::IndicatorId;
use crate::technical::*;

#[derive(Clone)]
pub struct DonchianChannel {
	exit_mode: ChannelExitMode,
	buffer: IndicatorBuffer,
	indicators: Option<(f64, f64, f64)>
}

impl DonchianChannel {
	pub const ID: &'static str = "donchian";

	pub fn new(period: usize, exit_mode: ChannelExitMode) -> anyhow::Result<Self> {
		validate_period(period)?;
		let output = Self {
			exit_mode,
			buffer: IndicatorBuffer::new(period),
			indicators: None
		};
		Ok(output)
	}

	pub fn get_id(period: usize) -> IndicatorId {
		IndicatorId::from_period(Self::ID, period)
	}
}

impl Indicator for DonchianChannel {
	fn get_description(&self) -> String {
		format!("Donchian({}, {})", self.buffer.size, self.exit_mode)
	}

	fn next(&mut self, record: &OhlcRecord) {
		let close = record.close;
		self.buffer.add(close);
		if !self.buffer.filled() {
			return;
		}
		let buffer = &self.buffer.buffer;
		let lower = buffer.iter().cloned().reduce(f64::min).unwrap();
		let upper = buffer.iter().cloned().reduce(f64::max).unwrap();
		let center = (lower + upper) / 2.0;
		self.indicators = Some((center, lower, upper));
	}

	fn get_indicators(&self) -> Option<Dynamic> {
		ChannelIndicators::new(self.indicators)
	}

	fn get_trade_signal(&self, state: PositionState) -> Option<TradeSignal> {
		get_channel_trade_signal(&self.buffer, &self.indicators, &self.exit_mode, state)
	}

	fn needs_initialization(&self) -> Option<usize> {
		match self.buffer.needs_initialization() {
			Some(size) => Some(size + 1),
			None => None
		}
	}

	fn clone_box(&self) -> Box<dyn Indicator> {
		Box::new(self.clone())
	}
}