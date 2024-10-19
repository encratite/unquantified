use rhai::Dynamic;
use unq_common::ohlc::OhlcRecord;
use crate::buffer::IndicatorBuffer;
use crate::id::IndicatorId;
use crate::technical::*;

#[derive(Clone)]
pub struct AverageTrueRange {
	previous_record: Option<OhlcRecord>,
	true_range_buffer: IndicatorBuffer
}

impl AverageTrueRange {
	pub const ID: &'static str = "atr";

	pub fn new(period: usize) -> anyhow::Result<Self> {
		validate_period(period)?;
		let output = Self {
			previous_record: None,
			true_range_buffer: IndicatorBuffer::new(period)
		};
		Ok(output)
	}

	pub fn get_id(period: usize) -> IndicatorId {
		IndicatorId::from_period(Self::ID, period)
	}

	pub fn get_true_range(record: &OhlcRecord, previous_close: f64) -> f64 {
		let part1 = record.high - record.low;
		let part2 = (record.high - previous_close).abs();
		let part3 = (record.low - previous_close).abs();
		let true_range = part1.max(part2).max(part3);
		true_range
	}
}

impl Indicator for AverageTrueRange {
	fn get_description(&self) -> String {
		format!("ATR({})", self.true_range_buffer.size)
	}

	fn next(&mut self, record: &OhlcRecord) {
		if let Some(previous_record) = &self.previous_record {
			let true_range = AverageTrueRange::get_true_range(record, previous_record.close);
			self.true_range_buffer.add(true_range);
		}
		self.previous_record = Some(record.clone());
	}

	fn get_indicators(&self) -> Option<Dynamic> {
		if self.true_range_buffer.filled() {
			let atr = self.true_range_buffer.average();
			Some(atr.into())
		} else {
			None
		}
	}

	fn get_trade_signal(&self, _: PositionState) -> Option<TradeSignal> {
		None
	}

	fn needs_initialization(&self) -> Option<usize> {
		match self.true_range_buffer.needs_initialization() {
			Some(_) => Some(self.true_range_buffer.size + 1),
			None => None
		}
	}

	fn clone_box(&self) -> Box<dyn Indicator> {
		Box::new(self.clone())
	}
}