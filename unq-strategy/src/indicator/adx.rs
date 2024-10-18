use unq_common::ohlc::OhlcRecord;
use crate::buffer::IndicatorBuffer;
use crate::id::IndicatorId;
use crate::indicator::atr::AverageTrueRange;
use crate::technical::*;

#[derive(Clone)]
pub struct AverageDirectionalIndex {
	period: usize,
	previous_record: Option<OhlcRecord>,
	true_range_buffer: IndicatorBuffer,
	plus_dm_buffer: IndicatorBuffer,
	minus_dm_buffer: IndicatorBuffer,
	dx_buffer: IndicatorBuffer
}

impl AverageDirectionalIndex {
	pub const ID: &'static str = "adx";

	pub fn new(period: usize) -> anyhow::Result<Self> {
		validate_period(period)?;
		let output = Self {
			period,
			previous_record: None,
			true_range_buffer: IndicatorBuffer::new(period),
			plus_dm_buffer: IndicatorBuffer::new(period),
			minus_dm_buffer: IndicatorBuffer::new(period),
			dx_buffer: IndicatorBuffer::new(period),
		};
		Ok(output)
	}

	pub fn get_id(period: usize) -> IndicatorId {
		IndicatorId::from_period(Self::ID, period)
	}
}

impl Indicator for AverageDirectionalIndex {
	fn get_description(&self) -> String {
		format!("ADX({})", self.period)
	}

	fn next(&mut self, record: &OhlcRecord) {
		if let Some(previous_record) = &self.previous_record {
			let true_range = AverageTrueRange::get_true_range(record, previous_record.close);
			self.true_range_buffer.add(true_range);
			let high_difference = record.high - previous_record.high;
			let low_difference = previous_record.low - record.low;
			let plus_dm = if high_difference > low_difference && high_difference > 0.0 {
				high_difference
			} else {
				0.0
			};
			let minus_dm = if low_difference > high_difference && low_difference > 0.0 {
				low_difference
			} else {
				0.0
			};
			self.plus_dm_buffer.add(plus_dm);
			self.minus_dm_buffer.add(minus_dm);
			if self.plus_dm_buffer.filled() && self.minus_dm_buffer.filled() && self.true_range_buffer.filled() {
				let average_true_range = self.true_range_buffer.average();
				let plus_di = self.plus_dm_buffer.average() / average_true_range;
				let minus_di =  self.minus_dm_buffer.average() / average_true_range;
				let dx = (plus_di - minus_di).abs() / (plus_di + minus_di);
				self.dx_buffer.add(dx);
			}
		}
		self.previous_record = Some(record.clone());
	}

	fn get_indicators(&self) -> Option<Vec<f64>> {
		if self.dx_buffer.filled() {
			let adx = 100.0 * self.dx_buffer.average();
			let indicators = vec![adx];
			Some(indicators)
		} else {
			None
		}
	}

	fn get_trade_signal(&self, _: PositionState) -> Option<TradeSignal> {
		None
	}

	fn needs_initialization(&self) -> Option<usize> {
		match self.plus_dm_buffer.needs_initialization() {
			Some(_) => Some(self.period + 1),
			None => None
		}
	}

	fn clone_box(&self) -> Box<dyn Indicator> {
		Box::new(self.clone())
	}
}