use rhai::Dynamic;
use unq_common::ohlc::OhlcRecord;
use crate::buffer::IndicatorBuffer;
use crate::id::IndicatorId;
use crate::technical::*;

#[derive(Clone)]
pub struct RateOfChange {
	buffer: IndicatorBuffer
}

impl RateOfChange {
	pub const ID: &'static str = "roc";

	pub fn new(period: usize) -> anyhow::Result<Self> {
		validate_period(period)?;
		let output = Self {
			buffer: IndicatorBuffer::new(period)
		};
		Ok(output)
	}

	pub fn get_id(period: usize) -> IndicatorId {
		IndicatorId::from_period(Self::ID, period)
	}
}

impl Indicator for RateOfChange {
	fn get_description(&self) -> String {
		format!("ROC({})", self.buffer.size)
	}

	fn next(&mut self, record: &OhlcRecord) {
		self.buffer.add(record.close);
	}

	fn get_indicators(&self) -> Option<Dynamic> {
		if self.buffer.filled() {
			match (self.buffer.buffer.front(), self.buffer.buffer.iter().last()) {
				(Some(first), Some(last)) => {
					let rate = 100.0 * (first / last - 1.0);
					Some(rate.into())
				},
				_ => None
			}
		} else {
			None
		}
	}

	fn get_trade_signal(&self, _: PositionState) -> Option<TradeSignal> {
		None
	}

	fn needs_initialization(&self) -> Option<usize> {
		self.buffer.needs_initialization()
	}

	fn clone_box(&self) -> Box<dyn Indicator> {
		Box::new(self.clone())
	}
}