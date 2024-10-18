use anyhow::Result;
use unq_common::ohlc::OhlcRecord;
use crate::buffer::IndicatorBuffer;
use crate::id::IndicatorId;
use crate::technical::*;

#[derive(Clone)]
pub struct MomentumIndicator {
	buffer: IndicatorBuffer,
	indicators: Option<(f64, f64)>
}

impl MomentumIndicator {
	pub const ID: &'static str = "momentum";

	pub fn new(period: usize) -> Result<Self> {
		validate_period(period)?;
		let output = Self {
			buffer: IndicatorBuffer::new(period),
			indicators: None
		};
		Ok(output)
	}

	pub fn get_id(period: usize) -> IndicatorId {
		IndicatorId::from_period(Self::ID, period)
	}
}

impl Indicator for MomentumIndicator {
	fn get_description(&self) -> String {
		format!("Momentum({})", self.buffer.size)
	}

	fn next(&mut self, record: &OhlcRecord) {
		let buffer = &mut self.buffer;
		buffer.add(record.close);
		if !buffer.filled() {
			return;
		}
		if let (Some(first), Some(last)) = (buffer.buffer.front(), buffer.buffer.iter().last()) {
			self.indicators = Some((*first, *last));
		}
	}

	fn get_indicators(&self) -> Option<Vec<f64>> {
		match self.indicators {
			Some((_, last)) => Some(vec![last]),
			_ => None
		}
	}

	fn get_trade_signal(&self, _: PositionState) -> Option<TradeSignal> {
		match self.indicators {
			Some((first, last)) => {
				let momentum = first - last;
				translate_signal(momentum)
			},
			None => None
		}
	}

	fn needs_initialization(&self) -> Option<usize> {
		self.buffer.needs_initialization()
	}

	fn clone_box(&self) -> Box<dyn Indicator> {
		Box::new(self.clone())
	}
}