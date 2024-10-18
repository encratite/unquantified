use unq_common::ohlc::OhlcRecord;
use unq_common::stats::mean;
use crate::buffer::IndicatorBuffer;
use crate::id::IndicatorId;
use crate::technical::*;

#[derive(Clone)]
pub struct RelativeStrengthIndicator {
	period: usize,
	low_threshold: f64,
	high_threshold: f64,
	buffer: IndicatorBuffer,
	indicator: Option<f64>
}

impl RelativeStrengthIndicator {
	pub const ID: &'static str = "rsi";

	pub fn new(period: usize, low_threshold: f64, high_threshold: f64) -> anyhow::Result<Self> {
		validate_period(period)?;
		let output = Self {
			period,
			low_threshold,
			high_threshold,
			buffer: IndicatorBuffer::new(period + 1),
			indicator: None
		};
		Ok(output)
	}

	pub fn get_id(period: usize) -> IndicatorId {
		IndicatorId::from_period(Self::ID, period)
	}

	fn calculate(&self) -> f64 {
		let mut up = Vec::new();
		let mut down = Vec::new();
		let buffer = &self.buffer.buffer;
		let mut previous_close = buffer.iter().last().unwrap();
		for close in buffer.iter().rev() {
			let difference = close - previous_close;
			if difference >= 0.0 {
				up.push(difference)
			} else {
				down.push(- difference)
			}
			previous_close = close;
		}
		let up_mean = mean(up.iter()).unwrap_or(0.0);
		let down_mean = mean(down.iter()).unwrap_or(0.0);
		let rsi = 100.0 * up_mean / (up_mean + down_mean);
		rsi
	}
}

impl Indicator for RelativeStrengthIndicator {
	fn get_description(&self) -> String {
		format!("RSI({}, {}, {})", self.period, self.low_threshold, self.high_threshold)
	}

	fn next(&mut self, record: &OhlcRecord) {
		self.buffer.add(record.close);
		if !self.buffer.filled() {
			return;
		}
		let rsi = self.calculate();
		self.indicator = Some(rsi);
	}

	fn get_indicators(&self) -> Option<Vec<f64>> {
		match self.indicator {
			Some(rsi) => Some(vec![rsi]),
			None => None
		}
	}

	fn get_trade_signal(&self, state: PositionState) -> Option<TradeSignal> {
		if !self.buffer.filled() {
			return None;
		}
		let rsi = self.calculate();
		match state {
			PositionState::Long => {
				if rsi > self.high_threshold  {
					Some(TradeSignal::Close)
				} else {
					None
				}
			},
			PositionState::Short => {
				if rsi < self.low_threshold  {
					Some(TradeSignal::Close)
				} else {
					None
				}
			},
			_ => {
				if rsi > self.high_threshold  {
					Some(TradeSignal::Short)
				} else if rsi < self.low_threshold {
					Some(TradeSignal::Long)
				} else {
					None
				}
			}
		}
	}

	fn needs_initialization(&self) -> Option<usize> {
		self.buffer.needs_initialization()
	}

	fn clone_box(&self) -> Box<dyn Indicator> {
		Box::new(self.clone())
	}
}