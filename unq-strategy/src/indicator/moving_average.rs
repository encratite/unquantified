use std::collections::VecDeque;
use anyhow::{bail, Result};
use rhai::Dynamic;
use unq_common::ohlc::OhlcRecord;
use crate::buffer::IndicatorBuffer;
use crate::technical::*;

#[derive(Clone)]
pub struct MovingAverage {
	pub fast_period: usize,
	pub slow_period: Option<usize>,
	pub buffer: IndicatorBuffer,
	pub fast_average: Option<f64>,
	pub slow_average: Option<f64>,
	pub trade_signal: Option<TradeSignal>
}

impl MovingAverage {
	pub fn new(fast_period: usize, slow_period: Option<usize>, buffer_size_multiplier: usize) -> Result<Self> {
		if buffer_size_multiplier < 1 || buffer_size_multiplier > 5 {
			bail!("Invalid buffer size multiplier specified ({buffer_size_multiplier}");
		}
		validate_fast_slow_parameters(fast_period, slow_period)?;
		let output = Self {
			fast_period,
			slow_period,
			buffer: IndicatorBuffer::with_slow(fast_period, slow_period, buffer_size_multiplier),
			fast_average: None,
			slow_average: None,
			trade_signal: None
		};
		Ok(output)
	}

	pub fn calculate_averages(&mut self, record: &OhlcRecord, calculate: &dyn Fn(usize, &VecDeque<f64>) -> f64) {
		self.buffer.add(record.close);
		if !self.buffer.filled() {
			return;
		}
		let buffer = &self.buffer.buffer;
		let fast_average = calculate(self.fast_period, buffer);
		self.fast_average = Some(fast_average);
		let difference = if let Some(slow_period) = self.slow_period {
			let slow_average = calculate(slow_period, buffer);
			self.slow_average = Some(slow_average);
			fast_average - slow_average
		} else {
			let price = *buffer.front().unwrap();
			price - fast_average
		};
		self.trade_signal = translate_signal(difference);
	}

	pub fn get_indicators(&self) -> Option<Dynamic> {
		match self.fast_average {
			Some(fast_average) => Some(fast_average.into()),
			None => None
		}
	}
}