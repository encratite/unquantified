use anyhow::{bail, Result};
use rhai::{CustomType, Dynamic, TypeBuilder};
use strum_macros::Display;
use unq_common::ohlc::OhlcRecord;
use crate::buffer::IndicatorBuffer;

pub const EMA_BUFFER_SIZE_MULTIPLIER: usize = 2;

#[derive(PartialEq, Clone, Debug)]
pub enum TradeSignal {
	Long,
	Close,
	Short
}

#[derive(PartialEq, Debug)]
pub enum PositionState {
	Long,
	None,
	Short
}

#[derive(PartialEq, Debug, Clone, Display)]
pub enum ChannelExitMode {
	#[strum(serialize = "center")]
	Center,
	#[strum(serialize = "opposite")]
	Opposite
}

pub trait Indicator: Send + Sync {
	fn get_description(&self) -> String;
	fn next(&mut self, record: &OhlcRecord);
	fn get_indicators(&self) -> Option<Dynamic>;
	fn get_trade_signal(&self, state: PositionState) -> Option<TradeSignal>;
	fn needs_initialization(&self) -> Option<usize>;
	fn clone_box(&self) -> Box<dyn Indicator>;

	fn initialize(&mut self, records: &Vec<OhlcRecord>) {
		for record in records.iter().rev() {
			let _ = self.next(record);
		}
	}
}

#[derive(Clone, CustomType)]
pub struct AverageDifference {
	#[rhai_type(readonly)]
	pub average: f64,
	#[rhai_type(readonly)]
	pub difference: f64
}

impl AverageDifference {
	pub fn new(indicators: Option<(f64, f64)>) -> Option<Dynamic> {
		match indicators {
			Some((average, difference)) => {
				let indicators = Self {
					average,
					difference
				};
				Some(Dynamic::from(indicators))
			},
			None => None
		}
	}
}

#[derive(Clone, CustomType)]
pub struct ChannelIndicators {
	#[rhai_type(readonly)]
	pub center: f64,
	#[rhai_type(readonly)]
	pub lower: f64,
	#[rhai_type(readonly)]
	pub upper: f64
}

impl ChannelIndicators {
	pub fn new(indicators: Option<(f64, f64, f64)>) -> Option<Dynamic> {
		match indicators {
			Some((center, lower, upper)) => {
				let indicators = Self {
					center,
					lower,
					upper
				};
				Some(Dynamic::from(indicators))
			},
			None => None
		}
	}
}

pub fn exponential_moving_average<'a, I>(records: I, period: usize) -> f64
where
	I: Iterator<Item = &'a f64>
{
	let mut average = 0.0;
	let mut i = 0;
	let lambda = 2.0 / ((period + 1) as f64);
	for x in records.take(period) {
		average += lambda * (1.0 - lambda).powi(i) * x;
		i += 1;
	}
	average
}

pub fn validate_period(period: usize) -> Result<()> {
	if period < 2 {
		bail!("Invalid period for indicator");
	}
	Ok(())
}

pub fn validate_fast_slow_parameters(fast_period: usize, slow_period: Option<usize>) -> Result<()> {
	validate_period(fast_period)?;
	if let Some(slow) = slow_period {
		validate_period(slow)?;
		if slow <= fast_period {
			bail!("Invalid combination of fast period ({fast_period}) and slow period ({slow}) for indicator");
		}
	}
	Ok(())
}

pub fn validate_signal_parameters(signal_period: usize, fast_period: usize, slow_period: usize) -> Result<()> {
	if signal_period >= fast_period || fast_period >= slow_period {
		bail!("Invalid combination of signal periods ({signal_period}, {fast_period}, {slow_period})");
	}
	Ok(())
}

pub fn validate_multiplier(multiplier: f64) -> Result<()> {
	if multiplier <= 0.0 {
		bail!("Multiplier ({multiplier}) is too low");
	}
	Ok(())
}

pub fn translate_signal(signal: f64) -> Option<TradeSignal> {
	if signal > 0.0 {
		Some(TradeSignal::Long)
	} else if signal < 0.0 {
		Some(TradeSignal::Short)
	} else {
		Some(TradeSignal::Close)
	}
}

pub fn translate_channel_signal(close: f64, center: f64, lower: f64, upper: f64, state: PositionState, exit_mode: &ChannelExitMode) -> TradeSignal {
	if close >= upper {
		TradeSignal::Long
	} else if close <= lower {
		TradeSignal::Short
	} else {
		if *exit_mode == ChannelExitMode::Center {
			if state == PositionState::Long && close > center {
				TradeSignal::Long
			} else if state == PositionState::Short && close < center {
				TradeSignal::Short
			} else {
				TradeSignal::Close
			}
		} else {
			TradeSignal::Close
		}
	}
}

pub fn needs_initialization_sum(close_buffer: &IndicatorBuffer, signal_buffer: &IndicatorBuffer) -> Option<usize> {
	let close = close_buffer.needs_initialization();
	let signal = signal_buffer.needs_initialization();
	match (close, signal) {
		(Some(x), Some(y)) => Some(x + y),
		(Some(x), None) => Some(x),
		(None, Some(y)) => Some(y),
		(None, None) => None,
	}
}

pub fn get_difference_trade_signal(indicators: &Option<(f64, f64)>) -> Option<TradeSignal> {
	match indicators {
		Some((first, second)) => translate_signal(first - second),
		None => None
	}
}

pub fn get_channel_trade_signal(buffer: &IndicatorBuffer, indicators: &Option<(f64, f64, f64)>, exit_mode: &ChannelExitMode, state: PositionState) -> Option<TradeSignal> {
	match (buffer.buffer.front(), indicators) {
		(Some(close), Some((center, lower, upper))) => {
			let signal = translate_channel_signal(*close, *center, *lower, *upper, state, exit_mode);
			Some(signal)
		}
		_ => None
	}
}