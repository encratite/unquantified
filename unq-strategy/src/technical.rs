use std::collections::VecDeque;
use anyhow::{bail, Result};
use unq_common::ohlc::OhlcRecord;
use unq_common::stats::{mean, standard_deviation_mean_biased};

const EMA_BUFFER_SIZE_MULTIPLIER: usize = 2;

#[derive(PartialEq, Debug)]
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

pub trait Indicator: Send + Sync {
	fn next(&mut self, record: &OhlcRecord, state: PositionState) -> Option<TradeSignal>;
	fn needs_initialization(&self) -> Option<usize>;
	fn clone_box(&self) -> Box<dyn Indicator>;

	fn initialize(&mut self, records: &Vec<&OhlcRecord>) {
		for record in records {
			let _ = self.next(record, PositionState::None);
		}
	}
}

#[derive(Clone)]
struct IndicatorBuffer {
	buffer: VecDeque<f64>,
	size: usize
}

impl IndicatorBuffer {
	pub fn new(size: usize) -> Self {
		Self {
			buffer: VecDeque::new(),
			size
		}
	}

	pub fn with_slow(fast_size: usize, slow_size: Option<usize>, multiplier: usize) -> Self {
		let max_size = if let Some(slow) = slow_size {
			fast_size.max(slow)
		} else {
			fast_size
		};
		Self {
			buffer: VecDeque::new(),
			size: multiplier * max_size
		}
	}

	pub fn add(&mut self, sample: f64) -> bool {
		self.buffer.push_back(sample);
		if self.buffer.len() > self.size {
			self.buffer.pop_front();
			true
		} else {
			false
		}
	}

	pub fn needs_initialization(&self) -> Option<usize> {
		if self.buffer.len() < self.size {
			Some(self.size)
		} else {
			None
		}
	}
}

#[derive(Clone)]
pub struct MomentumIndicator(IndicatorBuffer);

impl MomentumIndicator {
	pub const ID: &'static str = "momentum";

	pub fn new(period: usize) -> Result<Self> {
		validate_period(period)?;
		let output = Self(IndicatorBuffer::new(period));
		Ok(output)
	}
}

impl Indicator for MomentumIndicator {
	fn next(&mut self, record: &OhlcRecord, _: PositionState) -> Option<TradeSignal> {
		let filled = self.0.add(record.close);
		if !filled {
			return None;
		}
		let buffer = &self.0.buffer;
		let first = buffer.front().unwrap();
		let last = buffer.iter().last().unwrap();
		let momentum = first - last;
		translate_signal(momentum)
	}

	fn needs_initialization(&self) -> Option<usize> {
		self.0.needs_initialization()
	}

	fn clone_box(&self) -> Box<dyn Indicator> {
		Box::new(self.clone())
	}
}

#[derive(Clone)]
struct MovingAverage {
	fast_period: usize,
	slow_period: Option<usize>,
	buffer: IndicatorBuffer
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
			buffer: IndicatorBuffer::with_slow(fast_period, slow_period, buffer_size_multiplier)
		};
		Ok(output)
	}

	fn calculate_next(&mut self, record: &OhlcRecord, calculate: &dyn Fn(usize, &VecDeque<f64>) -> f64) -> Option<TradeSignal> {
		let filled = self.buffer.add(record.close);
		if !filled {
			return None;
		}
		let buffer = &self.buffer.buffer;
		let fast_average = calculate(self.fast_period, buffer);
		let difference = if let Some(slow_period) = self.slow_period {
			let slow_average = calculate(slow_period, buffer);
			fast_average - slow_average
		} else {
			let price = *buffer.front().unwrap();
			price - fast_average
		};
		translate_signal(difference)
	}
}

#[derive(Clone)]
pub struct SimpleMovingAverage(MovingAverage);

impl SimpleMovingAverage {
	pub const ID: &'static str = "p-sma";
	pub const CROSSOVER_ID: &'static str = "smac";

	pub fn new(fast_period: usize, slow_period: Option<usize>) -> Result<Self> {
		let moving_average = MovingAverage::new(fast_period, slow_period, 1)?;
		let output = SimpleMovingAverage(moving_average);
		Ok(output)
	}
}

impl Indicator for SimpleMovingAverage {
	fn next(&mut self, record: &OhlcRecord, _: PositionState) -> Option<TradeSignal> {
		let calculate = |period: usize, buffer: &VecDeque<f64>| -> f64 {
			let sum: f64 = buffer.iter().take(period).sum();
			let average = sum / (period as f64);
			average
		};
		self.0.calculate_next(record, &calculate)
	}

	fn needs_initialization(&self) -> Option<usize> {
		self.0.buffer.needs_initialization()
	}

	fn clone_box(&self) -> Box<dyn Indicator> {
		Box::new(self.clone())
	}
}

#[derive(Clone)]
pub struct LinearMovingAverage(MovingAverage);

impl LinearMovingAverage {
	pub const ID: &'static str = "p-lma";
	pub const CROSSOVER_ID: &'static str = "lmac";

	pub fn new(fast_period: usize, slow_period: Option<usize>) -> Result<Self> {
		let moving_average = MovingAverage::new(fast_period, slow_period, 1)?;
		let output = LinearMovingAverage(moving_average);
		Ok(output)
	}
}

impl Indicator for LinearMovingAverage {
	fn next(&mut self, record: &OhlcRecord, _: PositionState) -> Option<TradeSignal> {
		let calculate = |period: usize, buffer: &VecDeque<f64>| -> f64 {
			let mut average = 0.0;
			let mut i = 0;
			for x in buffer.iter().take(period) {
				average += ((period - i) as f64) * x;
				i += 1;
			}
			average /= ((period * (period + 1)) as f64) / 2.0;
			average
		};
		self.0.calculate_next(record, &calculate)
	}

	fn needs_initialization(&self) -> Option<usize> {
		self.0.buffer.needs_initialization()
	}

	fn clone_box(&self) -> Box<dyn Indicator> {
		Box::new(self.clone())
	}
}

#[derive(Clone)]
pub struct ExponentialMovingAverage(MovingAverage);

impl ExponentialMovingAverage {
	pub const ID: &'static str = "e-lma";
	pub const CROSSOVER_ID: &'static str = "emac";

	pub fn new(fast_period: usize, slow_period: Option<usize>) -> Result<Self> {
		// Increase the buffer size to twice the normal size for moving averages
		let moving_average = MovingAverage::new(fast_period, slow_period, EMA_BUFFER_SIZE_MULTIPLIER)?;
		let output = ExponentialMovingAverage(moving_average);
		Ok(output)
	}

	pub fn calculate(period: usize, buffer: &VecDeque<f64>) -> f64 {
		let mut sum = 0.0;
		let mut coefficient_sum = 0.0;
		let mut i = 0;
		let lambda = 2.0 / ((period + 1) as f64);
		for x in buffer.iter().take(period) {
			let coefficient = lambda * (1.0 - lambda).powi(i);
			sum += coefficient * x;
			coefficient_sum += coefficient;
			i += 1;
		}
		// Normalize the weights to 1.0 to avoid P-EMA distortion with limited buffer size
		let average = sum / coefficient_sum;
		average
	}
}

impl Indicator for ExponentialMovingAverage {
	fn next(&mut self, record: &OhlcRecord, _: PositionState) -> Option<TradeSignal> {
		let calculate = ExponentialMovingAverage::calculate;
		self.0.calculate_next(record, &calculate)
	}

	fn needs_initialization(&self) -> Option<usize> {
		self.0.buffer.needs_initialization()
	}

	fn clone_box(&self) -> Box<dyn Indicator> {
		Box::new(self.clone())
	}
}

#[derive(Clone)]
pub struct RelativeStrengthIndicator {
	upper_band: f64,
	lower_band: f64,
	buffer: IndicatorBuffer,
}

impl RelativeStrengthIndicator {
	pub const ID: &'static str = "rsi";

	pub fn new(period: usize, high_threshold: f64, low_threshold: f64) -> Result<Self> {
		validate_period(period)?;
		let output = Self {
			upper_band: high_threshold,
			lower_band: low_threshold,
			buffer: IndicatorBuffer::new(period + 1)
		};
		Ok(output)
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
	fn next(&mut self, record: &OhlcRecord, _: PositionState) -> Option<TradeSignal> {
		let filled = self.buffer.add(record.close);
		if filled {
			let rsi = self.calculate();
			translate_band_signal(rsi, self.upper_band, self.lower_band)
		} else {
			None
		}
	}

	fn needs_initialization(&self) -> Option<usize> {
		self.buffer.needs_initialization()
	}

	fn clone_box(&self) -> Box<dyn Indicator> {
		Box::new(self.clone())
	}
}

#[derive(Clone)]
pub struct MovingAverageConvergence {
	signal_period: usize,
	fast_period: usize,
	slow_period: usize,
	close_buffer: IndicatorBuffer,
	signal_buffer: IndicatorBuffer,
}

impl MovingAverageConvergence {
	pub const ID: &'static str = "macd";

	pub fn new(signal_period: usize, fast_period: usize, slow_period: usize) -> Result<Self> {
		validate_signal_parameters(signal_period, fast_period, slow_period)?;
		let close_buffer_size = EMA_BUFFER_SIZE_MULTIPLIER * fast_period.max(slow_period);
		let signal_buffer_size = EMA_BUFFER_SIZE_MULTIPLIER * signal_period;
		let output = Self {
			signal_period,
			fast_period,
			slow_period,
			close_buffer: IndicatorBuffer::new(close_buffer_size),
			signal_buffer: IndicatorBuffer::new(signal_buffer_size)
		};
		Ok(output)
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
	fn next(&mut self, record: &OhlcRecord, _: PositionState) -> Option<TradeSignal> {
		let close_filled = self.close_buffer.add(record.close);
		if !close_filled {
			return None;
		}
		let (signal, macd) = self.calculate();
		let signal_filled = self.signal_buffer.add(macd);
		if !signal_filled {
			return None;
		}
		translate_signal(signal - macd)
	}

	fn needs_initialization(&self) -> Option<usize> {
		needs_initialization(&self.close_buffer, &self.signal_buffer)
	}

	fn clone_box(&self) -> Box<dyn Indicator> {
		Box::new(self.clone())
	}
}

#[derive(Clone)]
pub struct PercentagePriceOscillator {
	signal_period: usize,
	fast_period: usize,
	slow_period: usize,
	close_buffer: IndicatorBuffer,
	signal_buffer: IndicatorBuffer
}

impl PercentagePriceOscillator {
	pub const ID: &'static str = "ppo";

	pub fn new(signal_period: usize, fast_period: usize, slow_period: usize) -> Result<Self> {
		validate_signal_parameters(signal_period, fast_period, slow_period)?;
		let close_buffer_size = fast_period.max(slow_period);
		let output = Self {
			signal_period,
			fast_period,
			slow_period,
			close_buffer: IndicatorBuffer::new(close_buffer_size),
			signal_buffer: IndicatorBuffer::new(signal_period)
		};
		Ok(output)
	}

	fn calculate(&self) -> f64 {
		let buffer = &self.close_buffer.buffer;
		let fast_ema = exponential_moving_average(buffer.iter(), self.fast_period);
		let slow_ema = exponential_moving_average(buffer.iter(), self.slow_period);
		let ppo = 100.0 * (fast_ema - slow_ema) / slow_ema;
		ppo
	}
}

impl Indicator for PercentagePriceOscillator {
	fn next(&mut self, record: &OhlcRecord, _: PositionState) -> Option<TradeSignal> {
		let close_filled = self.close_buffer.add(record.close);
		if !close_filled {
			return None;
		}
		let ppo = self.calculate();
		let ppo_filled = self.signal_buffer.add(ppo);
		if !ppo_filled {
			return None;
		}
		let signal = exponential_moving_average(self.signal_buffer.buffer.iter(), self.signal_period);
		translate_signal(signal - ppo)
	}

	fn needs_initialization(&self) -> Option<usize> {
		needs_initialization(&self.close_buffer, &self.signal_buffer)
	}

	fn clone_box(&self) -> Box<dyn Indicator> {
		Box::new(self.clone())
	}
}

#[derive(Clone)]
pub struct BollingerBands {
	multiplier: f64,
	buffer: IndicatorBuffer
}

impl BollingerBands {
	pub const ID: &'static str = "bollinger";

	pub fn new(period: usize, multiplier: f64) -> Result<Self> {
		validate_period(period)?;
		validate_multiplier(multiplier)?;
		let output = Self {
			multiplier,
			buffer: IndicatorBuffer::new(period)
		};
		Ok(output)
	}

	fn calculate(&self) -> (f64, f64, f64) {
		let buffer = &self.buffer.buffer;
		let center = mean(buffer.iter()).unwrap();
		let standard_deviation = standard_deviation_mean_biased(buffer.iter(), center).unwrap();
		let upper = center + self.multiplier * standard_deviation;
		let lower = center - self.multiplier * standard_deviation;
		(center, upper, lower)
	}
}

impl Indicator for BollingerBands {
	fn next(&mut self, record: &OhlcRecord, state: PositionState) -> Option<TradeSignal> {
		let close = record.close;
		let filled = self.buffer.add(close);
		if !filled {
			return None;
		}
		let (center, upper, lower) = self.calculate();
		let signal = match state {
			PositionState::None => {
				if close > upper {
					TradeSignal::Long
				} else if close < lower {
					TradeSignal::Short
				} else {
					TradeSignal::Close
				}
			},
			PositionState::Long => {
				if close > center {
					TradeSignal::Long
				} else {
					TradeSignal::Close
				}
			},
			PositionState::Short => {
				if close < center {
					TradeSignal::Short
				} else {
					TradeSignal::Close
				}
			}
		};
		Some(signal)
	}

	fn needs_initialization(&self) -> Option<usize> {
		self.buffer.needs_initialization()
	}

	fn clone_box(&self) -> Box<dyn Indicator> {
		Box::new(self.clone())
	}
}

fn exponential_moving_average<'a, I>(records: I, period: usize) -> f64
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

fn validate_period(period: usize) -> Result<()> {
	if period < 2 {
		bail!("Invalid period for indicator");
	}
	Ok(())
}

fn validate_fast_slow_parameters(fast_period: usize, slow_period: Option<usize>) -> Result<()> {
	validate_period(fast_period)?;
	if let Some(slow) = slow_period {
		validate_period(slow)?;
		if slow <= fast_period {
			bail!("Invalid combination of fast period ({fast_period}) and slow period ({slow}) for indicator");
		}
	}
	Ok(())
}

fn validate_signal_parameters(signal_period: usize, fast_period: usize, slow_period: usize) -> Result<()> {
	if signal_period >= fast_period || fast_period >= slow_period {
		bail!("Invalid combination of signal periods ({signal_period}, {fast_period}, {slow_period})");
	}
	Ok(())
}

fn validate_multiplier(multiplier: f64) -> Result<()> {
	if multiplier <= 0.0 {
		bail!("Multiplier ({multiplier}) is too low");
	}
	Ok(())
}

fn translate_signal(signal: f64) -> Option<TradeSignal> {
	if signal > 0.0 {
		Some(TradeSignal::Long)
	} else if signal < 0.0 {
		Some(TradeSignal::Short)
	} else {
		Some(TradeSignal::Close)
	}
}

fn translate_band_signal(signal: f64, upper: f64, lower: f64) -> Option<TradeSignal> {
	if signal > upper {
		Some(TradeSignal::Long)
	} else if signal < lower {
		Some(TradeSignal::Short)
	} else {
		Some(TradeSignal::Close)
	}
}

fn needs_initialization(close_buffer: &IndicatorBuffer, signal_buffer: &IndicatorBuffer) -> Option<usize> {
	let close = close_buffer.needs_initialization();
	let signal = signal_buffer.needs_initialization();
	match (close, signal) {
		(Some(x), Some(y)) => Some(x.max(y)),
		(Some(x), None) => Some(x),
		(None, Some(y)) => Some(y),
		(None, None) => None,
	}
}