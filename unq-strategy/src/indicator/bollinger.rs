use unq_common::ohlc::OhlcRecord;
use unq_common::stats::standard_deviation_mean_biased;
use crate::buffer::IndicatorBuffer;
use crate::id::IndicatorId;
use crate::technical::*;

#[derive(Clone)]
pub struct BollingerBands {
	multiplier: f64,
	exit_mode: ChannelExitMode,
	buffer: IndicatorBuffer,
	indicators: Option<(f64, f64, f64)>
}

impl BollingerBands {
	pub const ID: &'static str = "bollinger";

	pub fn new(period: usize, multiplier: f64, exit_mode: ChannelExitMode) -> anyhow::Result<Self> {
		validate_period(period)?;
		validate_multiplier(multiplier)?;
		let output = Self {
			multiplier,
			exit_mode,
			buffer: IndicatorBuffer::new(period),
			indicators: None
		};
		Ok(output)
	}

	pub fn get_id(period: usize, multiplier: f64) -> IndicatorId {
		IndicatorId::from_period_multiplier(Self::ID, period, multiplier)
	}

	fn calculate(&self) -> (f64, f64, f64) {
		let buffer = &self.buffer.buffer;
		let center = exponential_moving_average(buffer.iter(), buffer.len());
		let standard_deviation = standard_deviation_mean_biased(buffer.iter(), center).unwrap();
		let lower = center - self.multiplier * standard_deviation;
		let upper = center + self.multiplier * standard_deviation;
		(center, lower, upper)
	}
}

impl Indicator for BollingerBands {
	fn get_description(&self) -> String {
		format!("Bollinger({}, {:.1}, {})", self.buffer.size, self.multiplier, self.exit_mode)
	}

	fn next(&mut self, record: &OhlcRecord) {
		let close = record.close;
		self.buffer.add(close);
		if !self.buffer.filled() {
			return;
		}
		let (center, lower, upper) = self.calculate();
		self.indicators = Some((center, lower, upper));
	}

	fn get_indicators(&self) -> Option<Vec<f64>> {
		get_channel_indicators(&self.indicators)
	}

	fn get_trade_signal(&self, state: PositionState) -> Option<TradeSignal> {
		get_channel_trade_signal(&self.buffer, &self.indicators, &self.exit_mode, state)
	}

	fn needs_initialization(&self) -> Option<usize> {
		self.buffer.needs_initialization()
	}

	fn clone_box(&self) -> Box<dyn Indicator> {
		Box::new(self.clone())
	}
}