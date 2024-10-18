use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use anyhow::{Error, Result};
use chrono::Datelike;
use rhai::{Dynamic, EvalAltResult, ImmutableString};
use unq_common::backtest::Backtest;
use unq_common::ohlc::OhlcRecord;
use crate::id::IndicatorId;
use crate::indicator::adx::AverageDirectionalIndex;
use crate::indicator::atr::AverageTrueRange;
use crate::indicator::bollinger::BollingerBands;
use crate::indicator::donchian::DonchianChannel;
use crate::indicator::exponential::ExponentialMovingAverage;
use crate::indicator::keltner::KeltnerChannel;
use crate::indicator::linear::LinearMovingAverage;
use crate::indicator::macd::MovingAverageConvergence;
use crate::indicator::ppo::PercentagePriceOscillator;
use crate::indicator::rate::RateOfChange;
use crate::indicator::rsi::RelativeStrengthIndicator;
use crate::indicator::simple::SimpleMovingAverage;
use crate::strategy::script::TradeSignal;
use crate::technical::{ChannelExitMode, Indicator};

pub type ApiResult<T> = anyhow::Result<T, Box<EvalAltResult>>;

pub struct ApiIndicator {
	symbol: String,
	id: IndicatorId,
	indicator: Box<dyn Indicator>
}

impl ApiIndicator {
	fn new(symbol: String, id: IndicatorId, indicator: Box<dyn Indicator>) -> Self {
		Self {
			symbol,
			id,
			indicator
		}
	}
}

pub struct ApiContext {
	current_symbol: String,
	parameters: HashMap<String, Dynamic>,
	indicators: Vec<ApiIndicator>,
	signals: HashMap<String, TradeSignal>,
	backtest: Rc<RefCell<Backtest>>
}

impl ApiContext {
	pub fn new(current_symbol: String, parameters: HashMap<String, Dynamic>, backtest: Rc<RefCell<Backtest>>) -> ApiContext {
		Self {
			current_symbol,
			parameters,
			indicators: Vec::new(),
			signals: HashMap::new(),
			backtest
		}
	}

	pub fn get_signal(&self, symbol: &String) -> Option<&TradeSignal> {
		self.signals.get(symbol)
	}

	pub fn get_valid_symbol_signals(&self) -> Result<Vec<(&String, &TradeSignal)>> {
		let mut valid_symbol_signals: Vec<(&String, &TradeSignal)> = Vec::new();
		for (symbol, signal) in self.signals.iter() {
			let is_valid = self.is_valid_symbol_signal(symbol, signal)?;
			if is_valid {
				valid_symbol_signals.push((symbol, signal));
			}
		}
		Ok(valid_symbol_signals)
	}

	pub fn is_valid_symbol_signal(&self, symbol: &String, signal: &TradeSignal) -> Result<bool> {
		if *signal == TradeSignal::Close {
			Ok(false)
		} else {
			let available = self.backtest.borrow().is_available(&symbol)?;
			Ok(available)
		}
	}

	pub fn reset_signals(&mut self, symbols: &Vec<String>) {
		let close_signals = symbols.iter().map(|symbol| (symbol.clone(), TradeSignal::Close));
		self.signals = HashMap::from_iter(close_signals);
	}

	pub fn insert_signal(&mut self, symbol: &String, signal: TradeSignal) {
		self.signals.insert(symbol.clone(), signal);
	}

	pub fn update_indicators(&mut self, symbol: &String, record: &OhlcRecord) {
		for api_indicator in self.indicators.iter_mut() {
			if api_indicator.symbol == *symbol {
				api_indicator.indicator.next(record);
			}
		}
	}

	pub fn set_symbol(&mut self, symbol: &String) {
		self.current_symbol = symbol.clone();
	}

	pub fn get_parameter_int(&self, name: ImmutableString, default_value: i64) -> ApiResult<i64> {
		match self.parameters.get(&name.to_string()) {
			Some(value) => {
				if value.is_float() {
					let value = value.as_float()?;
					Ok(value as i64)
				} else {
					Ok(value.as_int()?)
				}
			},
			None => Ok(default_value)
		}
	}

	pub fn get_parameter_float(&self, name: ImmutableString, default_value: f64) -> ApiResult<f64> {
		match self.parameters.get(&name.to_string()) {
			Some(value) => {
				if value.is_int() {
					let value = value.as_int()?;
					Ok(value as f64)
				} else {
					Ok(value.as_float()?)
				}
			},
			None => Ok(default_value)
		}
	}

	pub fn get_parameter_string(&self, name: ImmutableString, default_value: ImmutableString) -> ApiResult<ImmutableString> {
		match self.parameters.get(&name.to_string()) {
			Some(value) => {
				Ok(value.clone().into_immutable_string()?)
			},
			None => Ok(default_value)
		}
	}

	pub fn time(&self) -> ImmutableString {
		let backtest = self.backtest.borrow();
		let time = backtest.get_time();
		time.to_string().into()
	}

	pub fn month(&self) -> i64 {
		let backtest = self.backtest.borrow();
		let time = backtest.get_time();
		time.month() as i64
	}

	pub fn close(&self) -> ApiResult<f64> {
		let backtest = self.backtest.borrow();
		let record = backtest.most_recent_record(&self.current_symbol)
			.map_err(|error| -> Box<EvalAltResult> {
				format!("Failed to retrieve most recent record: {error}").into()
			})?;
		Ok(record.close)
	}

	pub fn simple_moving_average(&mut self, period: i64) -> ApiResult<Dynamic> {
		Self::validate_period(period)?;
		let indicator_id = SimpleMovingAverage::get_id(period as usize);
		let get_indicator = move || -> ApiResult<Box<dyn Indicator>> {
			let indicator = SimpleMovingAverage::new(period as usize, None)
				.map_err(Self::get_error)?;
			let indicator_box = Box::new(indicator);
			Ok(indicator_box)
		};
		self.execute_indicator(indicator_id, Box::new(get_indicator))
	}

	pub fn linear_moving_average(&mut self, period: i64) -> ApiResult<Dynamic> {
		Self::validate_period(period)?;
		let indicator_id = LinearMovingAverage::get_id(period as usize);
		let get_indicator = move || -> ApiResult<Box<dyn Indicator>> {
			let indicator = LinearMovingAverage::new(period as usize, None)
				.map_err(Self::get_error)?;
			let indicator_box = Box::new(indicator);
			Ok(indicator_box)
		};
		self.execute_indicator(indicator_id, Box::new(get_indicator))
	}

	pub fn exponential_moving_average(&mut self, period: i64) -> ApiResult<Dynamic> {
		Self::validate_period(period)?;
		let indicator_id = ExponentialMovingAverage::get_id(period as usize);
		let get_indicator = move || -> ApiResult<Box<dyn Indicator>> {
			let indicator = ExponentialMovingAverage::new(period as usize, None)
				.map_err(Self::get_error)?;
			let indicator_box = Box::new(indicator);
			Ok(indicator_box)
		};
		self.execute_indicator(indicator_id, Box::new(get_indicator))
	}

	pub fn relative_strength_indicator(&mut self, period: i64) -> ApiResult<Dynamic> {
		Self::validate_period(period)?;
		let indicator_id = RelativeStrengthIndicator::get_id(period as usize);
		let get_indicator = move || -> ApiResult<Box<dyn Indicator>> {
			let indicator = RelativeStrengthIndicator::new(period as usize, 0.0, 100.0)
				.map_err(Self::get_error)?;
			let indicator_box = Box::new(indicator);
			Ok(indicator_box)
		};
		self.execute_indicator(indicator_id, Box::new(get_indicator))
	}

	pub fn moving_average_convergence(&mut self, signal_period: i64, fast_period: i64, slow_period: i64) -> ApiResult<Dynamic> {
		Self::validate_periods(signal_period, fast_period, slow_period)?;
		let indicator_id = MovingAverageConvergence::get_id(signal_period as usize, fast_period as usize, slow_period as usize);
		let get_indicator = move || -> ApiResult<Box<dyn Indicator>> {
			let indicator = MovingAverageConvergence::new(signal_period as usize, fast_period as usize, slow_period as usize)
				.map_err(Self::get_error)?;
			let indicator_box = Box::new(indicator);
			Ok(indicator_box)
		};
		self.execute_indicator(indicator_id, Box::new(get_indicator))
	}

	pub fn percentage_price_oscillator(&mut self, signal_period: i64, fast_period: i64, slow_period: i64) -> ApiResult<Dynamic> {
		Self::validate_periods(signal_period, fast_period, slow_period)?;
		let indicator_id = PercentagePriceOscillator::get_id(signal_period as usize, fast_period as usize, slow_period as usize);
		let get_indicator = move || -> ApiResult<Box<dyn Indicator>> {
			let indicator = PercentagePriceOscillator::new(signal_period as usize, fast_period as usize, slow_period as usize)
				.map_err(Self::get_error)?;
			let indicator_box = Box::new(indicator);
			Ok(indicator_box)
		};
		self.execute_indicator(indicator_id, Box::new(get_indicator))
	}

	pub fn bollinger_band(&mut self, period: i64, multiplier: f64) -> ApiResult<Dynamic> {
		Self::validate_period(period)?;
		Self::validate_multiplier(multiplier)?;
		let indicator_id = BollingerBands::get_id(period as usize, multiplier);
		let get_indicator = move || -> ApiResult<Box<dyn Indicator>> {
			let indicator = BollingerBands::new(period as usize, multiplier, ChannelExitMode::Center)
				.map_err(Self::get_error)?;
			let indicator_box = Box::new(indicator);
			Ok(indicator_box)
		};
		self.execute_indicator(indicator_id, Box::new(get_indicator))
	}

	pub fn keltner_channel(&mut self, period: i64, multiplier: f64) -> ApiResult<Dynamic> {
		Self::validate_period(period)?;
		Self::validate_multiplier(multiplier)?;
		let indicator_id = KeltnerChannel::get_id(period as usize, multiplier);
		let get_indicator = move || -> ApiResult<Box<dyn Indicator>> {
			let indicator = KeltnerChannel::new(period as usize, multiplier, ChannelExitMode::Center)
				.map_err(Self::get_error)?;
			let indicator_box = Box::new(indicator);
			Ok(indicator_box)
		};
		self.execute_indicator(indicator_id, Box::new(get_indicator))
	}

	pub fn donchian_channel(&mut self, period: i64) -> ApiResult<Dynamic> {
		Self::validate_period(period)?;
		let indicator_id = DonchianChannel::get_id(period as usize);
		let get_indicator = move || -> ApiResult<Box<dyn Indicator>> {
			let indicator = DonchianChannel::new(period as usize, ChannelExitMode::Center)
				.map_err(Self::get_error)?;
			let indicator_box = Box::new(indicator);
			Ok(indicator_box)
		};
		self.execute_indicator(indicator_id, Box::new(get_indicator))
	}

	pub fn average_directional_index(&mut self, period: i64) -> ApiResult<Dynamic> {
		Self::validate_period(period)?;
		let indicator_id = AverageDirectionalIndex::get_id(period as usize);
		let get_indicator = move || -> ApiResult<Box<dyn Indicator>> {
			let indicator = AverageDirectionalIndex::new(period as usize)
				.map_err(Self::get_error)?;
			let indicator_box = Box::new(indicator);
			Ok(indicator_box)
		};
		self.execute_indicator(indicator_id, Box::new(get_indicator))
	}

	pub fn average_true_range(&mut self, period: i64) -> ApiResult<Dynamic> {
		Self::validate_period(period)?;
		let indicator_id = AverageTrueRange::get_id(period as usize);
		let get_indicator = move || -> ApiResult<Box<dyn Indicator>> {
			let indicator = AverageTrueRange::new(period as usize)
				.map_err(Self::get_error)?;
			let indicator_box = Box::new(indicator);
			Ok(indicator_box)
		};
		self.execute_indicator(indicator_id, Box::new(get_indicator))
	}

	pub fn rate_of_change(&mut self, period: i64) -> ApiResult<Dynamic> {
		Self::validate_period(period)?;
		let indicator_id = RateOfChange::get_id(period as usize);
		let get_indicator = move || -> ApiResult<Box<dyn Indicator>> {
			let indicator = RateOfChange::new(period as usize)
				.map_err(Self::get_error)?;
			let indicator_box = Box::new(indicator);
			Ok(indicator_box)
		};
		self.execute_indicator(indicator_id, Box::new(get_indicator))
	}

	fn validate_period(period: i64) -> ApiResult<()> {
		if period < 1 {
			return Err(format!("Invalid period ({period})").into())
		}
		Ok(())
	}

	fn validate_periods(signal_period: i64, fast_period: i64, slow_period: i64) -> ApiResult<()> {
		if signal_period < 1 {
			return Err(format!("Invalid signal period ({signal_period})").into())
		} else if fast_period < 1 {
			return Err(format!("Invalid fast period ({fast_period})").into())
		} else if slow_period < 1 {
			return Err(format!("Invalid slow period ({slow_period})").into())
		} else if signal_period >= fast_period {
			return Err(format!("Signal period must be less than fast period ({signal_period}, {fast_period})").into())
		} else if fast_period >= slow_period {
			return Err(format!("Fast period must be less than slow period ({fast_period}, {slow_period})").into())
		}
		Ok(())
	}

	fn validate_multiplier(multiplier: f64) -> ApiResult<()> {
		if multiplier <= 0.0 {
			return Err(format!("Invalid multiplier ({multiplier})").into())
		}
		Ok(())
	}

	fn translate_indicator_values(indicators: Option<Vec<f64>>) -> ApiResult<Dynamic> {
		let output = match indicators {
			Some(indicators) => {
				if indicators.len() == 1 {
					if let Some(first) = indicators.first() {
						return Ok((*first).into());
					}
				}
				indicators.into()
			},
			None => ().into()
		};
		Ok(output)
	}

	fn execute_indicator(&mut self, indicator_id: IndicatorId, get_indicator: Box<dyn Fn() -> ApiResult<Box<dyn Indicator>>>) -> ApiResult<Dynamic> {
		match self.indicators.iter().find(|x| x.id == indicator_id) {
			Some(api_indicator) => {
				let indicator_values = api_indicator.indicator.get_indicators();
				Self::translate_indicator_values(indicator_values)
			},
			None => {
				let mut indicator = get_indicator()?;
				if let Some(initialization_bars) = indicator.needs_initialization() {
					if let Ok(initialization_records) = self.backtest.borrow().get_records(&self.current_symbol, initialization_bars) {
						indicator.initialize(&initialization_records);
					}
				}
				let api_indicator = ApiIndicator::new(self.current_symbol.clone(), indicator_id, indicator);
				let indicator_values = api_indicator.indicator.get_indicators();
				self.indicators.push(api_indicator);
				Self::translate_indicator_values(indicator_values)
			}
		}
	}

	fn get_error(error: Error) ->  Box<EvalAltResult> {
		format!("Failed to create indicator: {error}").as_str().into()
	}
}