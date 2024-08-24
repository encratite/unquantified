use anyhow::Result;
use crate::backtest::{Backtest, BacktestResult};

pub trait Strategy {
	fn run(&mut self, symbols: Vec<String>, parameters: StrategyParameters, backtest: &mut Backtest) -> Result<BacktestResult>;
}

pub struct StrategyParameter {
	pub name: String,
	pub values: Option<Vec<f64>>,
	pub min: Option<f64>,
	pub max: Option<f64>,
	pub step: Option<f64>
}

pub struct StrategyParameters(Vec<StrategyParameter>);

impl StrategyParameters {
	pub fn get_values(&self, name: &str) -> Option<Vec<f64>> {
		match self.get_parameter(name) {
			Some(parameter) => parameter.values.clone(),
			None => None
		}
	}

	fn get_parameter(&self, name: &str) -> Option<&StrategyParameter> {
		self.0
			.iter()
			.find(|x| x.name.as_str() == name)
	}
}