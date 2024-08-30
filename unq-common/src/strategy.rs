use anyhow::Result;
use serde::Deserialize;

pub trait Strategy {
	fn next(&mut self) -> Result<()>;
}

#[derive(Deserialize)]
pub struct StrategyParameter {
	pub name: String,
	pub values: Option<Vec<f64>>,
	pub min: Option<f64>,
	pub max: Option<f64>,
	pub step: Option<f64>
}

pub struct StrategyParameters(pub Vec<StrategyParameter>);

impl StrategyParameters {
	pub fn new(params: Vec<StrategyParameter>) -> Self {
		StrategyParameters(params)
	}

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