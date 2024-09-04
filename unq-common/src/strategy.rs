use anyhow::{Result, bail};
use serde::Deserialize;

pub trait Strategy {
	fn next(&mut self) -> Result<()>;
}

/*
Strategy parameters specified in the "backtest" command.

{parameter1: 12.34} corresponds to:
- name: "parameter1"
- value: Some(12.34)
- limit: None
- increment: None
- values: None

{parameter2: 1 to 5 step 1} corresponds to:
- name: "parameter2"
- value: Some(1)
- limit: Some(5)
- increment: Some(1)
- values: None

{parameter3: [1.2, 3.4, 4.5]} corresponds to:
- name: "parameter3"
- value: None
- limit: None
- increment: None
- values: Some({1.2, 3.4, 4.5})
*/
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StrategyParameter {
	pub name: String,
	pub value: Option<f64>,
	pub limit: Option<f64>,
	pub increment: Option<f64>,
	pub values: Option<Vec<f64>>,
	pub bool_value: Option<bool>
}

pub struct StrategyParameters(pub Vec<StrategyParameter>);

impl StrategyParameter {
	pub fn sanity_check(&self) -> Result<()> {
		let tuple = (
			self.value.is_some(),
			self.limit.is_some(),
			self.increment.is_some(),
			self.values.is_some(),
			self.bool_value.is_some()
		);
		match tuple {
			(true, false, false, false, false) |
			(true, true, false, false, false) |
			(true, true, true, false, false) |
			(false, false, false, true, false) |
			(false, false, false, false, true) => Ok(()),
			_ => bail!("Invalid combination of values in strategy parameter")
		}
	}

	pub fn not_bool_check(&self) -> Result<()> {
		if self.bool_value.is_some() {
			bail!("Parameter \"{}\" cannot take a boolean value", self.name);
		}
		Ok(())
	}
}

impl StrategyParameters {
	pub fn new(params: Vec<StrategyParameter>) -> Self {
		StrategyParameters(params)
	}

	pub fn get_value(&self, name: &str) -> Result<Option<f64>> {
		match self.get_parameter(name) {
			Some(parameter) => {
				if parameter.values.is_some() {
					bail!("Cannot specify multiple values for parameter \"{name}\"");
				}
				parameter.not_bool_check()?;
				Ok(parameter.value)
			}
			None => Ok(None)
		}
	}

	pub fn get_values(&self, name: &str) -> Result<Option<Vec<f64>>> {
		match self.get_parameter(name) {
			Some(parameter) => {
				parameter.not_bool_check()?;
				match parameter.value {
					Some(value) => Ok(Some(vec![value])),
					None => Ok(parameter.values.clone())
				}
			}
			None => Ok(None)
		}
	}

	pub fn get_bool(&self, name: &str) -> Result<Option<bool>> {
		match self.get_parameter(name) {
			Some(parameter) => {
				if parameter.value.is_some() || parameter.values.is_some() {
					bail!("Invalid value specified for boolean parameter \"{name}\"");
				}
				Ok(parameter.bool_value)
			}
			None => Ok(None)
		}
	}

	fn get_parameter(&self, name: &str) -> Option<&StrategyParameter> {
		self.0
			.iter()
			.find(|x| x.name.as_str() == name)
	}
}