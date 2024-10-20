use std::collections::vec_deque::Iter;
use std::collections::VecDeque;
use std::fmt::{Display, Formatter};
use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use crate::web::WebF64;

#[derive(PartialEq, Debug)]
pub enum StrategyParameterType {
	NumericSingle,
	NumericMulti,
	NumericRange,
	Bool,
	StringSingle,
	StringMulti,
}

type StrategyParameterSelect<'a, T> = &'a dyn Fn(&StrategyParameter) -> Option<T>;
type StrategyParameterValueSelect<'a, T> = (StrategyParameterType, &'a dyn Fn(&StrategyParameter) -> Vec<T>);

pub trait Strategy {
	fn next(&mut self) -> Result<()>;
}

#[derive(Debug)]
pub struct StrategyParameterError {
	message: String
}

impl StrategyParameterError {
	pub fn new(message: String) -> Self {
		Self {
			message
		}
	}
}

impl Display for StrategyParameterError {
	fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
		formatter.write_str(self.message.as_str())
	}
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
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct StrategyParameter {
	pub name: String,
	pub value: Option<WebF64>,
	pub limit: Option<WebF64>,
	pub increment: Option<WebF64>,
	pub values: Option<Vec<WebF64>>,
	pub bool_value: Option<bool>,
	pub string_value: Option<String>,
	pub string_values: Option<Vec<String>>
}

#[derive(Serialize, Clone, Debug)]
pub struct StrategyParameters(VecDeque<StrategyParameter>);

impl StrategyParameter {
	pub fn single(name: String, value: f64) -> Self {
		Self {
			name,
			value: Some(WebF64::new(value)),
			limit: None,
			increment: None,
			values: None,
			bool_value: None,
			string_value: None,
			string_values: None
		}
	}

	pub fn get_type(&self) -> Result<StrategyParameterType> {
		let tuple = (
			self.value.is_some(),
			self.limit.is_some(),
			self.increment.is_some(),
			self.values.is_some(),
			self.bool_value.is_some(),
			self.string_value.is_some(),
			self.string_values.is_some(),
		);
		match tuple {
			(true, false, false, false, false, false, false) => Ok(StrategyParameterType::NumericSingle),
			(true, true, false, false, false, false, false) => Ok(StrategyParameterType::NumericRange),
			(true, true, true, false, false, false, false) => Ok(StrategyParameterType::NumericRange),
			(false, false, false, true, false, false, false) => Ok(StrategyParameterType::NumericMulti),
			(false, false, false, false, true, false, false) => Ok(StrategyParameterType::Bool),
			(false, false, false, false, false, true, false) => Ok(StrategyParameterType::StringSingle),
			(false, false, false, false, false, false, true) => Ok(StrategyParameterType::StringMulti),
			_ => bail!("Invalid combination of values in strategy parameter")
		}
	}
}

impl StrategyParameters {
	pub fn new() -> Self {
		StrategyParameters(VecDeque::new())
	}

	pub fn from_vec(parameters: Vec<StrategyParameter>) -> Self {
		StrategyParameters(VecDeque::from(parameters))
	}

	pub fn get_value(&self, name: &str) -> Result<Option<f64>> {
		let select: StrategyParameterSelect<f64> = &|parameter| parameter.value.clone().map(|x| x.get());
		self.get_typed_parameter(name, StrategyParameterType::NumericSingle, select)
	}

	pub fn get_values(&self, name: &str) -> Result<Option<Vec<f64>>> {
		let single: StrategyParameterValueSelect<f64> = (StrategyParameterType::NumericSingle, &|parameter: &StrategyParameter| -> Vec<f64> {
			let value = parameter.value
				.clone()
				.unwrap()
				.get();
			vec![value]
		});
		let multi: StrategyParameterValueSelect<f64> = (StrategyParameterType::NumericMulti, &|parameter: &StrategyParameter| -> Vec<f64> {
			parameter.values
				.clone()
				.unwrap()
				.iter()
				.map(|x| x.get())
				.collect()
		});
		self.get_multi_value(name, single, multi)
	}

	pub fn get_bool(&self, name: &str) -> Result<Option<bool>> {
		let select: StrategyParameterSelect<bool> = &|parameter| parameter.bool_value;
		self.get_typed_parameter(name, StrategyParameterType::Bool, select)
	}

	pub fn get_string(&self, name: &str) -> Result<Option<String>> {
		let select: StrategyParameterSelect<String> = &|parameter| parameter.string_value.clone();
		self.get_typed_parameter(name, StrategyParameterType::StringSingle, select)
	}

	pub fn get_strings(&self, name: &str) -> Result<Option<Vec<String>>> {
		let single: StrategyParameterValueSelect<String> = (StrategyParameterType::StringSingle, &|parameter: &StrategyParameter| -> Vec<String> {
			let value = parameter.string_value.clone().unwrap();
			vec![value]
		});
		let multi: StrategyParameterValueSelect<String> = (StrategyParameterType::StringMulti, &|parameter: &StrategyParameter| -> Vec<String> {
			parameter.string_values.clone().unwrap()
		});
		self.get_multi_value(name, single, multi)
	}

	pub fn push_back(&mut self, parameter: StrategyParameter) {
		self.0.push_back(parameter);
	}

	pub fn pop_front(&mut self) -> Option<StrategyParameter> {
		self.0.pop_front()
	}

	fn get_parameter(&self, name: &str) -> Option<&StrategyParameter> {
		self.0
			.iter()
			.find(|x| x.name.as_str() == name)
	}

	fn get_typed_parameter<T>(&self, name: &str, expected_type: StrategyParameterType, select: StrategyParameterSelect<T>) -> Result<Option<T>> {
		if let Some(parameter) = self.get_parameter(name) {
			let parameter_type = parameter.get_type()?;
			if parameter_type == expected_type {
				 Ok(select(parameter))
			} else {
				bail!("Found parameter type \"{parameter_type:?}\" for parameter \"{name}\", expected \"{expected_type:?}\"")
			}
		} else {
			Ok(None)
		}
	}

	fn get_multi_value<T>(&self, name: &str, single: StrategyParameterValueSelect<T>, multi: StrategyParameterValueSelect<T>) -> Result<Option<Vec<T>>> {
		if let Some(parameter) = self.get_parameter(name) {
			let (single_type, single_select) = single;
			let (multi_type, multi_select) = multi;
			let parameter_type = parameter.get_type()?;
			if parameter_type == single_type {
				let values = single_select(parameter);
				Ok(Some(values))
			} else if parameter_type == multi_type {
				let values = multi_select(parameter);
				Ok(Some(values))
			} else {
				bail!("Found parameter type \"{parameter_type:?}\" for parameter \"{name}\", expected \"{single_type:?}\" or \"{multi_type:?}\"")
			}
		} else {
			Ok(None)
		}
	}

	pub fn iter(&self) -> Iter<StrategyParameter> {
		self.0.iter()
	}
}