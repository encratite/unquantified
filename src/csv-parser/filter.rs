use std::error::Error;

use configparser::ini::Ini;
use lazy_static::lazy_static;
use regex::Regex;

lazy_static! {
	static ref GLOBEX_PATTERN: Regex = Regex::new("^[A-Z0-9]+([FGHJKMNQUVXZ])[0-9]{2}$").unwrap();
}

#[derive(Debug, Clone)]
pub struct ContractFilter {
	pub root: String,
	first_contract: Option<String>,
	last_contract: Option<String>,
	include_months: Option<Vec<String>>,
	exclude_months: Option<Vec<String>>,
	active: bool,
	previous_symbol: Option<String>
}

impl ContractFilter {
	pub fn new(root: &String, ini: &Ini) -> Result<ContractFilter, Box<dyn Error>> {
		let get_filter = |key| -> Option<Vec<String>> {
			ini.get(root, key)
				.map(move |x|
					x.split(",")
					.map(|x| x.trim().to_string())
					.collect()
				)
		};
		let first_contract = ini.get(root, "first_contract");
		let last_contract = ini.get(root, "last_contract");
		let include_months = get_filter("include_months");
		let exclude_months = get_filter("exclude_months");
		let include_valid = include_months.is_some() != exclude_months.is_some();
		let first_last_contract_valid = !first_contract.is_some() || last_contract.is_some();
		if include_valid && first_last_contract_valid {
			let mut filter = ContractFilter {
				root: root.clone(),
				first_contract,
				last_contract,
				include_months,
				exclude_months,
				active: true,
				previous_symbol: None
			};
			filter.reset();
			Ok(filter)
		}
		else {
			Err(format!("Invalid contract filter for \"{}\"", root).into())
		}
	}

	pub fn from_ini(ini: &Ini) -> Result<Vec<ContractFilter>, Box<dyn Error>> {
		let config_map = ini.get_map()
			.ok_or_else(|| "Unable to read configuration file")?;
		config_map.keys()
			.filter(|x| *x != "data")
			.map(|symbol| ContractFilter::new(symbol, &ini))
			.collect()
	}

	pub fn is_included(&mut self, symbol: &String) -> bool {
		let Some(captures) = GLOBEX_PATTERN.captures(symbol.as_str()) else {
			return true;
		};
		let month = &captures[1].to_string();
		if let Some(first_contract) = &self.first_contract {
			if symbol == first_contract {
				self.active = true;
			}
			else if
				let (Some(last_contract), Some(previous_symbol)) =
				(&self.last_contract, &self.previous_symbol)
			{
				if previous_symbol == last_contract && symbol != last_contract {
					self.active = false;
				}
			}
		}
		self.previous_symbol = Some(symbol.clone());
		if self.active {
			if let Some(include_months) = &self.include_months {
				include_months.contains(month)
			}
			else if let Some(exclude_months) = &self.exclude_months {
				!exclude_months.contains(month)
			}
			else {
				false
			}
		}
		else {
			true
		}
	}

	pub fn reset(&mut self) {
		self.active = !self.first_contract.is_some();
		self.previous_symbol = None
	}
}