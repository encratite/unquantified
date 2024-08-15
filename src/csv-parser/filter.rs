use anyhow::{Context, Result, anyhow};

use common::parse_globex_code;
use configparser::ini::Ini;

use crate::ini_file::get_ini_sections;

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
	pub fn new(root: &String, ini: &Ini) -> Option<ContractFilter> {
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
		let first_last_contract_valid = !last_contract.is_some() || first_contract.is_some();
		if include_valid && first_last_contract_valid {
			let mut filter = ContractFilter {
				root: root.to_uppercase(),
				first_contract,
				last_contract,
				include_months,
				exclude_months,
				active: true,
				previous_symbol: None
			};
			filter.reset();
			Some(filter)
		} else {
			None
		}
	}

	pub fn from_ini(ini: &Ini) -> Result<Vec<ContractFilter>> {
		let config_map = get_ini_sections(ini)?;
		let filters = config_map.keys()
			.filter_map(|symbol| ContractFilter::new(symbol, &ini))
			.collect();
		Ok(filters)
	}

	pub fn is_included(&mut self, symbol: &String) -> bool {
		let Some((_, month, _)) = parse_globex_code(symbol) else {
			return true;
		};
		if let Some(first_contract) = &self.first_contract {
			if symbol == first_contract {
				self.active = true;
			} else if
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
				include_months.contains(&month)
			} else if let Some(exclude_months) = &self.exclude_months {
				!exclude_months.contains(&month)
			} else {
				false
			}
		} else {
			true
		}
	}

	pub fn reset(&mut self) {
		self.active = !self.first_contract.is_some();
		self.previous_symbol = None
	}
}