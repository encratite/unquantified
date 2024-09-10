use std::cmp::Ordering;
use anyhow::{bail, Context, Result};
use configparser::ini::Ini;
use unq_common::globex::GlobexCode;
use crate::ini_file::get_ini_sections;

#[derive(Clone)]
pub struct ContractFilter {
	pub root: String,
	legacy_cutoff: Option<GlobexCode>,
	first_contract: Option<String>,
	last_contract: Option<String>,
	include_months: Option<Vec<String>>,
	exclude_months: Option<Vec<String>>,
	active: bool,
	previous_symbol: Option<String>
}

impl ContractFilter {
	pub fn new(root: &String, ini: &Ini) -> Result<ContractFilter> {
		let get_filter = |key| -> Option<Vec<String>> {
			ini.get(root, key)
				.map(move |x|
					x.split(",")
					.map(|x| x.trim().to_string())
					.collect()
				)
		};
		let legacy_cutoff = match ini.get(root, "legacy_cutoff") {
			Some(string) => {
				let globex_code = GlobexCode::new(&string)
					.with_context(|| "Invalid Globex code in parser configuration")?;
				Some(globex_code)
			},
			None => None
		};
		let first_contract = ini.get(root, "first_contract");
		let last_contract = ini.get(root, "last_contract");
		let include_months = get_filter("include_months");
		let exclude_months = get_filter("exclude_months");
		if (first_contract.is_some() || last_contract.is_some()) && include_months.is_none() && exclude_months.is_none() {
			bail!("Invalid combination of filters for symbol \"{root}\"");
		}
		let mut filter = ContractFilter {
			root: root.to_uppercase(),
			legacy_cutoff,
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

	pub fn from_ini(ini: &Ini) -> Result<Vec<ContractFilter>> {
		let config_map = get_ini_sections(ini)?;
		let filters = config_map.keys()
			.map(|symbol| ContractFilter::new(symbol, &ini))
			.collect::<Result<Vec<_>>>()?;
		Ok(filters)
	}

	pub fn is_included(&mut self, symbol: &String) -> bool {
		let Some(globex_code) = GlobexCode::new(symbol) else {
			// It isn't a futures contract, bypass all checks
			return true;
		};
		if let Some(legacy_cutoff) = &self.legacy_cutoff {
			if globex_code.cmp(legacy_cutoff) == Ordering::Less {
				// The parser has a legacy cutoff Globex code specified and this contract is too old
				// This feature is meant to exclude data with missing volume data from prior to 2003 - 2006
				return false;
			};
		}
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
				include_months.contains(&globex_code.month)
			} else if let Some(exclude_months) = &self.exclude_months {
				!exclude_months.contains(&globex_code.month)
			} else {
				true
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