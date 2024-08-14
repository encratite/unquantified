use std::collections::HashMap;

use common::parse_globex_code;
use configparser::ini::Ini;
use anyhow::Result;

use crate::ini_file::get_ini_sections;

pub struct SymbolMapper {
	symbols: HashMap<String, String>
}

impl SymbolMapper {
	pub fn new(ini: &Ini) -> Result<SymbolMapper> {
		let mut symbols = HashMap::new();
		let config_map = get_ini_sections(ini)?;
		for (data_symbol, map) in config_map {
			if let Some(Some(exchange_symbol)) = map.get("exchange_symbol") {
				symbols.insert(data_symbol, exchange_symbol.clone());
			}
		}
		let mapper = SymbolMapper {
			symbols
		};
		Ok(mapper)
	}

	pub fn translate(&self, symbol: &String) -> String {
		if let Some((root, month, year)) = parse_globex_code(symbol) {
			if let Some(exchange_root) = self.symbols.get(&root) {
				format!("{exchange_root}{month}{year}")
			} else {
				symbol.clone()
			}
		} else {
			if let Some(exchange_symbol) = self.symbols.get(symbol) {
				exchange_symbol.clone()
			} else {
				symbol.clone()
			}
		}

	}
}