mod parser;
mod filter;
mod symbol;
mod ini_file;

use std::path::PathBuf;
use anyhow::{Result, anyhow};
use filter::ContractFilter;
use parser::CsvParser;
use symbol::SymbolMapper;
use unq_common::get_ini;

fn main() -> Result<()> {
	let ini = get_ini("config/unq-parser.ini")?;
	let section = "data";
	let get_value = |key| -> Result<String> {
		match ini.get(section, key) {
			Some(value) => Ok(value),
			None => Err(anyhow!("Missing value \"{key}\" in configuration file"))
		}
	};
	let enable_intraday_string = get_value("enable_intraday")?;
	let enable_intraday = enable_intraday_string.parse::<bool>()?;
	let intraday_time_frame_string = get_value("intraday_time_frame")?;
	let intraday_time_frame = intraday_time_frame_string.parse::<u16>()?;
	let input_directory = PathBuf::from(get_value("input_directory")?);
	let output_directory = PathBuf::from(get_value("output_directory")?);
	let filters = ContractFilter::from_ini(&ini)?;
	let symbol_mapper = SymbolMapper::new(&ini)?;
	let parser = CsvParser::new(enable_intraday, intraday_time_frame, input_directory, output_directory, filters, symbol_mapper);
	parser.run();
	Ok(())
}