mod parser;
mod filter;
mod symbol;
mod ini_file;

use std::{path::PathBuf, str::FromStr};
use chrono_tz::Tz;
use anyhow::{Result, anyhow};
use common::get_ini;
use filter::ContractFilter;
use parser::CsvParser;
use symbol::SymbolMapper;

fn main() -> Result<()> {
	let ini = get_ini("csv-parser.ini")?;
	let section = "data";
	let get_value = |key| -> Result<String> {
		match ini.get(section, key) {
			Some(value) => Ok(value),
			None => Err(anyhow!("Missing value \"{key}\" in configuration file"))
		}
	};
	let time_zone_string = get_value("time_zone")?;
	let time_zone = Tz::from_str(time_zone_string.as_str())?;
	let intraday_time_frame_string = get_value("intraday_time_frame")?;
	let intraday_time_frame = intraday_time_frame_string.parse::<u16>()?;
	let input_directory = PathBuf::from(get_value("input_directory")?);
	let output_directory = PathBuf::from(get_value("output_directory")?);
	let filters = ContractFilter::from_ini(&ini)?;
	let symbol_mapper = SymbolMapper::new(&ini)?;
	let parser = CsvParser::new(time_zone, intraday_time_frame, input_directory, output_directory, filters, symbol_mapper);
	parser.run();
	Ok(())
}