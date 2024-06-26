mod parser;

use std::{error::Error, path::PathBuf};
use std::str::FromStr;
use chrono_tz::Tz;
use common::*;
use parser::CsvParser;

fn main() -> Result<(), Box<dyn Error>> {
	let config = get_config("csv-parser.ini")?;
	let section = "data";
	let get_path = |key| {
		match config.get(section, key) {
			Some(value) => Ok(PathBuf::from(value)),
			None => Err(format!("Missing path \"{}\" in configuration file", key))
		}
	};
	let time_zone_string = config.get(section, "time_zone")
		.ok_or("Missing time zone configuration")?;
	let time_zone = Tz::from_str(time_zone_string.as_str())?;
	let input_directory = get_path("input_directory")?;
	let output_directory = get_path("output_directory")?;
	let parser = CsvParser::new(&time_zone, &input_directory, &output_directory);
	parser.run();
	Ok(())
}