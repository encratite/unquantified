mod parser;

use std::{error::Error, path::PathBuf};
use std::str::FromStr;
use chrono_tz::Tz;
use common::*;
use parser::CsvParser;

fn main() -> Result<(), Box<dyn Error>> {
	let config = get_config("csv-parser.ini")?;
	let section = "data";
	let get_value = |key| -> Result<String, Box<dyn Error>> {
		match config.get(section, key) {
			Some(value) => Ok(value),
			None => Err(format!("Missing value \"{}\" in configuration file", key).into())
		}
	};
	let time_zone_string = get_value("time_zone")?;
	let time_zone = Tz::from_str(time_zone_string.as_str())?;
	let intraday_time_frame_string = get_value("intraday_time_frame")?;
	let intraday_time_frame = intraday_time_frame_string.parse::<u16>()?;
	let input_directory = PathBuf::from(get_value("input_directory")?);
	let output_directory = PathBuf::from(get_value("output_directory")?);
	let parser = CsvParser::new(time_zone, intraday_time_frame, input_directory, output_directory);
	parser.run();
	Ok(())
}