use std::collections::HashMap;
use anyhow::{Context, Result};
use configparser::ini::Ini;

pub type IniMap = HashMap<String, HashMap<String, Option<String>>>;

pub fn get_ini_sections(ini: &Ini) -> Result<IniMap> {
	let mut config_map = ini.get_map()
		.with_context(|| "Unable to read configuration file")?;
	config_map.remove("data");
	Ok(config_map)
}