use std::{collections::{HashMap, HashSet, VecDeque}, error::Error};

use chrono::{DateTime, Utc};
use chrono_tz::Tz;

use crate::{ErrorBox, OhlcBox, OhlcContractMap, OhlcVec, RawOhlcArchive};

pub struct PanamaChannel<'a> {
	map: &'a OhlcContractMap,
	expiration_map: HashMap<&'a String, DateTime<Tz>>,
	offset: f64,
	current_contract: String,
	used_contracts: HashSet<String>,
	skip_front_contract: bool
}

impl<'a> PanamaChannel<'a> {
	pub fn new(map: &OhlcContractMap, skip_front_contract: bool) -> Result<Option<PanamaChannel>, ErrorBox> {
		let Some(last_records) = map.values().last() else {
			return Ok(None);
		};
		if !last_records.iter().any(|x| x.open_interest.is_some()) {
			// If the most recent records feature no open interest, it's probably not a futures contract
			return Ok(None);
		}
		let expiration_map = Self::get_expiration_map(map);
		let last_record = RawOhlcArchive::get_most_popular_record(last_records, skip_front_contract)?;
		let current_contract = last_record.symbol;
		let used_contracts = HashSet::from_iter([current_contract.clone()]);
		let channel = PanamaChannel {
			map,
			expiration_map,
			offset: 0.0,
			current_contract,
			used_contracts,
			skip_front_contract
		};
		Ok(Some(channel))
	}

	pub fn get_adjusted_data(&mut self) -> Result<OhlcVec, ErrorBox> {
		let mut output = VecDeque::new();
		for (time, records) in self.map.iter().rev() {
			let next_record = self.get_next_record(time, records)?;
			let adjusted_record = next_record.apply_offset(self.offset);
			output.push_front(Box::new(adjusted_record));
		}
		let output_vec = Vec::from(output);
		Ok(output_vec)
	}

	fn get_expiration_map(map: &OhlcContractMap) -> HashMap<&String, DateTime<Tz>> {
		// Keep track of when contracts expire so we don't accidentally roll over into the wrong contract based on an open interest scan
		let mut expiration_map: HashMap<&String, DateTime<Tz>> = HashMap::new();
		for records in map.values() {
			for record in records {
				let key = &record.symbol;
				let value = record.time;
				if let Some(expiration_time) = expiration_map.get(key) {
					if value > *expiration_time {
						// Increase expiration date
						expiration_map.insert(key, value);
					}
				} else {
					// Initialize expiration date
					expiration_map.insert(key, value);
				}
			}
		}
		expiration_map
	}

	fn get_next_record(&mut self, time: &DateTime<Utc>, records: &OhlcVec) -> Result<OhlcBox, ErrorBox> {
		let new_record = RawOhlcArchive::get_most_popular_record(records, self.skip_front_contract)?;
		let new_symbol = new_record.symbol.clone();
		if *new_symbol == self.current_contract {
			// No rollover necessary yet
			Ok(Box::clone(&new_record))
		} else {
			let Some(current_record) = records.iter().find(|x| x.symbol == self.current_contract) else {
				let message = format!("Failed to perform rollover for contract {} at {}", self.current_contract, time.to_rfc3339());
				return Err(message.into());
			};
			if !self.used_contracts.contains(&new_symbol) {
				// Check if the expiration dates are compatible
				let current_expiration = self.get_expiration_date(&self.current_contract)?;
				let new_expiration = self.get_expiration_date(&new_symbol)?;
				if new_expiration < current_expiration {
					// Perform rollover and adjust channel offset
					let delta = current_record.close - new_record.close;
					self.offset += delta;
					self.current_contract = new_symbol.clone();
					self.used_contracts.insert(new_symbol);
					Ok(Box::clone(&new_record))
				} else {
					// We already switched to a contract with a more recent expiration date, ignore it
					Ok(Box::clone(current_record))
				}
			} else {
				// Unusual scenario, the open interest scan resulted in a previously used contract being selected again
				// Ignore it and stick to the current contract
				Ok(Box::clone(current_record))
			}
		}
	}

	fn get_expiration_date(&self, symbol: &String) -> Result<&DateTime<Tz>, ErrorBox> {
		match self.expiration_map.get(symbol) {
			Some(time) => Ok(time),
			None => Err(format!("Failed to determine contract expiration date of {}", symbol).into())
		}
	}
}