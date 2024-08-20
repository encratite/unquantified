use std::{collections::{HashSet, VecDeque}, sync::Arc};
use std::cmp::Ordering;
use std::collections::BTreeMap;
use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use anyhow::{Result, anyhow, bail};

use crate::{GlobexCode, OhlcArc, OhlcContractMap, OhlcVec, RawOhlcArchive};

type BoundaryMap<'a> = BTreeMap<&'a String, (DateTime<Tz>, DateTime<Tz>)>;

pub struct PanamaCanal<'a> {
	map: &'a OhlcContractMap,
	boundary_map: BoundaryMap<'a>,
	offset: f64,
	current_contract: String,
	used_contracts: HashSet<String>,
	skip_front_contract: bool
}

impl<'a> PanamaCanal<'a> {
	pub fn new(map: &'a OhlcContractMap, skip_front_contract: bool) -> Result<Option<PanamaCanal>> {
		let Some(last_records) = map.values().last() else {
			return Ok(None);
		};
		if !last_records.iter().any(|x| x.open_interest.is_some()) {
			// If the most recent records feature no open interest, it's probably not a futures contract
			return Ok(None);
		}
		let boundary_map = Self::get_boundary_map(map);
		let last_record = RawOhlcArchive::get_most_popular_record(last_records, skip_front_contract)?;
		let current_contract = last_record.symbol.clone();
		let used_contracts = HashSet::from_iter([current_contract.clone()]);
		let channel = PanamaCanal {
			map,
			boundary_map,
			offset: 0.0,
			current_contract,
			used_contracts,
			skip_front_contract
		};
		Ok(Some(channel))
	}

	pub fn get_adjusted_data(&mut self) -> Result<OhlcVec> {
		let mut output = VecDeque::new();
		let time_limit_opt = self.get_time_limit()?;
		for (time, records) in self.map.iter().rev() {
			if let Some(time_limit) = time_limit_opt {
				if *time < time_limit {
					break;
				}
			}
			if let Some(next_record) = self.get_next_record(time, records)? {
				let adjusted_record = next_record.apply_offset(self.offset);
				output.push_front(Arc::new(adjusted_record));
			}
		}
		let output_vec = Vec::from(output);
		Ok(output_vec)
	}

	fn get_boundary_map(map: &'a OhlcContractMap) -> BoundaryMap<'a> {
		// Keep track of when contracts start and expire, so we don't accidentally roll over into the wrong contract
		let mut boundary_map: BoundaryMap = BTreeMap::new();
		for records in map.values() {
			for record in records {
				let key = &record.symbol;
				let value = record.time;
				if let Some((first, last)) = boundary_map.get(key) {
					// Expand boundaries
					if value < *first {
						boundary_map.insert(key, (value, *last));
					} else if value > *last {
						boundary_map.insert(key, (*first, value));
					}
				} else {
					// Initialize with identical boundaries
					boundary_map.insert(key, (value, value));
				}
			}
		}
		boundary_map
	}

	fn get_next_record(&mut self, time: &DateTime<Utc>, records: &OhlcVec) -> Result<Option<OhlcArc>> {
		let get_output = |record: &OhlcArc| Ok(Some(record.clone()));
		let filtered_records = self.filter_records(records);
		if filtered_records.is_empty() {
			bail!("Failed to filter for older records for contract {} at {}", self.current_contract, time.to_rfc3339());
		}
		let new_record = RawOhlcArchive::get_most_popular_record(&filtered_records, self.skip_front_contract)?;
		let new_symbol = new_record.symbol.clone();
		if *new_symbol == self.current_contract {
			// No rollover necessary yet
			get_output(&new_record)
		} else {
			let Some(current_record) = filtered_records.iter().find(|x| x.symbol == self.current_contract) else {
				let (first, _) = self.get_boundaries(&self.current_contract)?;
				if first < time {
					// There is still more data available for that contract, just not for the current point in time
					// Leave a gap and wait for the older records to become available to perform the rollover
					return Ok(None);
				} else {
					bail!("Failed to perform rollover for contract {} at {}", self.current_contract, time.to_rfc3339());
				}
			};
			if !self.used_contracts.contains(&new_symbol) {
				// Check if the expiration dates are compatible
				let (_, current_expiration) = self.get_boundaries(&self.current_contract)?;
				let (_, new_expiration) = self.get_boundaries(&new_symbol)?;
				if new_expiration < current_expiration {
					// Perform rollover and adjust channel offset
					let delta = current_record.close - new_record.close;
					self.offset += delta;
					self.current_contract = new_symbol.clone();
					self.used_contracts.insert(new_symbol);
					get_output(&new_record)
				} else {
					// We already switched to a contract with a more recent expiration date, ignore it
					get_output(current_record)
				}
			} else {
				// Unusual scenario, the open interest scan resulted in a previously used contract being selected again
				// Ignore it and stick to the current contract
				get_output(current_record)
			}
		}
	}

	fn get_boundaries(&self, symbol: &String) -> Result<&(DateTime<Tz>, DateTime<Tz>)> {
		self.boundary_map
			.get(symbol)
			.ok_or_else(|| anyhow!("Failed to determine contract expiration date of {symbol}"))
	}

	fn get_time_limit(&self) -> Result<Option<DateTime<Tz>>> {
		if self.skip_front_contract {
			// Prevent get_next_record from being called on the first contract with skip_front_contract enabled
			// Otherwise it would return an error
			let mut contracts: Vec<(GlobexCode, DateTime<Tz>)> = self.boundary_map
				.iter()
				.map(|(key, (first, _))| (GlobexCode::new(key).unwrap(), first.clone()))
				.collect();
			if contracts.len() < 2 {
				bail!("Invalid contract count");
			}
			contracts.sort_by(|(globex_code1, _), (globex_code2, _)| globex_code1.cmp(globex_code2));
			let (_, time_limit) = contracts[1];
			Ok(Some(time_limit.clone()))
		} else {
			Ok(None)
		}
	}

	fn filter_records(&self, records: &OhlcVec) -> OhlcVec {
		records
			.iter()
			.filter(|x| {
				let ordering = GlobexCode::new(&x.symbol).cmp(&GlobexCode::new(&self.current_contract));
				ordering == Ordering::Less || ordering == Ordering::Equal
			})
			.cloned()
			.collect()
	}
}