use std::{collections::{HashSet, VecDeque, BTreeMap, HashMap}, cmp::Ordering};
use chrono::{Duration, NaiveDate, NaiveDateTime, Timelike};
use anyhow::{Result, anyhow, bail, Context};
use crate::{globex::GlobexCode, RawOhlcArchive};
use crate::ohlc::{OhlcContractMap, OhlcRecord, OhlcVec};

type BoundaryMap<'a> = BTreeMap<&'a String, (NaiveDateTime, NaiveDateTime)>;
pub type OffsetMap = HashMap<String, f64>;

pub struct PanamaCanal<'a> {
	map: &'a OhlcContractMap,
	boundary_map: BoundaryMap<'a>,
	offset: f64,
	offset_map: OffsetMap,
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
		let Some(last_record) = RawOhlcArchive::get_most_popular_record(last_records, skip_front_contract)? else {
			bail!("Unable to determine initial contract");
		};
		let current_contract = last_record.symbol.clone();
		let used_contracts = HashSet::from_iter([current_contract.clone()]);
		let mut channel = PanamaCanal {
			map,
			boundary_map,
			offset: 0.0,
			offset_map: HashMap::new(),
			current_contract,
			used_contracts,
			skip_front_contract
		};
		channel.update_offset_map();
		Ok(Some(channel))
	}

	pub fn get_adjusted_data(&mut self) -> Result<(OhlcVec, OffsetMap)> {
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
				output.push_front(adjusted_record);
			}
		}
		let output_vec = Vec::from(output);
		Ok((output_vec, self.offset_map.clone()))
	}

	// Generate a continuous contract with intraday data from the rollovers that had previously been calculated from daily data
	pub fn from_offset_map(intraday: &OhlcContractMap, daily: &OhlcVec, offset_map: &OffsetMap) -> Result<OhlcVec> {
		let mut output = OhlcVec::new();
		let mut daily_map = HashMap::new();
		for x in daily {
			daily_map.insert(x.time.date(), &x.symbol);
		}
		let (mut current_contract, first_date) = Self::get_current_contract(intraday, daily, &daily_map)?;
		let Some(mut offset) = Self::deref(offset_map.get(current_contract)) else {
			bail!("Unable to initialize Panama offset for contract {current_contract}");
		};
		let mut rollover_date = first_date;
		for (time, records) in intraday.iter() {
			let date = time.date();
			if let Some(daily_contract) = Self::deref(daily_map.get(&date)) {
				if daily_contract != current_contract {
					// Try to perform the rollover during the primary trading session and not at midnight
					if time.hour() >= 12 || date > rollover_date {
						let Some(new_offset) = Self::deref(offset_map.get(daily_contract)) else {
							bail!("Unable to determine offset for contract {daily_contract}");
						};
						offset = new_offset;
						current_contract = daily_contract;
					}
					rollover_date = date;
				}
			}
			// Only generate an adjusted record in case of a matching contract for the current period in the intraday contract map
			if let Some(contract_record) = records.iter().find(|x| x.symbol == *current_contract) {
				let mut adjusted_record = contract_record.apply_offset(offset);
				adjusted_record.symbol = current_contract.clone();
				output.push(adjusted_record);
			}
		}
		Ok(output)
	}

	fn get_current_contract<'b>(intraday: &OhlcContractMap, daily: &OhlcVec, daily_map: &HashMap<NaiveDate, &'b String>) -> Result<(&'b String, NaiveDate)> {
		let first_intraday_date_opt = intraday
			.keys()
			.map(|x| x.date())
			.next();
		let Some(first_intraday_date) = first_intraday_date_opt else {
			bail!("Unable to get first intraday date");
		};
		let first_daily_date_opt = daily
			.iter()
			.map(|x| x.time.date())
			.next();
		let Some(first_daily_date) = first_daily_date_opt else {
			bail!("Unable to get first daily date");
		};
		let first_date = first_intraday_date.max(first_daily_date);
		for i in 0..30 {
			let try_date = first_daily_date - Duration::days(i);
			if let Some(current_contract) = Self::deref(daily_map.get(&try_date)) {
				return Ok((current_contract, first_date));
			}
		}
		bail!("Unable to determine first contract");
	}

	fn deref<T>(opt: Option<&T>) -> Option<T>
	where
		T: Copy
	{
		opt.map(|x| *x)
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

	fn get_next_record(&mut self, time: &NaiveDateTime, records: &OhlcVec) -> Result<Option<OhlcRecord>> {
		let get_output = |record: &OhlcRecord| Ok(Some(record.clone()));
		let filtered_records = self.filter_records(records);
		let Some(new_record) = RawOhlcArchive::get_most_popular_record(&filtered_records, self.skip_front_contract)? else {
			return self.perform_time_check(time);
		};
		let new_symbol = new_record.symbol.clone();
		if *new_symbol == self.current_contract {
			// No rollover necessary yet
			get_output(&new_record)
		} else {
			let Some(current_record) = filtered_records.iter().find(|x| x.symbol == self.current_contract) else {
				return self.perform_time_check(time);
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
					self.update_offset_map();
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

	fn get_boundaries(&self, symbol: &String) -> Result<&(NaiveDateTime, NaiveDateTime)> {
		self.boundary_map
			.get(symbol)
			.with_context(|| anyhow!("Failed to determine contract expiration date of {symbol}"))
	}

	fn get_time_limit(&self) -> Result<Option<NaiveDateTime>> {
		if self.skip_front_contract {
			// Prevent get_next_record from being called on the first contract with skip_front_contract enabled
			// Otherwise it would return an error
			let mut contracts: Vec<(GlobexCode, NaiveDateTime)> = self.boundary_map
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

	fn perform_time_check(&self, time: &NaiveDateTime) -> Result<Option<OhlcRecord>> {
		let (first, _) = self.get_boundaries(&self.current_contract)?;
		if first < time {
			// There is still more data available for that contract, just not for the current point in time
			// Leave a gap and wait for the older records to become available to perform the rollover
			Ok(None)
		} else {
			bail!("Failed to perform rollover for contract {} at {}", self.current_contract, time);
		}
	}

	fn update_offset_map(&mut self) {
		self.offset_map.insert(self.current_contract.clone(), self.offset);
	}
}