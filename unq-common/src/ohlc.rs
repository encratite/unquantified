use std::collections::{BTreeMap, HashSet};
use anyhow::{bail, Result};
use chrono::NaiveDateTime;
use rkyv::{Archive, Deserialize, Serialize};
use crate::globex::GlobexCode;
use crate::panama::{OffsetMap, PanamaCanal};

pub type OhlcVec = Vec<OhlcRecord>;
pub type OhlcMap = BTreeMap<NaiveDateTime, OhlcRecord>;
pub type OhlcContractMap = BTreeMap<NaiveDateTime, OhlcVec>;

#[derive(Clone, PartialEq, Archive, Serialize, serde::Deserialize)]
pub enum TimeFrame {
	#[serde(rename = "daily")]
	Daily,
	#[serde(rename = "intraday")]
	Intraday
}

#[derive(Archive, Serialize, Deserialize)]
pub struct RawOhlcArchive {
	pub daily: Vec<RawOhlcRecord>,
	pub intraday: Vec<RawOhlcRecord>,
	pub intraday_time_frame: u16
}

#[derive(Archive, Serialize, Deserialize)]
pub struct RawOhlcRecord {
	pub symbol: String,
	pub time: NaiveDateTime,
	pub open: f64,
	pub high: f64,
	pub low: f64,
	pub close: f64,
	pub volume: u32,
	pub open_interest: Option<u32>
}

pub struct OhlcArchive {
	pub daily: OhlcData,
	pub intraday: OhlcData,
	pub intraday_time_frame: u16
}

/*
General container for the actual OHLC records. Contents depend on the asset type.

Currency pair:
- "unadjusted" contains the original records
- "adjusted" is None
- "contract_map" is None

Futures:
- Both "unadjusted"/"adjusted" contain a continuous contract with new records generated from multiple overlapping contracts
- In the case of "unadjusted" it is the original values with automatic roll-overs based on volume and open interest
- "adjusted" features new records generated using the Panama Canal method for use with indicators, same roll-over criteria
- Each vector in "contract_map" contains the full set of active contracts for that particular point in time
 */
pub struct OhlcData {
	pub unadjusted: OhlcMap,
	pub adjusted: Option<OhlcMap>,
	pub contract_map: Option<OhlcContractMap>
}

#[derive(Clone, Debug)]
pub struct OhlcRecord {
	pub symbol: String,
	pub time: NaiveDateTime,
	pub open: f64,
	pub high: f64,
	pub low: f64,
	pub close: f64,
	pub volume: u32,
	pub open_interest: Option<u32>
}

impl RawOhlcArchive {
	pub fn to_archive(&self, skip_front_contract: bool) -> Result<OhlcArchive> {
		let is_contract = Self::is_contract(&self.daily);
		let (daily, intraday) = if is_contract {
			let (daily, offset_map_opt) = Self::get_data(&self.daily, None, skip_front_contract)?;
			let Some(offset_map) = offset_map_opt else {
				bail!("Missing offset map");
			};
			let Some(daily_adjusted) = &daily.adjusted else {
				bail!("Missing daily adjusted records");
			};
			let daily_offset_map = Some((daily_adjusted, &offset_map));
			let (intraday, _) = Self::get_data(&self.intraday, daily_offset_map, skip_front_contract)?;
			(daily, intraday)
		} else {
			let (daily, _) = Self::get_data(&self.daily, None, skip_front_contract)?;
			let (intraday, _) = Self::get_data(&self.intraday, None, skip_front_contract)?;
			(daily, intraday)
		};
		let archive = OhlcArchive {
			daily,
			intraday,
			intraday_time_frame: self.intraday_time_frame
		};
		Ok(archive)
	}

	pub fn get_most_popular_record(records: &OhlcVec, skip_front_contract: bool) -> Result<Option<OhlcRecord>> {
		if records.is_empty() {
			return Ok(None);
		} else if records.len() == 1 {
			if let Some(first) = records.first() {
				return Ok(Some(first.clone()));
			}
		}
		let filtered_records = Self::filter_records_by_contract(records, skip_front_contract)?;
		let open_interest: Vec<u32> = filtered_records
			.iter()
			.filter_map(|x| x.open_interest)
			.collect();
		let open_interest_available = open_interest.len() == filtered_records.len();
		let non_zero_open_interest = open_interest.iter().all(|x| *x > 0);
		let non_zero_volume = filtered_records
			.iter()
			.any(|x| x.volume > 0);
		let max = if open_interest_available && non_zero_open_interest {
			filtered_records
				.iter()
				.max_by_key(|x| x.open_interest.unwrap())
		} else if non_zero_volume {
			filtered_records
				.iter()
				.max_by_key(|x| x.volume)
		} else {
			// Fallback for old records from around 2000
			filtered_records
				.iter()
				.min_by_key(|x| GlobexCode::new(&x.symbol).unwrap())
		};
		Ok(Some(max.unwrap().clone()))
	}

	fn is_contract(records: &Vec<RawOhlcRecord>) -> bool {
		let mut contracts = HashSet::new();
		for x in records {
			contracts.insert(&x.symbol);
			if contracts.len() >= 2 {
				return true;
			}
		}
		false
	}

	fn get_data(records: &Vec<RawOhlcRecord>, daily_offset_map: Option<(&OhlcMap, &OffsetMap)>, skip_front_contract: bool) -> Result<(OhlcData, Option<OffsetMap>)> {
		let is_contract = Self::is_contract(records);
		if is_contract {
			// Futures contract
			let contract_map = Self::get_contract_map(records);
			let unadjusted = Self::get_unadjusted_data_from_map(&contract_map, skip_front_contract)?;
			let adjusted;
			let output_offset_map;
			if let Some((daily, offset_map)) = daily_offset_map {
				adjusted = Some(PanamaCanal::from_offset_map(&contract_map, daily, offset_map)?);
				output_offset_map = None;
			} else {
				let adjusted_data_opt = Self::get_adjusted_data_from_map(&contract_map, skip_front_contract)?;
				(adjusted, output_offset_map) = match adjusted_data_opt {
					Some((x, y)) => (Some(x), Some(y)),
					None => (None, None)
				};
			}
			let data = OhlcData {
				unadjusted,
				adjusted,
				contract_map: Some(contract_map)
			};
			Ok((data, output_offset_map))
		} else {
			// Currency
			let contract_map = None;
			let unadjusted = Self::get_unadjusted_data(records);
			let adjusted = None;
			let data = OhlcData {
				unadjusted,
				adjusted,
				contract_map
			};
			Ok((data, None))
		}
	}

	fn filter_records_by_contract(records: &OhlcVec, skip_front_contract: bool) -> Result<OhlcVec> {
		if skip_front_contract && records.len() >= 2 {
			let mut tuples: Vec<(GlobexCode, OhlcRecord)> = records
				.iter()
				.map(|record| {
					if let Some(globex_code) = GlobexCode::new(&record.symbol) {
						Ok((globex_code, record.clone()))
					} else {
						bail!("Failed to parse Globex code while filtering records")
					}
				})
				.collect::<Result<Vec<(GlobexCode, OhlcRecord)>>>()?;
			tuples.sort_by(|(globex_code1, _), (globex_code2, _)| globex_code1.cmp(globex_code2));
			let filtered_records: Vec<OhlcRecord> = tuples
				.iter()
				.skip(1)
				.map(|(_, record)| record.clone())
				.collect();
			Ok(filtered_records)
		} else {
			Ok(records.clone())
		}
	}

	fn get_unadjusted_data(records: &Vec<RawOhlcRecord>) -> OhlcMap {
		let mut output = OhlcMap::new();
		for x in records {
			output.insert(x.time, x.to_archive());
		}
		output
	}

	fn get_unadjusted_data_from_map(map: &OhlcContractMap, skip_front_contract: bool) -> Result<OhlcMap> {
		let mut output = OhlcMap::new();
		for records in map.values() {
			if let Some(record) = Self::get_most_popular_record(records, skip_front_contract)? {
				output.insert(record.time, record);
			}
		}
		Ok(output)
	}

	fn get_adjusted_data_from_map(map: &OhlcContractMap, skip_front_contract: bool) -> Result<Option<(OhlcMap, OffsetMap)>> {
		let Some(mut panama) = PanamaCanal::new(map, skip_front_contract)? else {
			return Ok(None);
		};
		let output = panama.get_adjusted_data()?;
		Ok(Some(output))
	}

	fn get_contract_map(records: &Vec<RawOhlcRecord>) -> OhlcContractMap {
		let mut map = OhlcContractMap::new();
		records.iter().for_each(|x| {
			let record = x.to_archive();
			if let Some(records) = map.get_mut(&x.time) {
				records.push(record);
			} else {
				let records = vec![record];
				map.insert(x.time, records);
			}
		});
		map
	}
}

impl RawOhlcRecord {
	pub fn to_archive(&self) -> OhlcRecord {
		OhlcRecord {
			symbol: self.symbol.clone(),
			time: self.time,
			open: self.open,
			high: self.high,
			low: self.low,
			close: self.close,
			volume: self.volume,
			open_interest: self.open_interest
		}
	}
}

impl OhlcRecord {
	pub fn apply_offset(&self, offset: f64) -> OhlcRecord {
		OhlcRecord {
			symbol: self.symbol.clone(),
			time: self.time.clone(),
			open: self.open + offset,
			high: self.high + offset,
			low: self.low + offset,
			close: self.close + offset,
			volume: self.volume,
			open_interest: self.open_interest
		}
	}
}

impl OhlcArchive {
	pub fn get_data(&self, time_frame: &TimeFrame) -> &OhlcData {
		if *time_frame == TimeFrame::Daily {
			&self.daily
		} else {
			&self.intraday
		}
	}
}

impl OhlcData {
	pub fn get_adjusted_fallback(&self) -> &OhlcMap {
		match &self.adjusted {
			Some(ref x) => x,
			None => &self.unadjusted
		}
	}
}