use std::collections::{BTreeMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use anyhow::{anyhow, bail};
use chrono::{DateTime, NaiveDateTime, Utc};
use chrono_tz::Tz;
use rkyv::{Archive, Deserialize, Serialize};
use crate::GlobexCode;
use crate::panama::{OffsetMap, PanamaCanal};

pub type OhlcArc = Arc<OhlcRecord>;
pub type OhlcVec = Vec<OhlcArc>;
pub type OhlcTimeMap = BTreeMap<DateTime<Utc>, OhlcArc>;
pub type OhlcContractMap = BTreeMap<DateTime<Utc>, OhlcVec>;

#[derive(Debug, Clone, PartialEq)]
pub enum TimeFrame {
	Daily,
	Intraday
}

#[derive(Debug, Archive, Serialize, Deserialize)]
pub struct RawOhlcArchive {
	pub daily: Vec<RawOhlcRecord>,
	pub intraday: Vec<RawOhlcRecord>,
	pub intraday_time_frame: u16,
	pub time_zone: String
}

#[derive(Debug, Archive, Serialize, Deserialize)]
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

#[derive(Debug)]
pub struct OhlcArchive {
	pub daily: OhlcData,
	pub intraday: OhlcData,
	pub intraday_time_frame: u16
}

/*
General container for the actual OHLC records, both as a map for time-based lookups and as a vector for efficiently selecting ranges.
The post-processed records feature an additional index into the vector that can be used to accelerate lookups of all records between t_1 and t_2.
The underlying OHLC type is boxed to reduce memory usage. The contents of vector and map depend on the type of asset:

Currency pair:
- "unadjusted" contains the original records in ascending order
- "adjusted" is None
- "time_map" maps timestamps to records
- "contract_map" is None

Futures:
- Both "unadjusted"/"adjusted" contains a continuous contract with new records generated from multiple overlapping contracts
- In the case of "unadjusted" it is the original values with automatic roll-overs based on volume and open interest
- "adjusted" features new records generated using the Panama Canal method for use with indicators, same roll-over criteria
- "time_map" maps timestamps to continuous contract data in "adjusted"
- Each vector in "contract_map" contains the full set of active contracts for that particular point in time
 */
#[derive(Debug)]
pub struct OhlcData {
	pub unadjusted: OhlcVec,
	pub adjusted: Option<OhlcVec>,
	pub time_map: OhlcTimeMap,
	pub contract_map: Option<OhlcContractMap>
}

#[derive(Debug, Clone)]
pub struct OhlcRecord {
	pub symbol: String,
	pub time: DateTime<Tz>,
	pub open: f64,
	pub high: f64,
	pub low: f64,
	pub close: f64,
	pub volume: u32,
	pub open_interest: Option<u32>
}

impl RawOhlcArchive {
	pub fn to_archive(&self, skip_front_contract: bool) -> anyhow::Result<OhlcArchive> {
		let time_zone = Tz::from_str(self.time_zone.as_str())
			.expect("Invalid time zone in archive");
		let (daily, offset_map_opt) = Self::get_data(&self.daily, None, &time_zone, skip_front_contract)?;
		let Some(offset_map) = offset_map_opt else {
			bail!("Missing offset map");
		};
		let Some(daily_adjusted) = &daily.adjusted else {
			bail!("Missing daily adjusted records");
		};
		let daily_offset_map = Some((daily_adjusted, &offset_map));
		let (intraday, _) = Self::get_data(&self.intraday, daily_offset_map, &time_zone, skip_front_contract)?;
		let archive = OhlcArchive {
			daily,
			intraday,
			intraday_time_frame: self.intraday_time_frame
		};
		Ok(archive)
	}

	pub fn get_most_popular_record(records: &OhlcVec, skip_front_contract: bool) -> anyhow::Result<Option<OhlcArc>> {
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

	fn get_data(records: &Vec<RawOhlcRecord>, daily_offset_map: Option<(&OhlcVec, &OffsetMap)>, time_zone: &Tz, skip_front_contract: bool) -> anyhow::Result<(OhlcData, Option<OffsetMap>)> {
		let is_contract = Self::is_contract(records);
		if is_contract {
			// Futures contract
			let contract_map = Self::get_contract_map(records, time_zone);
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
			let time_map = Self::get_time_map(&unadjusted, &adjusted);
			let data = OhlcData {
				unadjusted,
				adjusted,
				time_map,
				contract_map: Some(contract_map)
			};
			Ok((data, output_offset_map))
		} else {
			// Currency
			let contract_map = None;
			let unadjusted = Self::get_unadjusted_data(records, time_zone);
			let adjusted = None;
			let time_map = Self::get_time_map(&unadjusted, &adjusted);
			let data = OhlcData {
				unadjusted,
				adjusted,
				time_map,
				contract_map
			};
			Ok((data, None))
		}
	}

	fn filter_records_by_contract(records: &OhlcVec, skip_front_contract: bool) -> anyhow::Result<OhlcVec> {
		if skip_front_contract && records.len() >= 2 {
			let mut tuples: Vec<(GlobexCode, OhlcArc)> = records
				.iter()
				.map(|record| {
					if let Some(globex_code) = GlobexCode::new(&record.symbol) {
						Ok((globex_code, record.clone()))
					} else {
						Err(anyhow!("Failed to parse Globex code while filtering records"))
					}
				})
				.collect::<anyhow::Result<Vec<(GlobexCode, OhlcArc)>>>()?;
			tuples.sort_by(|(globex_code1, _), (globex_code2, _)| globex_code1.cmp(globex_code2));
			let filtered_records: Vec<OhlcArc> = tuples
				.iter()
				.skip(1)
				.map(|(_, record)| record.clone())
				.collect();
			Ok(filtered_records)
		} else {
			Ok(records.clone())
		}
	}

	fn get_unadjusted_data(records: &Vec<RawOhlcRecord>, time_zone: &Tz) -> OhlcVec {
		records.iter().map(|x| {
			let record = x.to_archive(&time_zone);
			Arc::new(record)
		}).collect()
	}

	fn get_unadjusted_data_from_map(map: &OhlcContractMap, skip_front_contract: bool) -> anyhow::Result<OhlcVec> {
		map.values()
			.map(|records| {
				Self::get_most_popular_record(records, skip_front_contract)
			})
			.filter_map(|result| match result {
				Ok(Some(value)) => Some(Ok(value)),
				Ok(None) => None,
				Err(err) => Some(Err(err)),
			})
			.collect()
	}

	fn get_adjusted_data_from_map(map: &OhlcContractMap, skip_front_contract: bool) -> anyhow::Result<Option<(OhlcVec, OffsetMap)>> {
		let Some(mut panama) = PanamaCanal::new(map, skip_front_contract)? else {
			return Ok(None);
		};
		let output = panama.get_adjusted_data()?;
		Ok(Some(output))
	}

	fn get_contract_map(records: &Vec<RawOhlcRecord>, time_zone: &Tz) -> OhlcContractMap {
		let mut map = OhlcContractMap::new();
		records.iter().for_each(|x| {
			let time = x.get_time_utc(time_zone);
			let record = x.to_archive(&time_zone);
			let value = Arc::new(record);
			if let Some(records) = map.get_mut(&time) {
				records.push(value);
			} else {
				let records = vec![value];
				map.insert(time, records);
			}
		});
		map
	}

	fn get_time_map(unadjusted: &OhlcVec, adjusted: &Option<OhlcVec>) -> OhlcTimeMap {
		let source = match adjusted {
			Some(adjusted_vec) => adjusted_vec,
			None => unadjusted
		};
		let mut map = OhlcTimeMap::new();
		for record in source {
			let key = record.time.to_utc();
			let value = record.clone();
			map.insert(key, value);
		}
		map
	}
}

impl RawOhlcRecord {
	pub fn to_archive(&self, time_zone: &Tz) -> OhlcRecord {
		let time_tz = self.get_time_tz(time_zone);
		OhlcRecord {
			symbol: self.symbol.clone(),
			time: time_tz,
			open: self.open,
			high: self.high,
			low: self.low,
			close: self.close,
			volume: self.volume,
			open_interest: self.open_interest
		}
	}

	pub fn get_time_tz(&self, time_zone: &Tz) -> DateTime<Tz> {
		let time_utc = DateTime::<Utc>::from_naive_utc_and_offset(self.time, Utc);
		time_utc.with_timezone(time_zone)
	}

	pub fn get_time_utc(&self, time_zone: &Tz) -> DateTime<Utc> {
		let time_tz = self.get_time_tz(time_zone);
		time_tz.to_utc()
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
	pub fn get_adjusted_fallback(&self) -> &OhlcVec {
		match &self.adjusted {
			Some(ref x) => x,
			None => &self.unadjusted
		}
	}
}