mod panama;

use std::{cmp::Ordering, collections::BTreeMap, fs::File, path::PathBuf, str::FromStr, sync::Arc};
use chrono::{DateTime, NaiveDateTime, Utc};
use chrono_tz::Tz;
use panama::PanamaChannel;
use rkyv::{Archive, Deserialize, Serialize};
use configparser::ini::Ini;
use serde::de::DeserializeOwned;
use lazy_static::lazy_static;
use regex::Regex;
use anyhow::{Context, Result, anyhow, bail};

pub type OhlcArc = Arc<OhlcRecord>;
pub type OhlcVec = Vec<OhlcArc>;
pub type OhlcTimeMap = BTreeMap<DateTime<Utc>, OhlcArc>;
pub type OhlcContractMap = BTreeMap<DateTime<Utc>, OhlcVec>;

lazy_static! {
	static ref GLOBEX_REGEX: Regex = Regex::new("^([A-Z0-9]{2,})([FGHJKMNQUVXZ])([0-9]{2})$").unwrap();
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
- "adjusted" features new records generated using the Panama channel method for use with indicators, same roll-over criteria
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

#[derive(Eq, PartialEq, PartialOrd)]
struct GlobexCode {
	pub symbol: String,
	pub month: String,
	pub year: u16
}

pub fn parse_globex_code(symbol: &String) -> Option<(String, String, String)> {
	match GLOBEX_REGEX.captures(symbol.as_str()) {
		Some(captures) => {
			let get_capture = |i: usize| captures[i].to_string();
			let root = get_capture(1);
			let month = get_capture(2);
			let year = get_capture(3);
			Some((root, month, year))
		},
		None => None
	}
}

pub fn read_archive(path: &PathBuf, skip_front_contract: bool) -> Result<OhlcArchive> {
	let file = File::open(path)?;
	let mut buffer = Vec::<u8>::new();
	zstd::stream::copy_decode(file, &mut buffer)?;
	let raw_archive: RawOhlcArchive = unsafe { rkyv::from_bytes_unchecked(&buffer)? };
	let archive = raw_archive.to_archive(skip_front_contract)?;
	return Ok(archive);
}

pub fn write_archive(path: &PathBuf, archive: &RawOhlcArchive) -> Result<()> {
	let binary_data = rkyv::to_bytes::<_, 1024>(archive)?;
	let file = File::create(path.clone())?;
	zstd::stream::copy_encode(binary_data.as_slice(), file, 1)?;
	Ok(())
}

pub fn get_ini(path: &str) -> Result<Ini> {
	let mut config = Ini::new();
	config.load(path)
		.map_err(|error| anyhow!(error))
		.with_context(|| format!("Failed to read configuration file \"{path}\""))?;
	Ok(config)
}

pub fn get_archive_file_name(symbol: &String) -> String {
	format!("{symbol}.zrk")
}

pub fn read_csv<T>(path: PathBuf, mut on_record: impl FnMut(T))
where
	T: DeserializeOwned
{
	let mut reader = csv::Reader::from_path(path)
		.expect("Unable to read .csv file");
	let headers = reader.headers()
		.expect("Unable to parse headers")
		.clone();
	let mut string_record = csv::StringRecord::new();
	while reader.read_record(&mut string_record).is_ok() && string_record.len() > 0 {
		let record: T = string_record.deserialize(Some(&headers))
			.expect("Failed to deserialize record");
		on_record(record);
	}
}

impl RawOhlcArchive {
	pub fn to_archive(&self, skip_front_contract: bool) -> Result<OhlcArchive> {
		let time_zone = Tz::from_str(self.time_zone.as_str())
			.expect("Invalid time zone in archive");
		let daily = Self::get_data(&self.daily, &time_zone, skip_front_contract)?;
		let intraday = Self::get_data(&self.intraday, &time_zone, skip_front_contract)?;
		let archive = OhlcArchive {
			daily,
			intraday,
			intraday_time_frame: self.intraday_time_frame
		};
		Ok(archive)
	}

	fn get_data(records: &Vec<RawOhlcRecord>, time_zone: &Tz, skip_front_contract: bool) -> Result<OhlcData> {
		let Some(last) = records.last() else {
			bail!("Encountered an OHLC archive without any records");
		};
		let is_contract = last.open_interest.is_some();
		if is_contract {
			// Futures contract
			let contract_map = Self::get_contract_map(records, time_zone);
			let unadjusted = Self::get_unadjusted_data_from_map(&contract_map, skip_front_contract)?;
			let adjusted = Self::get_adjusted_data_from_map(&contract_map, skip_front_contract)?;
			let time_map = Self::get_time_map(&unadjusted, &adjusted);
			let data = OhlcData {
				unadjusted,
				adjusted,
				time_map,
				contract_map: Some(contract_map)
			};
			Ok(data)
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
			Ok(data)
		}
	}

	fn filter_records_by_contract(records: &OhlcVec, skip_front_contract: bool) -> Result<OhlcVec> {
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
				.collect::<Result<Vec<(GlobexCode, OhlcArc)>>>()?;
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

	fn get_most_popular_record(records: &OhlcVec, skip_front_contract: bool) -> Result<OhlcArc> {
		if records.len() == 1 {
			if let Some(first) = records.first() {
				return Ok(first.clone());
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
			// Fallback for really old records from around 2000
			filtered_records
				.iter()
				.min_by_key(|x| GlobexCode::new(&x.symbol).unwrap())
		};
		Ok(max.unwrap().clone())
	}

	fn get_unadjusted_data(records: &Vec<RawOhlcRecord>, time_zone: &Tz) -> OhlcVec {
		records.iter().map(|x| {
			let record = x.to_archive(&time_zone);
			Arc::new(record)
		}).collect()
	}

	fn get_unadjusted_data_from_map(map: &OhlcContractMap, skip_front_contract: bool) -> Result<OhlcVec> {
		map.values().map(|records| {
			Self::get_most_popular_record(records, skip_front_contract)
		}).collect()
	}

	fn get_adjusted_data_from_map(map: &OhlcContractMap, skip_front_contract: bool) -> Result<Option<OhlcVec>> {
		let Some(mut panama) = PanamaChannel::new(map, skip_front_contract)? else {
			return Ok(None);
		};
		let adjusted = panama.get_adjusted_data()?;
		Ok(Some(adjusted))
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

impl OhlcData {
	pub fn get_adjusted_fallback(&self) -> &OhlcVec {
		match &self.adjusted {
			Some(ref x) => x,
			None => &self.unadjusted
		}
	}
}

impl GlobexCode {
	fn new(symbol: &String) -> Option<GlobexCode> {
		let Some((_, month, year_string)) = parse_globex_code(symbol) else {
			return None;
		};
		let Ok(year) = str::parse::<u16>(year_string.as_str()) else {
			return None;
		};
		let adjusted_year = if year < 70 {
			year + 2000
		} else {
			year + 1900
		};
		let globex_code = GlobexCode {
			symbol: symbol.clone(),
			month,
			year: adjusted_year
		};
		Some(globex_code)
	}
}

impl Ord for GlobexCode {
	fn cmp(&self, other: &Self) -> Ordering {
		self.year
			.cmp(&other.year)
			.then_with(|| self.month.cmp(&other.month))
	}
}