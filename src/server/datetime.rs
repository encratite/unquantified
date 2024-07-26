use std::{error::Error, sync::Arc};
use chrono::{DateTime, Duration, FixedOffset, Local, Months, NaiveDateTime, TimeDelta, TimeZone};
use chrono_tz::Tz;
use common::OhlcArchive;
use serde::Deserialize;

#[derive(Deserialize, Clone)]
enum OffsetUnit {
	#[serde(rename = "m")]
	Minutes,
	#[serde(rename = "h")]
	Hours,
	#[serde(rename = "d")]
	Days,
	#[serde(rename = "w")]
	Weeks,
	#[serde(rename = "mo")]
	Months,
	#[serde(rename = "y")]
	Years
}

#[derive(Deserialize, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
enum SpecialDateTime {
	First,
	Last,
	Now
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RelativeDateTime {
	date: Option<DateTime<FixedOffset>>,
	/*
	offset and offset_unit encode relative offsets such as +15m, -1w and -4y.
	If set, all other members of RelativeDateTime must be set to None.
	The following unit strings are supported:
	- "m": minutes
	- "h": hours
	- "d": days
	- "w": weeks
	- "mo": months
	- "y": years
	*/
	offset: Option<i16>,
	offset_unit: Option<OffsetUnit>,
	/*
	This optional member is used for the special keywords in the Unquantified prompt language:
	- "first": Evaluates to the first point in time at which data is available for the specified symbol.
	  If it is being used with multiple symbols, the minmum point in time out of all of them is used.
	  This keyword may only be used for the "from" parameter.
	- "last": Evaluates to the last point in time at wich data is available. With multiple symbols, the maximum is used.
	  This keyword may only be used for the "to" parameter.
	- "now": Evaluates to the current point in time.
	  This keyword may only be used for the "to" parameter.
	*/
	special_keyword: Option<SpecialDateTime>
}

impl RelativeDateTime {
	pub fn resolve(&self, other: &RelativeDateTime, archives: &Vec<Arc<OhlcArchive>>) -> Result<DateTime<FixedOffset>, Box<dyn Error>> {
		match (self.date.is_some(), self.offset.is_some(), self.offset_unit.is_some(), self.special_keyword.is_some()) {
			(true, false, false, false) => Ok(self.date.unwrap()),
			(false, true, true, false) => {
				let other_time = other.to_fixed(archives)?;
				let offset_time = get_offset_date_time(&other_time, self.offset.unwrap(), self.offset_unit.clone().unwrap())
					.expect("Invalid offset calculation".into());
				Ok(offset_time)
			},
			(false, false, false, true) => {
				let special_time = resolve_keyword(self.special_keyword.clone().unwrap(), archives)?;
				Ok(special_time)
			},
			_ => Err("Invalid relative date time".into())
		}
	}

	fn to_fixed(&self, archives: &Vec<Arc<OhlcArchive>>) -> Result<DateTime<FixedOffset>, Box<dyn Error>> {
		match (self.date.is_some(), self.special_keyword.is_some()) {
			(true, false) => Ok(self.date.unwrap()),
			(false, true) => {
				let special_time = resolve_keyword(self.special_keyword.clone().unwrap(), archives)?;
				Ok(special_time)
			},
			_ => Err("Invalid combination of relative date times".into())
		}
	}
}

pub fn get_date_time_tz(time: NaiveDateTime, tz: &Tz) -> DateTime<Tz> {
	tz.from_local_datetime(&time)
		.single()
		.unwrap()
}

fn resolve_first_last(is_first: bool, archive: &Arc<OhlcArchive>) -> Result<DateTime<FixedOffset>, Box<dyn Error>> {
	let mut time_values = archive.intraday
		.values()
		.map(|x| x.time);
	let get_some_time = |time: Option<DateTime<Tz>>| match time {
		Some(x) => Ok(x.fixed_offset()),
		None => Err("No records available".into())
	};
	if is_first {
		get_some_time(time_values.next())
	}
	else {
		get_some_time(time_values.last())
	}
}

fn resolve_keyword(special_keyword: SpecialDateTime, archives: &Vec<Arc<OhlcArchive>>) -> Result<DateTime<FixedOffset>, Box<dyn Error>> {
	if special_keyword == SpecialDateTime::Now {
		let now: DateTime<Local> = Local::now();
		let now_with_timezone: DateTime<FixedOffset> = now.with_timezone(now.offset());
		Ok(now_with_timezone)
	}
	else {
		let is_first = special_keyword == SpecialDateTime::First;
		let times = archives
			.iter()
			.map(|x| resolve_first_last(is_first, x))
			.collect::<Result<Vec<DateTime<FixedOffset>>, Box<dyn Error>>>()?;
		let time = if is_first {
			times.iter().min()
		}
		else {
			times.iter().max()
		};
		match time {
			Some(x) => Ok(*x),
			None => Err("No records available".into())
		}
	}
}

fn get_offset_date_time(time: &DateTime<FixedOffset>, offset: i16, offset_unit: OffsetUnit) -> Option<DateTime<FixedOffset>> {
	let add_signed = |duration: fn(i64) -> TimeDelta, x: i16|
		time.checked_add_signed(duration(x as i64));
	let get_months = |x: i16| if x >= 0 {
		Months::new(x as u32)
	}
	else {
		Months::new((- x) as u32)
	};
	let add_sub_months = |x| {
		let months = get_months(x);
		if offset >= 0 {
			time.checked_add_months(months)
		}
		else {
			time.checked_sub_months(months)
		}
	};
	match offset_unit {
		OffsetUnit::Minutes => add_signed(Duration::minutes, offset),
		OffsetUnit::Hours => add_signed(Duration::hours, offset),
		OffsetUnit::Days => add_signed(Duration::days, offset),
		OffsetUnit::Weeks => add_signed(Duration::days, 7 * offset),
		OffsetUnit::Months => add_sub_months(offset),
		OffsetUnit::Years => add_sub_months(12 * offset),
	}
}