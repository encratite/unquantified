use chrono::{Duration, Local, Months, NaiveDateTime, TimeDelta, Timelike};
use serde::Deserialize;
use anyhow::{Result, anyhow, Context};
use unq_common::ohlc::{OhlcArchive, TimeFrame};

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
	date: Option<NaiveDateTime>,
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
	  If it is being used with multiple symbols, the minimum point in time out of all of them is used.
	  This keyword may only be used for the "from" parameter.
	- "last": Evaluates to the last point in time at which data is available. With multiple symbols, the maximum is used.
	  This keyword may only be used for the "to" parameter.
	- "now": Evaluates to the current point in time.
	  This keyword may only be used for the "to" parameter.
	*/
	special_keyword: Option<SpecialDateTime>
}

impl RelativeDateTime {
	pub fn resolve(&self, other: &RelativeDateTime, time_frame: &TimeFrame, archives: &Vec<&OhlcArchive>) -> Result<NaiveDateTime> {
		match (self.date.is_some(), self.offset.is_some(), self.offset_unit.is_some(), self.special_keyword.is_some()) {
			(true, false, false, false) => Ok(self.date.unwrap()),
			(false, true, true, false) => {
				let other_time = other.to_fixed(time_frame, archives)?;
				let offset_time = get_offset_date_time(&other_time, self.offset.unwrap(), self.offset_unit.clone().unwrap())
					.expect("Invalid offset calculation".into());
				Ok(offset_time)
			},
			(false, false, false, true) => {
				let special_time = resolve_keyword(self.special_keyword.clone().unwrap(), time_frame, archives)?;
				Ok(special_time)
			},
			_ => Err(anyhow!("Invalid relative date time"))
		}
	}

	fn to_fixed(&self, time_frame: &TimeFrame, archives: &Vec<&OhlcArchive>) -> Result<NaiveDateTime> {
		match (self.date.is_some(), self.special_keyword.is_some()) {
			(true, false) => Ok(self.date.unwrap()),
			(false, true) => {
				let special_time = resolve_keyword(self.special_keyword.clone().unwrap(), time_frame, archives)?;
				Ok(special_time)
			},
			_ => Err(anyhow!("Invalid combination of relative date time parameters"))
		}
	}
}

fn resolve_first_last(is_first: bool, time_frame: &TimeFrame, archive: &OhlcArchive) -> Result<NaiveDateTime> {
	let data = archive.get_data(time_frame);
	let mut time_values = data.get_adjusted_fallback()
		.iter()
		.map(|x| x.time);
	let get_some_time = |time: Option<NaiveDateTime>| match time {
		Some(x) => Ok(x),
		None => Err(anyhow!("No records available"))
	};
	if is_first {
		get_some_time(time_values.next())
	} else {
		get_some_time(time_values.last())
	}
}

fn resolve_keyword(special_keyword: SpecialDateTime, time_frame: &TimeFrame, archives: &Vec<&OhlcArchive>) -> Result<NaiveDateTime> {
	if special_keyword == SpecialDateTime::Now {
		let now = Local::now();
		let time = now
			.with_minute(0)
			.and_then(|x| x.with_second(0))
			.and_then(|x| x.with_nanosecond(0))
			.with_context(|| anyhow!("Failed to adjust time"))?
			.naive_local();
		Ok(time)
	} else {
		let is_first = special_keyword == SpecialDateTime::First;
		let times = archives
			.iter()
			.map(|x| resolve_first_last(is_first, time_frame, x))
			.collect::<Result<Vec<NaiveDateTime>>>()?;
		let time = if is_first {
			times.iter().min()
		} else {
			times.iter().max()
		};
		match time {
			Some(x) => Ok(*x),
			None => Err(anyhow!("No records available"))
		}
	}
}

fn get_offset_date_time(time: &NaiveDateTime, offset: i16, offset_unit: OffsetUnit) -> Option<NaiveDateTime> {
	let add_signed = |duration: fn(i64) -> TimeDelta, x: i16|
		time.checked_add_signed(duration(x as i64));
	let get_months = |x: i16| if x >= 0 {
		Months::new(x as u32)
	} else {
		Months::new((- x) as u32)
	};
	let add_sub_months = |x| {
		let months = get_months(x);
		if offset >= 0 {
			time.checked_add_months(months)
		} else {
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