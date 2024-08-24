use std::cmp::Ordering;
use lazy_static::lazy_static;
use regex::Regex;

lazy_static! {
	static ref GLOBEX_REGEX: Regex = Regex::new("^([A-Z0-9]{2,})([FGHJKMNQUVXZ])([0-9]{2})$").unwrap();
}

#[derive(Eq, PartialEq, PartialOrd)]
pub struct GlobexCode {
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

impl GlobexCode {
	pub fn new(symbol: &String) -> Option<GlobexCode> {
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