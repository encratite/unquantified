use std::cmp::Ordering;
use serde::{Deserialize, Serialize, Serializer};

#[derive(Clone, Debug, Deserialize)]
pub struct WebF64 {
	value: f64,
	precision: i32
}

impl WebF64 {
	pub fn new(value: f64) -> WebF64 {
		WebF64 {
			value,
			precision: 2
		}
	}

	pub fn precise(value: f64) -> WebF64 {
		WebF64 {
			value,
			precision: 3
		}
	}

	pub fn get(&self) -> f64 {
		self.value
	}
}

impl PartialOrd for WebF64 {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		self.value.partial_cmp(&other.value)
	}

	fn lt(&self, other: &Self) -> bool {
		self.value < other.value
	}

	fn le(&self, other: &Self) -> bool {
		self.value <= other.value
	}

	fn gt(&self, other: &Self) -> bool {
		self.value > other.value
	}

	fn ge(&self, other: &Self) -> bool {
		self.value >= other.value
	}
}

impl PartialEq for WebF64 {
	fn eq(&self, other: &Self) -> bool {
		self.value == other.value
	}
}

impl Serialize for WebF64 {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		let factor = 10f64.powi(self.precision);
		let rounded = (self.value * factor).round() / factor;
		serializer.serialize_f64(rounded)
	}
}