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