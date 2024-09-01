use serde::{Serialize, Serializer};

#[derive(Clone)]
pub struct WebF64(pub f64);

impl WebF64 {
	pub fn get(&self) -> f64 {
		self.0
	}
}

impl Serialize for WebF64 {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		let precision = 100f64;
		let rounded = (self.0 * precision).round() / precision;
		serializer.serialize_f64(rounded)
	}
}