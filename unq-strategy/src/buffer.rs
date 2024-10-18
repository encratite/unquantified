use std::collections::VecDeque;

#[derive(Clone)]
pub struct IndicatorBuffer {
	pub buffer: VecDeque<f64>,
	pub size: usize
}

impl IndicatorBuffer {
	pub fn new(size: usize) -> Self {
		Self {
			buffer: VecDeque::new(),
			size
		}
	}

	pub fn with_slow(fast_size: usize, slow_size: Option<usize>, multiplier: usize) -> Self {
		let max_size = if let Some(slow) = slow_size {
			fast_size.max(slow)
		} else {
			fast_size
		};
		Self {
			buffer: VecDeque::new(),
			size: multiplier * max_size
		}
	}

	pub fn add(&mut self, sample: f64) {
		self.buffer.push_front(sample);
		if self.buffer.len() > self.size {
			self.buffer.pop_back();
		}
	}

	pub fn average(&self) -> f64 {
		let sum: f64 = self.buffer.iter().sum();
		let average = sum / (self.buffer.len() as f64);
		average
	}

	pub fn filled(&self) -> bool {
		self.buffer.len() >= self.size
	}

	pub fn needs_initialization(&self) -> Option<usize> {
		if self.buffer.len() < self.size {
			Some(self.size)
		} else {
			None
		}
	}
}