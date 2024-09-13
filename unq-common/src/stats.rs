use anyhow::{bail, Result};

pub fn mean(samples: &Vec<f64>) -> Result<f64> {
	let sum: f64 = samples.iter().sum();
	let n = samples.len();
	if n < 1 {
		bail!("Not enough samples to calculate mean");
	}
	let mean = sum / (n as f64);
	Ok(mean)
}

pub fn standard_deviation(samples: &Vec<f64>) -> Result<f64> {
	let mean = mean(samples)?;
	standard_deviation_internal(samples, mean, true)
}

pub fn standard_deviation_mean(samples: &Vec<f64>, mean: f64) -> Result<f64> {
	standard_deviation_internal(samples, mean, true)
}

pub fn standard_deviation_mean_biased(samples: &Vec<f64>, mean: f64) -> Result<f64> {
	standard_deviation_internal(samples, mean, false)
}

fn standard_deviation_internal(samples: &Vec<f64>, mean: f64, correction: bool) -> Result<f64> {
	let mut delta_sum = 0.0;
	for x in samples {
		let delta = x - mean;
		delta_sum += delta * delta;
	}
	let n = samples.len();
	if n < 2 {
		bail!("Not enough samples to calculate standard deviation");
	}
	let divisor = if correction {
		n - 1
	} else {
		n
	};
	let standard_deviation = (delta_sum / (divisor as f64)).sqrt();
	Ok(standard_deviation)
}