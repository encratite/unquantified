use anyhow::{bail, Result};

pub fn mean<'a, I>(samples: I) -> Result<f64>
where
	I: Iterator<Item = &'a f64>
{
	let mut sum = 0.0;
	let mut n = 0;
	for x in samples {
		sum += x;
		n += 1;
	}
	if n < 1 {
		bail!("Not enough samples to calculate mean");
	}
	let mean = sum / (n as f64);
	Ok(mean)
}

pub fn standard_deviation<'a, I>(samples: I) -> Result<f64>
where
	I: Iterator<Item = &'a f64> + Clone
{
	let mean = mean(samples.clone())?;
	standard_deviation_internal(samples, mean, true)
}

pub fn standard_deviation_mean<'a, I>(samples: I, mean: f64) -> Result<f64>
where
	I: Iterator<Item = &'a f64>
{
	standard_deviation_internal(samples, mean, true)
}

pub fn standard_deviation_mean_biased<'a, I>(samples: I, mean: f64) -> Result<f64>
where
	I: Iterator<Item = &'a f64>
{
	standard_deviation_internal(samples, mean, false)
}

fn standard_deviation_internal<'a, I>(samples: I, mean: f64, correction: bool) -> Result<f64>
where
	I: Iterator<Item = &'a f64>
{
	let mut delta_sum = 0.0;
	let mut n = 0;
	for x in samples {
		let delta = x - mean;
		delta_sum += delta * delta;
		n += 1;
	}
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