use anyhow::{bail, Result};
use unq_common::ohlc::OhlcRecord;

pub fn momentum(records: &Vec<&OhlcRecord>, n: usize) -> Result<f64> {
	if n > records.len() {
		bail!("Not enough samples to calculate MOM({n})");
	}
	let momentum = records[0].close / records[n - 1].close;
	Ok(momentum)
}

pub fn simple_moving_average(records: &Vec<&OhlcRecord>, n: usize) -> Result<f64> {
	if n > records.len() {
		bail!("Not enough samples to calculate SMA({n})");
	}
	let sum: f64 = records
		.iter()
		.take(n)
		.map(|x| x.close)
		.sum();
	let average = sum / (n as f64);
	Ok(average)
}

pub fn linear_moving_average(records: &Vec<&OhlcRecord>, n: usize) -> Result<f64> {
	if n > records.len() {
		bail!("Not enough samples to calculate LMA({n})");
	}
	let mut average = 0.0;
	let mut i = 0;
	for x in records.iter().take(n) {
		average += ((n - i) as f64) / (n as f64) * x.close;
		i += 1;
	}
	Ok(average)
}

pub fn exponential_moving_average(records: &Vec<&OhlcRecord>, n: usize) -> Result<f64> {
	let closes = records
		.iter()
		.map(|x| x.close)
		.collect::<Vec<f64>>();
	let ema = ema_internal(closes, n);
	Ok(ema)
}

pub fn average_true_range(records: &Vec<&OhlcRecord>, n: usize) -> Result<f64> {
	if n + 1 > records.len() {
		bail!("Not enough samples to calculate ATR({n})");
	}
	let mut sum = 0.0;
	for window in records.windows(2).take(n) {
		let [x, x_previous] = window else {
			bail!("Invalid window shape");
		};
		let part1 = x.high - x.low;
		let part2 = (x.high - x_previous.close).abs();
		let part3 = (x.low - x_previous.close).abs();
		let true_range = part1.max(part2).max(part3);
		sum += true_range;
	}
	let average = sum / (n as f64);
	Ok(average)
}

pub fn normalized_true_range(records: &Vec<&OhlcRecord>, n: usize) -> Result<f64> {
	let average = average_true_range(records, n)?;
	let normalized = average / records[0].close;
	Ok(normalized)
}

pub fn relative_strength_index(records: &Vec<&OhlcRecord>, n: usize) -> Result<f64> {
	if n + 1 > records.len() {
		bail!("Not enough samples to calculate RSI({n})");
	}
	let mut up = Vec::new();
	let mut down = Vec::new();
	for window in records.windows(2).take(n) {
		let [x, x_previous] = window else {
			bail!("Invalid window shape");
		};
		let difference = x.close - x_previous.close;
		if difference >= 0.0 {
			up.push(difference)
		} else {
			down.push(difference)
		}
	}
	let up_ema = ema_internal(up, n);
	let down_ema = ema_internal(down, n);
	let rsi = 100.0 * up_ema / (up_ema + down_ema);
	Ok(rsi)
}

fn ema_internal(records: Vec<f64>, n: usize) -> f64 {
	let mut average = 0.0;
	let mut i = 0;
	let lambda = 2.0 / ((n + 1) as f64);
	for x in records {
		average += lambda * (1.0 - lambda).powi(i) * x;
		i += 1;
	}
	average
}