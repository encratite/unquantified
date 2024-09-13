use anyhow::{bail, Result};
use unq_common::ohlc::OhlcRecord;
use unq_common::stats::{mean, standard_deviation_mean_biased};

const MACD_SIGNAL_PERIOD: usize = 9;
const MACD_FAST_PERIOD: usize = 12;
const MACD_SLOW_PERIOD: usize = 26;

pub fn momentum(records: &Vec<&OhlcRecord>, period: usize) -> Result<f64> {
	if period > records.len() {
		bail!("Not enough samples to calculate MOM({period})");
	}
	let momentum = records[0].close / records[period - 1].close - 1.0;
	Ok(momentum)
}

pub fn simple_moving_average(records: &Vec<&OhlcRecord>, period: usize) -> Result<f64> {
	if period > records.len() {
		bail!("Not enough samples to calculate SMA({period})");
	}
	let sum: f64 = records
		.iter()
		.take(period)
		.map(|x| x.close)
		.sum();
	let average = sum / (period as f64);
	Ok(average)
}

pub fn weighted_moving_average(records: &Vec<&OhlcRecord>, period: usize) -> Result<f64> {
	if period > records.len() {
		bail!("Not enough samples to calculate LMA({period})");
	}
	let mut average = 0.0;
	let mut i = 0;
	for x in records.iter().take(period) {
		average += ((period - i) as f64) * x.close;
		i += 1;
	}
	average /= ((period * (period + 1)) as f64) / 2.0;
	Ok(average)
}

pub fn exponential_moving_average(records: &Vec<&OhlcRecord>, period: usize) -> Result<f64> {
	let closes = get_closes(records);
	let ema = exponential_internal(closes, period);
	Ok(ema)
}

pub fn average_true_range(records: &Vec<&OhlcRecord>, period: usize) -> Result<f64> {
	if period + 1 > records.len() {
		bail!("Not enough samples to calculate ATR({period})");
	}
	let mut sum = 0.0;
	for window in records.windows(2).take(period) {
		let [x, x_previous] = window else {
			bail!("Invalid window shape");
		};
		let part1 = x.high - x.low;
		let part2 = (x.high - x_previous.close).abs();
		let part3 = (x.low - x_previous.close).abs();
		let true_range = part1.max(part2).max(part3);
		sum += true_range;
	}
	let average = sum / (period as f64);
	Ok(average)
}

pub fn normalized_true_range(records: &Vec<&OhlcRecord>, period: usize) -> Result<f64> {
	let average = average_true_range(records, period)?;
	let normalized = average / records[0].close;
	Ok(normalized)
}

pub fn relative_strength_index(records: &Vec<&OhlcRecord>, period: usize) -> Result<f64> {
	if period + 1 > records.len() {
		bail!("Not enough samples to calculate RSI({period})");
	}
	let mut up = Vec::new();
	let mut down = Vec::new();
	for window in records.windows(2).take(period) {
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
	let up_ema = exponential_internal(up, period);
	let down_ema = exponential_internal(down, period);
	let rsi = 100.0 * up_ema / (up_ema + down_ema);
	Ok(rsi)
}

pub fn moving_average_convergence(records: &Vec<&OhlcRecord>) -> Result<(f64, f64)> {
	if records.len() < MACD_SLOW_PERIOD {
		bail!("Not enough samples to calculate MACD");
	}
	let signal = exponential_moving_average(records, MACD_SIGNAL_PERIOD)?;
	let fast_ema = exponential_moving_average(records, MACD_FAST_PERIOD)?;
	let slow_ema = exponential_moving_average(records, MACD_SLOW_PERIOD)?;
	let macd = fast_ema - slow_ema;
	Ok((signal, macd))
}

pub fn percentage_price_oscillator(records: &Vec<&OhlcRecord>) -> Result<(f64, f64)> {
	if records.len() < MACD_SIGNAL_PERIOD + MACD_SLOW_PERIOD {
		bail!("Not enough samples to calculate PPO");
	}
	let windows = records
		.windows(MACD_SLOW_PERIOD)
		.take(MACD_SIGNAL_PERIOD);
	let ppo_samples = windows
		.map(|window| {
			let ppo_records = window.iter().cloned().collect();
			percentage_price_internal(&ppo_records)
		})
		.collect::<Result<Vec<f64>>>()?;
	let ppo = ppo_samples[0];
	let signal = exponential_internal(ppo_samples, MACD_SIGNAL_PERIOD);
	Ok((signal, ppo))
}

pub fn bollinger_bands(records: &Vec<&OhlcRecord>, period: usize, multiplier: f64) -> Result<(f64, f64, f64)> {
	let signal = exponential_moving_average(records, period)?;
	let closes = get_closes(records);
	let mean = mean(&closes)?;
	let standard_deviation = standard_deviation_mean_biased(&closes, mean)?;
	let upper = mean + multiplier * standard_deviation;
	let lower = mean - multiplier * standard_deviation;
	Ok((signal, upper, lower))
}

fn get_closes(records: &Vec<&OhlcRecord>) -> Vec<f64> {
	let closes = records
		.iter()
		.map(|x| x.close)
		.collect::<Vec<f64>>();
	closes
}

fn exponential_internal(records: Vec<f64>, period: usize) -> f64 {
	let mut average = 0.0;
	let mut i = 0;
	let lambda = 2.0 / ((period + 1) as f64);
	for x in records {
		average += lambda * (1.0 - lambda).powi(i) * x;
		i += 1;
	}
	average
}

fn percentage_price_internal(records: &Vec<&OhlcRecord>) -> Result<f64> {
	if records.len() < MACD_SLOW_PERIOD {
		bail!("Not enough samples to calculate PPO");
	}
	let fast_ema = exponential_moving_average(records, MACD_FAST_PERIOD)?;
	let slow_ema = exponential_moving_average(records, MACD_SLOW_PERIOD)?;
	let ppo = 100.0 * (fast_ema - slow_ema) / slow_ema;
	Ok(ppo)
}