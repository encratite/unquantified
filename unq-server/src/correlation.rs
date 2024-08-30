use std::{collections::HashMap, sync::Arc};
use chrono::NaiveDateTime;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde::Serialize;
use anyhow::{bail, Context, Result};
use unq_common::ohlc::{OhlcArc, OhlcArchive, OhlcVec};

#[derive(Debug, Serialize)]
pub struct CorrelationData {
	pub symbols: Vec<String>,
	pub from: NaiveDateTime,
	pub to: NaiveDateTime,
	pub correlation: Vec<Vec<f64>>,
}

pub fn get_correlation_matrix(symbols: Vec<String>, request_from: NaiveDateTime, request_to: NaiveDateTime, archives: Vec<Arc<OhlcArchive>>) -> Result<CorrelationData> {
	// Determine smallest overlapping time range across all OHLC records
	let (from, to) = get_common_time_range(request_from, request_to, &archives)?;
	// Retrieve pre-calculated x_i - x_mean values for each ticker
	let delta_samples = get_delta_samples(&from, &to, &archives)?;
	// Create a square a matrix, default to 1.0 for diagonal elements
	let count = archives.len();
	let mut matrix = vec![vec![1f64; count]; count];
	// Generate a list of pairs (i, j) of indices for one half of the matrix, excluding the diagonal, for parallel processing
	let mut pairs = Vec::new();
	for i in 0..count {
		for j in 0..count {
			if i < j {
				pairs.push((i, j));
			}
		}
	}
	// Calculate Pearson correlation coefficients
	let coefficients: Vec<(usize, usize, f64)> = pairs.par_iter().map(|(i, j)| {
		let (x_samples, x_sqrt) = &delta_samples[*i];
		let (y_samples, y_sqrt) = &delta_samples[*j];
		assert!(x_samples.len() == y_samples.len());
		let mut sum = 0.0;
		for k in 0..x_samples.len() {
			let delta_x = x_samples[k];
			let delta_y = y_samples[k];
			sum += delta_x * delta_y;
		}
		let coefficient = sum / (x_sqrt * y_sqrt);
		(*i, *j, coefficient)
	}).collect();
	// Store correlation coefficients symmetrically
	for (i, j, coefficient) in coefficients {
		matrix[i][j] = coefficient;
		matrix[j][i] = coefficient;
	}
	let output = CorrelationData {
		symbols,
		from: from,
		to: to,
		correlation: matrix
	};
	Ok(output)
}

fn get_common_time_range(request_from: NaiveDateTime, request_to: NaiveDateTime, archives: &Vec<Arc<OhlcArchive>>)
	-> Result<(NaiveDateTime, NaiveDateTime)> {
	let mut from = request_from;
	let mut to = request_to;
	for archive in archives {
			let get_time = |x: &OhlcArc| Some(x.time);
			let records = get_records(archive);
			let first = records
				.first()
				.and_then(get_time);
			let last = records
				.last()
				.and_then(get_time);
			match (first, last) {
				(Some(first_time), Some(last_time)) => {
					from = from.max(first_time);
					to = to.min(last_time);
				}
				_ => bail!("Missing records in archive")
			}
		}
	Ok((from, to))
}

fn get_delta_samples(from: &NaiveDateTime, to: &NaiveDateTime, archives: &Vec<Arc<OhlcArchive>>) -> Result<Vec<(Vec<f64>, f64)>> {
	// Create an index map to make sure that each cell in the matrix corresponds to the same point in time
	let in_range = |fixed_time| fixed_time >= *from && fixed_time <= *to;
	let mut indexes = HashMap::new();
	let first_archive = &archives.iter().next()
		.with_context(|| "No archives specified")?;
	let mut i: usize = 0;
	let records = get_records(first_archive);
	for x in records {
		if in_range(x.time) {
			indexes.insert(x.time, i);
			i += 1;
		}
	}
	let count = indexes.len();
	if count == 0 {
		bail!("Unable to finda any OHLC samples matching time constraints");
	}
	let delta_samples = archives.par_iter().map(|archive| {
		let mut sum = 0.0;
		let initial_value = 0.0;
		let mut samples = vec![initial_value; count];
		// Get close samples for the dynamic time range
		let records = get_records(archive);
		for record in records {
			if in_range(record.time) {
				if let Some(index) = indexes.get(&record.time) {
					let sample = record.close;
					samples[*index] = sample;
					sum += sample;
				}
			}
		}
		let mean = sum / (count as f64);
		let mut square_sum = 0.0;
		for x in &mut samples {
			if *x != initial_value {
				// Store pre-calculated x_i - x_mean values
				*x -= mean;
			} else {
				// Fill out gaps in the data with the mean value
				*x = mean;
			}
			square_sum += *x * *x;
		}
		let sqrt = square_sum.sqrt();
		(samples, sqrt)
	}).collect();
	Ok(delta_samples)
}

fn get_records(archive: &Arc<OhlcArchive>) -> &OhlcVec {
	archive.daily.get_adjusted_fallback()
}