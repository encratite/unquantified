use std::{collections::HashMap, error::Error, sync::Arc};

use chrono::{DateTime, FixedOffset};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde::Serialize;

use common::*;

#[derive(Serialize)]
pub struct CorrelationData {
	pub correlation: Vec<Vec<f64>>,
	pub from: DateTime<FixedOffset>,
	pub to: DateTime<FixedOffset>
}

pub fn get_correlation_matrix(request_from: DateTime<FixedOffset>, request_to: DateTime<FixedOffset>, archives: Vec<Arc<OhlcArchive>>) -> Result<CorrelationData, Box<dyn Error>> {
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
		let mut sum = 0f64;
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
		correlation: matrix,
		from: from,
		to: to
	};
	Ok(output)
}

fn get_common_time_range(request_from: DateTime<FixedOffset>, request_to: DateTime<FixedOffset>, archives: &Vec<Arc<OhlcArchive>>)
	-> Result<(DateTime<FixedOffset>, DateTime<FixedOffset>), Box<dyn Error>> {
	let mut from = request_from;
	let mut to = request_to;
	for archive in archives {
			let add_tz = |x: &OhlcRecord| Some(get_fixed_time(x, &archive));
			let records = &archive.daily;
			let first = records
				.iter()
				.next()
				.and_then(add_tz);
			let last = records
				.iter()
				.last()
				.and_then(add_tz);
			match (first, last) {
				(Some(first_time), Some(last_time)) => {
					from = from.max(first_time);
					to = to.min(last_time);
				}
				_ => return Err("Missing records in archive".into())
			}
		}
	Ok((from, to))
}

fn get_fixed_time(x: &OhlcRecord, archive: &OhlcArchive) -> DateTime<FixedOffset> {
	 archive.add_tz(x.time).fixed_offset()
}

fn get_delta_samples(from: &DateTime<FixedOffset>, to: &DateTime<FixedOffset>, archives: &Vec<Arc<OhlcArchive>>) -> Result<Vec<(Vec<f64>, f64)>, Box<dyn Error>> {
	// Create an index map to make sure that each cell in the matrix corresponds to the same point in time
	let in_range = |fixed_time| fixed_time >= *from && fixed_time <= *to;
	let mut indexes = HashMap::new();
	let first_archive = &archives.iter().next()
		.ok_or_else(|| "No archives specified")?;
	let mut i: usize = 0;
	for x in &first_archive.daily {
		let fixed_time = get_fixed_time(&x, &first_archive);
		if in_range(fixed_time) {
			indexes.insert(fixed_time, i);
			i += 1;
		}
	}
	let count = indexes.len();
	let delta_samples = archives.par_iter().map(|archive| {
		let mut sum = 0f64;
		let initial_value = 0f64;
		let mut samples = vec![initial_value; count];
		// Get close samples for the dynamic time range
		for record in &archive.daily {
			let fixed_time = get_fixed_time(&record, &archive);
			if in_range(fixed_time) {
				if let Some(index) = indexes.get(&fixed_time) {
					let sample = record.close;
					samples[*index] = sample;
					sum += sample;
				}
			}
		}
		let mean = sum / (count as f64);
		let mut square_sum = 0f64;
		for x in &mut samples {
			if *x != initial_value {
				// Store pre-calculated x_i - x_mean values
				*x -= mean;
			}
			else {
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