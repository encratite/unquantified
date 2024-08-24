use std::collections::HashMap;
use anyhow::{Result, bail};
use unq_common::backtest::{Backtest, BacktestResult, PositionSide};
use unq_common::strategy::{Strategy, StrategyParameters};

struct BuyAndHoldStrategy {
}

impl BuyAndHoldStrategy {
	fn get_contract_counts<'a>(symbols: &'a Vec<String>, parameters: &StrategyParameters) -> Result<HashMap<&'a String, u32>> {
		let contracts = parameters.get_values("contracts");
		let mut contract_counts = HashMap::new();
		let length = symbols.len();
		for i in 0..length {
			let symbol = &symbols[i];
			let contract_count = match &contracts {
				Some(values) => {
					let Some(count) = values.get(i) else {
						bail!("Missing contract count for symbol {symbol}");
					};
					*count as u32
				},
				None => 1u32
			};
			contract_counts.insert(symbol, contract_count);
		}
		Ok(contract_counts)
	}
}

impl Strategy for BuyAndHoldStrategy {
	fn run(&mut self, symbols: Vec<String>, parameters: StrategyParameters, backtest: &mut Backtest) -> Result<BacktestResult> {
		if symbols.is_empty() {
			bail!("Need at least one symbol");
		}
		let mut contract_counts = Self::get_contract_counts(&symbols, &parameters)?;
		let mut done = false;
		while !done {
			// Try to create all positions in each iteration, just in case we're dealing with illiquid assets and intraday data
			for (symbol, contract_count) in contract_counts.clone() {
				let result = backtest.open_by_root(symbol, contract_count, PositionSide::Long);
				if result.is_ok() {
					contract_counts.remove(symbol);
				}
			}
			done = backtest.next()?;
		}
		let result = backtest.get_result();
		Ok(result)
	}
}