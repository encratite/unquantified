use std::collections::HashMap;
use std::iter;
use anyhow::{Result, bail};
use unq_common::backtest::{Backtest, PositionSide};
use unq_common::strategy::{Strategy, StrategyParameters};

pub struct BuyAndHoldStrategy<'a> {
	remaining_symbols: HashMap<String, u32>,
	backtest: &'a mut Backtest<'a>
}

/*
Buys and holds one or multiple long positions of the specified symbols until the end of the backtest.
If there is no price data available due to missing price data, the strategy will keep on trying to purchase them.
By default, one contract of each asset is held, but the number can be customized like this:
- symbols: [GC, NG, CL]
- parameters: {contracts: [1, 2, 2]}
This would change the number of contracts for NG and CL to 2 each.
*/
impl<'a> BuyAndHoldStrategy<'a> {
	fn new(symbols: Vec<String>, contracts: Vec<u32>, backtest: &'a mut Backtest<'a>) -> Result<BuyAndHoldStrategy<'a>> {
		if symbols.is_empty() {
			bail!("Need at least one symbol");
		}
		let mut remaining_symbols: HashMap<String, u32> = HashMap::new();
		let n = symbols.len();
		for i in 0..n {
			let Some(symbol) = symbols.get(i) else {
				bail!("Unable to retrieve symbol");
			};
			let Some(count) = contracts.get(i) else {
				bail!("Missing contract count for symbol");
			};
			remaining_symbols.insert(symbol.clone(), *count);
		}
		let strategy = BuyAndHoldStrategy {
			remaining_symbols,
			backtest
		};
		Ok(strategy)
	}

	pub fn from_parameters(symbols: Vec<String>, parameters: &StrategyParameters, backtest: &'a mut Backtest<'a>) -> Result<BuyAndHoldStrategy<'a>> {
		let contracts: Vec<u32> = match parameters.get_values("contracts") {
			Some(count) => count
				.iter()
				.map(|x| *x as u32)
				.collect(),
			None => iter::repeat(1)
				.take(symbols.len())
				.collect()
		};
		Self::new(symbols, contracts, backtest)
	}
}

impl<'a> Strategy for BuyAndHoldStrategy<'a> {
	fn next(&mut self) -> Result<()> {
		// Try to create all positions in each iteration, just in case we're dealing with illiquid assets and intraday data
		for (symbol, contract_count) in self.remaining_symbols.clone() {
			let result = self.backtest.open_position(&symbol, contract_count, PositionSide::Long);
			if result.is_ok() {
				self.remaining_symbols.remove(&symbol);
			}
		}
		Ok(())
	}
}