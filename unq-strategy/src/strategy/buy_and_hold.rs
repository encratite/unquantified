use std::cell::RefCell;
use std::collections::HashMap;
use anyhow::{Result, bail};
use unq_common::backtest::{Backtest, PositionSide};
use unq_common::strategy::{Strategy, StrategyParameters};
use crate::{get_symbol_contracts, SymbolContracts};

pub struct BuyAndHoldStrategy {
	remaining_symbols: HashMap<String, u32>,
	side: PositionSide,
	backtest: RefCell<Backtest>
}

/*
Buys and holds one or multiple long positions of the specified symbols until the end of the backtest.
If there is no price data available due to missing price data, the strategy will keep on trying to purchase them.
Parameters:
- contracts: array of integers that determines the number of contracts for each symbol
- short: boolean value that that makes all positions short rather than long
By default, all positions are long and one contract of each asset is held, but the number can be customized like this:
- symbols: [GC, NG, CL]
- parameters: {contracts: [1, 2, 2]}
This would change the number of contracts for NG and CL to 2 each.
*/
impl BuyAndHoldStrategy {
	pub const ID: &'static str = "buy and hold";

	fn new(symbol_contracts: SymbolContracts, side: PositionSide, backtest: RefCell<Backtest>) -> Result<Self> {
		if symbol_contracts.is_empty() {
			bail!("Need at least one symbol");
		}
		let mut remaining_symbols: HashMap<String, u32> = HashMap::new();
		for (symbol, contracts) in symbol_contracts {
			remaining_symbols.insert(symbol.clone(), contracts);
		}
		let strategy = Self {
			remaining_symbols,
			side,
			backtest
		};
		Ok(strategy)
	}

	pub fn from_parameters(symbols: &Vec<String>, parameters: &StrategyParameters, backtest: RefCell<Backtest>) -> Result<Self> {
		let symbol_contracts = get_symbol_contracts(&symbols, parameters)?;
		let side = match parameters.get_bool("short")? {
			Some(value) => {
				if value {
					PositionSide::Short
				} else {
					PositionSide::Long
				}
			},
			None => PositionSide::Long
		};
		Self::new(symbol_contracts, side, backtest)
	}
}

impl Strategy for BuyAndHoldStrategy {
	fn next(&mut self) -> Result<()> {
		let mut backtest = self.backtest.borrow_mut();
		// Try to create all positions in each iteration, just in case we're dealing with illiquid assets and intraday data
		for (symbol, contract_count) in self.remaining_symbols.clone() {
			if !backtest.is_available(&symbol)? {
				// This symbol isn't available on the exchange yet, skip it
				continue;
			}
			let result = backtest.open_position(&symbol, contract_count, self.side.clone());
			if result.is_ok() {
				self.remaining_symbols.remove(&symbol);
			}
		}
		Ok(())
	}
}