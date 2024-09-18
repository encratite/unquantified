mod technical;
mod strategy {
	pub mod buy_and_hold;
	pub mod indicator;
}

use std::cell::RefCell;
use anyhow::{Result, bail};
use unq_common::backtest::Backtest;
use unq_common::strategy::{Strategy, StrategyParameters};
use crate::strategy::buy_and_hold::BuyAndHoldStrategy;
use crate::strategy::indicator::IndicatorStrategy;

pub fn get_strategy<'a>(name: &String, symbols: Vec<String>, parameters: &StrategyParameters, backtest: &'a RefCell<Backtest<'a>>) -> Result<Box<dyn Strategy + 'a>> {
	match name.as_str() {
		"buy and hold" => {
			let strategy = BuyAndHoldStrategy::from_parameters(symbols, parameters, backtest)?;
			Ok(Box::new(strategy))
		},
		"indicator" => {
			let strategy = IndicatorStrategy::from_parameters(symbols, parameters, backtest)?;
			Ok(Box::new(strategy))
		},
		_ => bail!("No such strategy")
	}
}