mod buy_and_hold;

use std::sync::Mutex;
use anyhow::{Result, anyhow};
use unq_common::backtest::Backtest;
use unq_common::strategy::{Strategy, StrategyParameters};
use crate::buy_and_hold::BuyAndHoldStrategy;

pub fn get_strategy<'a>(name: &String, symbols: Vec<String>, parameters: &StrategyParameters, backtest: &'a Mutex<Backtest<'a>>) -> Result<Box<dyn Strategy + 'a>> {
	match name.as_str() {
		"buy and hold" => {
			let strategy = BuyAndHoldStrategy::from_parameters(symbols, parameters, backtest)?;
			Ok(Box::new(strategy))
		},
		_ => Err(anyhow!("No such strategy"))
	}
}