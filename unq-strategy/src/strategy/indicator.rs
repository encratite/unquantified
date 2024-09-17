use std::cell::RefCell;
use anyhow::Result;
use unq_common::backtest::Backtest;
use unq_common::strategy::{Strategy, StrategyParameters};
use crate::technical::Indicator;

pub struct IndicatorStrategy<'a> {
	symbols: Vec<String>,
	indicator: Box<dyn Indicator>,
	backtest: &'a RefCell<Backtest<'a>>
}

impl<'a> IndicatorStrategy<'a> {
	pub fn new(symbols: Vec<String>, indicator: Box<dyn Indicator>, backtest: &'a RefCell<Backtest<'a>>) -> Result<Self> {
		let strategy = Self {
			symbols,
			indicator,
			backtest
		};
		Ok(strategy)
	}

	pub fn from_parameters(symbols: Vec<String>, parameters: &StrategyParameters, backtest: &'a RefCell<Backtest<'a>>) -> Result<Self> {
		todo!()
	}
}

impl<'a> Strategy for IndicatorStrategy<'a> {
	fn next(&mut self) -> Result<()> {
		let backtest = self.backtest.borrow_mut();
		todo!()
	}
}