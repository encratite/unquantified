mod server;
mod datetime;
mod correlation;
mod backtest;
mod manager;

use std::error::Error;
use std::net::SocketAddr;
use backtest::BacktestConfiguration;
use common::*;

#[tokio::main]
async fn main() -> Result<(), ErrorBox> {
	let config = get_config("server.ini")?;
	let get_string = |section, key| -> Result<String, ErrorBox> {
		config.get(section, key)
			.ok_or_else(|| format!("Failed to find key \"{}\" in section \"{}\" in configuration file", key, section).into())
	};
	let parse_error = |key: &str, section: &str| format!("Failed to parse value for key \"{}\" in section \"{}\" in configuration file", key, section);
	let get_f64 = |section, key| -> Result<f64, ErrorBox> {
		let value = get_string(section, key)?;
		value.parse()
			.map_err(|_| parse_error(key, section).into())
	};
	let get_u8 = |section, key| -> Result<u8, ErrorBox> {
		let value = get_string(section, key)?;
		value.parse()
		.map_err(|_| parse_error(key, section).into())
	};
	let get_bool = |section, key| -> Result<bool, ErrorBox> {
		let value = get_string(section, key)?;
		value.parse()
		.map_err(|_| parse_error(key, section).into())
	};
	let server_section = "server";
	let address_string = get_string(server_section, "address")?;
	let address: SocketAddr = address_string.parse()
		.map_err(|_| "Unable to parse server address")?;
	let ticker_directory = get_string(server_section, "ticker_directory")?;
	let assets_path = get_string(server_section, "assets")?;
	let backtest_section = "backtest";
	let starting_cash = get_f64(backtest_section, "starting_cash")?;
	let forex_order_fee = get_f64(backtest_section, "forex_order_fee")?;
	let forex_spread = get_f64(backtest_section, "forex_spread")?;
	let futures_spread_ticks = get_u8(backtest_section, "futures_spread_ticks")?;
	let initial_margin_ratio = get_f64(backtest_section, "initial_margin_ratio")?;
	let overnight_margin_ratio = get_f64(backtest_section, "overnight_margin_ratio")?;
	let automatic_rollover = get_bool(backtest_section, "automatic_rollover")?;
	let backtest_configuration = BacktestConfiguration {
		starting_cash,
		forex_order_fee,
		forex_spread,
		futures_spread_ticks,
		initial_margin_ratio,
		overnight_margin_ratio,
		automatic_rollover
	};
	server::run(address, ticker_directory, assets_path, backtest_configuration).await;
	Ok(())
}
