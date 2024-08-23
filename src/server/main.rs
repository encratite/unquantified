mod server;
mod datetime;
mod correlation;

use std::net::SocketAddr;
use anyhow::{anyhow, Context, Result};
use common::{backtest::BacktestConfiguration, get_ini};

#[tokio::main]
async fn main() -> Result<()> {
	let config = get_ini("server.ini")?;
	let get_string = |section, key| -> Result<String> {
		config.get(section, key)
			.with_context(|| anyhow!("Failed to find key \"{key}\" in section \"{section}\" in configuration file"))
	};
	let parse_error = |key: &str, section: &str| anyhow!("Failed to parse value for key \"{key}\" in section \"{section}\" in configuration file");
	let get_f64 = |section, key| -> Result<f64> {
		let value = get_string(section, key)?;
		value.parse()
			.with_context(|| parse_error(key, section))
	};
	let get_u8 = |section, key| -> Result<u8> {
		let value = get_string(section, key)?;
		value.parse()
			.with_context(|| parse_error(key, section))
	};
	let get_bool = |section, key| -> Result<bool> {
		let value = get_string(section, key)?;
		value.parse()
			.with_context(|| parse_error(key, section))
	};
	let server_section = "server";
	let address_string = get_string(server_section, "address")?;
	let address: SocketAddr = address_string.parse()
		.with_context(|| "Unable to parse server address")?;
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
