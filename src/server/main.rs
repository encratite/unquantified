mod server;
mod datetime;
mod correlation;
mod backtest;
mod manager;

use std::error::Error;
use std::net::SocketAddr;
use common::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
	let config = get_config("server.ini")?;
	let get_key = |section, key| {
		config.get("server", key)
			.expect(&*format!("Failed to find key \"{}\" in configuration file", key))
	};
	let server_section = "server";
	let address_string = get_key(server_section, "address");
	let ticker_directory = get_key(server_section, "ticker_directory");
	let assets_path = get_key(server_section, "assets");
	let backtest_section = "backtest";
	let initial_margin_string = get_key(backtest_section, "initial_margin_ratio");
	let address: SocketAddr = address_string.parse()
		.expect("Unable to parse server address");
	let initial_margin: f64 = initial_margin_string.parse()
		.expect("Unable to parse initial margion ratio");
	server::run(address, ticker_directory, assets_path).await;
	Ok(())
}
