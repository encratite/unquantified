mod server;
mod datetime;
mod correlation;
mod backtest;

use std::error::Error;
use std::net::SocketAddr;
use common::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>>{
	let config = get_config("server.ini")?;
	let get_key = |key| {
		config.get("server", key)
			.expect(&*format!("Failed to find key \"{}\" in configuration file", key))
	};
	let address_string = get_key("address");
	let ticker_directory = get_key("ticker_directory");
	let assets_path = get_key("assets");
	let address: SocketAddr = address_string.parse()
		.expect("Unable to parse server address");
	server::run(address, ticker_directory, assets_path).await;
	Ok(())
}