mod server;
mod datetime;

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
	let data_directory = get_key("data_directory");
	let address: SocketAddr = address_string.parse()
		.expect("Unable to parse server address");
	server::run(address, data_directory).await;
	Ok(())
}