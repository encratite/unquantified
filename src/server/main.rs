use std::error::Error;
use std::net::SocketAddr;
use configparser::ini::Ini;
use futures_util::{FutureExt, SinkExt, StreamExt};
use warp::Filter;
use warp::filters::ws::Message;
use tokio::time::{sleep, Duration};
use chrono::offset::Local;
use common::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>>{
	let config: Ini;
	match get_config("server.ini") {
		Ok(c) => {
			config = c;
		}
		Err(error) => {
			eprintln!("{error}");
			return Err(error.into());
		}
	}
	let get_key = |key| {
		config.get("server", key)
			.expect(&*format!("Failed to find key \"{}\" in configuration file", key))
	};
	let _data_directory = get_key("data_directory");
	let address_string = get_key("address");
	let address: SocketAddr = address_string.parse()
		.expect("Unable to parse server address");
	run_server(address).await;
	Ok(())
}

async fn run_server(address: SocketAddr) {
	println!("Running server on {}", address);
	let index = warp::path::end()
		.and(warp::fs::file("web/index.html"));
	let rpc = warp::path("rpc")
		.and(warp::ws())
		.map(|ws: warp::ws::Ws| {
			ws.on_upgrade(handle_client)
		});
	let routes = index
		.or(rpc);
	warp::serve(routes)
		.run(address)
		.await;
}

async fn handle_client(socket: warp::ws::WebSocket) {
	let (mut sender, mut _receiver) = socket.split();
	loop {
		let message = Message::text(Local::now().to_string());
		match sender.send(message).await {
			Ok(_) => {},
			Err(error) => {
				eprintln!("Client disconnected: {}", error.to_string());
				return;
			}
		}
		sleep(Duration::from_secs(1)).await;
	}
}