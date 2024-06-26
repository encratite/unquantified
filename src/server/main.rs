use std::{collections::HashMap, error::Error};
use std::net::SocketAddr;
use configparser::ini::Ini;
use futures_util::{FutureExt, SinkExt, StreamExt};
use serde_json::Value;
use warp::Filter;
use warp::filters::ws::Message;
use tokio::time::{sleep, Duration};
use serde::{Deserialize, Serialize};
use common::*;

#[derive(Deserialize)]
struct RpcRequest {
	name: String,
	arguments: Vec<Value>
}

#[derive(Serialize)]
struct RpcResponse {
	result: Value,
	error: Option<String>
}

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
            ws.on_upgrade(|socket| {
                async move {
                    if let Err(e) = handle_client(socket).await {
                        eprintln!("Error: {}", e);
                    }
                }
            })
		});
	let routes = index
		.or(rpc);
	warp::serve(routes)
		.run(address)
		.await;
}

async fn handle_client(socket: warp::ws::WebSocket) -> Result<(), Box<dyn Error>> {
	let requests = HashMap::from([
		("getHistory", get_history)
	]);
	let (mut sender, mut receiver) = socket.split();
	while let Some(message) = receiver.next().await {
		let json_message = message?;
		let json = json_message.to_str().map_err(|_| "Unexpected message type")?;
		let request: RpcRequest = serde_json::from_str(json)?;
	}
	Ok(())
}

fn get_history(arguments: Vec<Value>) -> Result<Value, String> {
	Err("Not implemented".to_string())
}