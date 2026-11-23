use tiny_http::{Header, Response};
use std::sync::{Arc, atomic::Ordering};
use std::thread;
use std::net::UdpSocket;
use crate::poll_worker::SharedState;
use crate::upstreams::adsb::ADSB_CACHE_KEY;


fn build_airspace_request(state: &SharedState, removed_airports: &[String]) -> String {
    let data_pool = state.latest_data.read().unwrap();
    let adsb_data = data_pool.get(ADSB_CACHE_KEY);
    let icao_data = state.icao_cache.read().unwrap();

    let filtered_data = adsb_data.and_then(|adsb_data| {
        let filtered: Vec<_> = adsb_data
            .iter()
            .filter(|item| {
                let Some(aircraft) = item.as_aircraft() else {
                    return true
                };
                let Some(cache_entry) = icao_data.get(&aircraft.callsign) else {
                    return true;
                };
                let Some(route) = &cache_entry.route else {
                    return true
                };
                !removed_airports.contains(&route.origin) && !removed_airports.contains(&route.destination)
            })
            .collect();
        (!filtered.is_empty()).then_some(filtered)
    });
    
    serde_json::to_string(&serde_json::json!({
        "type": "airspace",
        "status": "ok",
        "data": filtered_data
    }))
    .unwrap_or_else(|_| r#"{"type":"airspace","status":"ok", "data": "None"}"#.to_owned())
}

fn build_heartbeat_json(state: &SharedState) -> String {
    serde_json::to_string(&serde_json::json!({
        "type": "heartbeat",
        "status": "ok",
        "api_calls": {
            "adsb": state.api_calls.adsb.load(Ordering::Relaxed),
            "icao": state.api_calls.icao.load(Ordering::Relaxed),
        },
    }))
    .unwrap_or_else(|_| r#"{"type":"heartbeat","status":"ok"}"#.to_owned())
}

pub fn serve(state: Arc<SharedState>, address: &str, removed_airports: &[String]) {
    let udp_state = state.clone();
    let udp_addr = address.to_owned(); //UDP and TCP servers can share the same address
    let removed_airports: Arc<[String]> = removed_airports.into();
    thread::spawn(move || {
        let socket = UdpSocket::bind(&udp_addr).expect(&*format!("Failed to create server on {udp_addr}..."));
        let mut buff = [0u8; 32];
        loop {
            if let Ok((size, src_addr)) = socket.recv_from(&mut buff) {
                *udp_state.last_local_ping.write().unwrap() = std::time::Instant::now();
                let command = std::str::from_utf8(&buff[..size])
                    .unwrap_or("")
                    .trim();
                
                match command {
                    "HEARTBEAT" => {
                        let reply = build_heartbeat_json(&udp_state);
                        let _ = socket.send_to(reply.as_bytes(), src_addr);
                    }
                    "AIRSPACE" => {
                        let reply = build_airspace_request(&udp_state, &removed_airports);
                        let _ = socket.send_to(reply.as_bytes(), src_addr);
                    }
                    _ => {
                        let _ = socket.send_to("Unknown request. Valid requests are \"HEARTBEAT\" and \"AIRSPACE\"".as_bytes(), src_addr);
                    }
                }
            }
        }
    });
    // for browser debugging
    let server = tiny_http::Server::http(address).expect("failed to start local server...");
    for request in server.incoming_requests() {
        *state.last_local_ping.write().unwrap() = std::time::Instant::now();

        let latest_data = state.latest_data.read().unwrap();
        let icao_cache = state.icao_cache.read().unwrap();
        let json = serde_json::to_string(&serde_json::json!({
            "data": &*latest_data,
            "icao": &*icao_cache,
            "api_calls": {
                "adsb": state.api_calls.adsb.load(std::sync::atomic::Ordering::Relaxed),
                "icao": state.api_calls.icao.load(std::sync::atomic::Ordering::Relaxed),
            },
        }))
        .unwrap_or_else(|_| "{}".to_owned());
        let mut response = Response::from_string(json);
        response.add_header(
            Header::from_bytes(
                &b"Content-Type"[..],
                &b"application/json"[..],
            )
            .unwrap(),
        );

        let _ = request.respond(response);
    }
}