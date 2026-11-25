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
        
        // Cap to avoid exceeding MTU. This should give us plenty of headroom (70ish kb) and more planes would crash DCS anyway
        let capped: Vec<_> = filtered.into_iter().take(300).collect();
        (!capped.is_empty()).then_some(capped)
    });
    
    let mut aircraft = String::from("{");

    if let Some(filtered) = filtered_data {
        for (index, item) in filtered.iter().take(20).enumerate() {
            if let Some(aircraft_data) = item.as_aircraft() {
                if index > 0 {
                    aircraft.push(',');
                }
                aircraft.push_str(&build_lua_aircraft_table(
                    &aircraft_data.callsign,
                    &aircraft_data.hex,
                    &aircraft_data.category,
                    &aircraft_data.registration,
                    aircraft_data.lat,
                    aircraft_data.lon,
                    aircraft_data.alt_baro_m,
                    aircraft_data.gs_ms,
                    aircraft_data.track,
                ));
            }
        }
    }

    aircraft.push('}');

    format!(
        "{{type={},status={},data={aircraft}}}",
        lua_string("airspace"),
        lua_string("ok"),
    )
}

fn build_heartbeat_json(state: &SharedState) -> String {
    format!(
        "{{type={},status={},api_calls={{adsb={},icao={}}}}}",
        lua_string("heartbeat"),
        lua_string("ok"),
        state.api_calls.adsb.load(Ordering::Relaxed),
        state.api_calls.icao.load(Ordering::Relaxed),
    )
}

fn lua_string(value: &str) -> String {
    let escaped = value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t");
    format!("\"{}\"", escaped)
}

fn build_lua_aircraft_table(
    callsign: &str,
    hex: &str,
    category: &str,
    registration: &str,
    lat: f64,
    lon: f64,
    alt_baro_m: f64,
    gs_ms: f64,
    track: f64,
) -> String {
    format!(
        "{{hex={},callsign={},lat={lat},lon={lon},category={},registration={},alt_baro_m={alt_baro_m},gs_ms={gs_ms},track={track}}}",
        lua_string(hex),
        lua_string(callsign),
        lua_string(category),
        lua_string(registration),
    )
}

pub fn serve(state: Arc<SharedState>, address: &str, removed_airports: &[String]) {
    let udp_state = state.clone();
    let udp_addr = address.to_owned(); //UDP and TCP servers can share the same address
    let removed_airports: Arc<[String]> = removed_airports.into();
    let udp_removed_airports = Arc::clone(&removed_airports);
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
                        let reply = build_airspace_request(&udp_state, &udp_removed_airports);
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
        let command = request.url();
        match command {
            "/heartbeat" => {
                let mut reply = Response::from_string(build_heartbeat_json(&state));
                reply.add_header(
                    Header::from_bytes(
                        &b"Content-Type"[..],
                        &b"application/json"[..],
                    )
                    .unwrap(),
                );
                let _ = request.respond(reply);
            }
            "/airspace" => {
                let mut reply = Response::from_string(build_airspace_request(&state, &removed_airports));
                reply.add_header(
                    Header::from_bytes(
                        &b"Content-Type"[..],
                        &b"application/json"[..],
                    )
                    .unwrap(),
                );
                let _ = request.respond(reply);
            }
            "/" => {
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
                let mut reply = Response::from_string(json);
                reply.add_header(
                    Header::from_bytes(
                        &b"Content-Type"[..],
                        &b"application/json"[..],
                    )
                    .unwrap(),
                );

                let _ = request.respond(reply);
            }
            _ => {
                let mut reply = Response::from_string("Unknown request. Valid requests are \"/heartbear\", \"airspace\" and \"/\"");
                reply.add_header(
                    Header::from_bytes(
                        &b"Content-Type"[..],
                        &b"application/json"[..],
                    )
                    .unwrap(),
                );
                let _ = request.respond(reply);
            }
        }
    }
}