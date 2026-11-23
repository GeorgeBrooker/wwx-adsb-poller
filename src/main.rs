use std::{
    collections::HashMap, sync::{Arc, RwLock}, time::{Duration, Instant}
};
use clap::{Args, Parser};
use crate::{
    poll_worker::{SharedState, spawn_poll_worker}, upstreams::{AdsbUpstream, UpstreamSource, adsb::ADSB_CACHE_KEY}
};
mod data_model;
mod helpers;
mod poll_worker;
mod upstreams;
mod local_server;

#[derive(Parser, Debug)]
struct Cli {
    #[command(flatten)]
    adsb: Option<AdsbArgs>,
    /// Remove any aircraft going to or from these airports; comma seperated ICAO codes
    #[arg(long, value_delimiter = ',')]
    removed_airports: Vec<String>,
    /// Refresh rate in seconds, accepts decimal values.
    #[arg(long, value_parser = clap::value_parser!(f64), default_value_t=5.0)]
    refresh_rate: f64,
}
#[derive(Args, Debug)]
#[group(requires_all = ["adsb_lat", "adsb_lon", "adsb_range"])]
struct AdsbArgs {
    /// Decimal lattitude of lookup point
    #[arg(long)]
    adsb_lat: f64,
    /// Decimal longditude of lookup point
    #[arg(long)]
    adsb_lon: f64,
    /// Range of ADSB lookup in nautical miles
    #[arg(long, value_parser = clap::value_parser!(u8).range(1..=250))]
    adsb_range: u8,
}
fn main() {
    let args = Cli::parse();
    const ICAO_CACHE_TTL: u64 = 4;
    const ICAO_MISS_TTL: u64 = 2;
    let mut removed_airports = args.removed_airports.clone(); // take this as a CLI argument as this is designed to be tightly coupled to one miz at a time. We want to do as much processing in rust as possible
    removed_airports.iter_mut().for_each(|airport| {*airport = airport.trim().to_ascii_uppercase()});
    let refresh_rate = args.refresh_rate.clone();
    let poll_timeout = 15; // seconds
    let idle_timout = refresh_rate * 4.0; // server will kill itself after 4 missing polls
    let local_server_address = "127.0.0.1:35535";

    // state will store the master table of collected api data
    let state = Arc::new(SharedState {
        icao_cache: RwLock::new(HashMap::new()),
        latest_data: RwLock::new(HashMap::new()),
        last_local_ping: RwLock::new(Instant::now()),
        api_calls: crate::upstreams::ApiCallCounts::new()
    });
    let mut upstream_maps = HashMap::new();

    // Build the api polling table
    if let Some(ref adsb) = args.adsb {
        let lat = adsb.adsb_lat;
        let lon = adsb.adsb_lon;
        let range = adsb.adsb_range;

        let url: String = format!("https://opendata.adsb.fi/api/v3/lat/{lat}/lon/{lon}/dist/{range}");
        upstream_maps.insert(
            ADSB_CACHE_KEY.to_owned(),
            Box::new(AdsbUpstream {
                url: url.to_owned(),
            }) as Box<dyn UpstreamSource>,
        );
    }

    // Start the API poller
    spawn_poll_worker(
        state.clone(),
        upstream_maps,
        Duration::from_secs_f64(refresh_rate * 60.0),
        Duration::from_secs(poll_timeout),
        Duration::from_secs_f64(idle_timout * 60.0),
        Duration::from_hours(ICAO_CACHE_TTL),
        Duration::from_hours(ICAO_MISS_TTL)
    );

    local_server::serve(state, local_server_address, &removed_airports);
}
