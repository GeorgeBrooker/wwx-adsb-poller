use crate::{
    data_model::ProcessedApiData::{self, Icao}, helpers::get_icao_for_adsb, upstreams::{UpstreamSource, adsb::ADSB_CACHE_KEY, adsb_db::ICAO_CACHE_KEY}
};
use std::{
    collections::HashMap, ops::ControlFlow::Continue, sync::{Arc, RwLock}, thread::{self, JoinHandle}, time::{Duration, Instant},
};
pub struct SharedState {
    pub latest_data: RwLock<HashMap<String, Vec<ProcessedApiData>>>,
    pub last_local_ping: RwLock<Instant>,
}

pub fn spawn_poll_worker(
    state: Arc<SharedState>,
    upstreams: HashMap<String, Box<dyn UpstreamSource>>,
    poll_interval: Duration,
    poll_timout: Duration,
    idle_timeout: Duration, // time from last DCS request to activate dead man switch
    icao_cache_period: Duration,
) -> JoinHandle<()> {
    thread::spawn(move || {
        //Share one agent across all upstreams
        let agent = ureq::AgentBuilder::new()
        .timeout(poll_timout)
        .user_agent("WWX-Telemetry-Sidecar/1.0")
        .build();

        loop {
            // Dead mans switch
            if state.last_local_ping.read().unwrap().elapsed() > idle_timeout {
                std::process::exit(0);
            }
            // ICAO cache validity check
            let mut latest_data = state.latest_data.write().unwrap();
            let icao_cache = latest_data
                .entry(ICAO_CACHE_KEY.to_owned())
                .or_default();
            icao_cache.retain(|entry|{
                match entry.as_icao() {
                    Some(icao) => icao.lookup_time.elapsed() < icao_cache_period,
                    None => false,
                }
            });
            drop(latest_data); // Release lock

            // Poll APIs
            for (name, source) in &upstreams {
                match source.poll(&agent) {
                    Ok(items) => {
                        if name == ADSB_CACHE_KEY { // sensor fusion with ICAO lookup
                            let icao_additions = get_icao_for_adsb(
                                &items, 
                                state.clone(), 
                                &agent
                            ).unwrap_or_else(|error| {
                                eprintln!("[Poller] ICAO lookup failed: '{error}'");
                                Vec::new()
                            });

                            let mut latest_data = state.latest_data.write().unwrap();
                            latest_data // Icao Cache update
                                .entry(ICAO_CACHE_KEY.to_owned())
                                .or_default()
                                .extend(icao_additions); // Polled API update
                            latest_data.insert(name.clone(), items);
                        } else {
                            let mut latest_data = state.latest_data.write().unwrap();
                            latest_data.insert(name.clone(), items);
                        }
                    }
                    Err(e) => {
                        eprintln!("[Poller] Upstream '{name}' failed: '{e}'");
                    }
                }
            }
            thread::sleep(poll_interval);
        }
    })
}