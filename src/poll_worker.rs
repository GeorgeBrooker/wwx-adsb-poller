use crate::{
    data_model::ProcessedApiData, helpers::get_icao_for_adsb, upstreams::{UpstreamSource, adsb::ADSB_CACHE_KEY, adsb_db::IcaoCacheEntry}
};
use std::{
    collections::HashMap, sync::{Arc, RwLock}, thread::{self, JoinHandle}, time::{Duration, Instant},
};
pub struct SharedState {
    pub icao_cache: RwLock<HashMap<String, IcaoCacheEntry>>,
    pub latest_data: RwLock<HashMap<String, Vec<ProcessedApiData>>>,
    pub last_local_ping: RwLock<Instant>,
    pub api_calls: crate::upstreams::ApiCallCounts,
}

pub fn spawn_poll_worker(
    state: Arc<SharedState>,
    upstreams: HashMap<String, Box<dyn UpstreamSource>>,
    poll_interval: Duration,
    poll_timout: Duration,
    idle_timeout: Duration, // time from last DCS request to activate dead man switch
    icao_cache_period: Duration,
    icao_miss_period: Duration,
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
            let mut icao_cache = state.icao_cache.write().unwrap();
            icao_cache.retain(|_, entry|{
                let ttl = if entry.route.is_some() { icao_cache_period } else { icao_miss_period };
                entry.lookup_time.elapsed() < ttl
            });
            drop(icao_cache); // Release lock

            // Poll APIs
            for (name, source) in &upstreams {
                match source.poll(&agent, &state.api_calls, name) {
                    Ok(items) => {
                        if name == ADSB_CACHE_KEY { // sensor fusion with ICAO lookup
                            let icao_additions = get_icao_for_adsb(
                                &items, 
                                state.clone(), 
                                &agent
                            ).unwrap_or_else(|error| {
                                eprintln!("[Poller] ICAO lookup failed: '{error}'");
                                HashMap::new()
                            });

                            state.icao_cache.write().unwrap().extend(icao_additions);
                            state.latest_data.write().unwrap().insert(name.clone(), items);
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