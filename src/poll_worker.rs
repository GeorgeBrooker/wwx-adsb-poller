use crate::{
    data_model::ProcessedApiData,
    upstreams::UpstreamSource
};
use std::{
    collections::HashMap, sync::{Arc, RwLock}, thread::{self, JoinHandle}, time::{Duration, Instant},
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
    idle_timeout: Duration // time from last DCS request to activate dead man switch
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

            for (name, source) in &upstreams {
                match source.poll(&agent) {
                    Ok(items) => {
                        let mut latest_data = state.latest_data.write().unwrap();
                        latest_data.insert(name.clone(), items);
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