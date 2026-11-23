pub mod adsb;
pub mod adsb_db;
pub use adsb::AdsbUpstream;

use crate::data_model::ProcessedApiData;
use std::sync::atomic::{AtomicU64, Ordering};

pub const ADSB_ENDPOINT: &str = "adsb";
pub const ICAO_ENDPOINT: &str = "icao";

pub struct ApiCallCounts {
    pub adsb: AtomicU64,
    pub icao: AtomicU64,
}

impl ApiCallCounts {
    pub fn new() -> Self {
        Self {
            adsb: AtomicU64::new(0),
            icao: AtomicU64::new(0),
        }
    }

    pub fn increment(&self, endpoint: &str) {
        match endpoint {
            ADSB_ENDPOINT => {
                self.adsb.fetch_add(1, Ordering::Relaxed);
            }
            ICAO_ENDPOINT => {
                self.icao.fetch_add(1, Ordering::Relaxed);
            }
            _ => eprintln!("[Poller] Unknown API endpoint: {endpoint}"),
        }
    }
}

pub trait UpstreamSource: Send + Sync {
    fn poll(
        &self,
        agent: &ureq::Agent,
        api_calls: &ApiCallCounts,
        endpoint: &str,
    ) -> Result<Vec<ProcessedApiData>, String>;
}
