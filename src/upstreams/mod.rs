pub mod adsb;
pub use adsb::AdsbUpstream;

use crate::data_model::ProcessedApiData;

pub trait UpstreamSource: Send + Sync {
    fn poll(&self, agent: &ureq::Agent) -> Result<Vec<ProcessedApiData>, String>;
}
