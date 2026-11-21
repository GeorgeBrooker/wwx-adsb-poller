use serde::{Serialize};
use crate::upstreams::adsb::ProcessedAircraft;

#[derive(Serialize, Clone, Debug)]
pub enum ProcessedApiData {
    Aircraft(ProcessedAircraft)
    // Vessel - Support extention later...
    // ICAO
}
// Json from the API is mapped onto this struct
