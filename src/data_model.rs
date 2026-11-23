use serde::{Serialize};
use crate::upstreams::{
    adsb::ProcessedAircraft,
    adsb_db::ProcessedIcaoLookup
};

#[derive(Serialize, Clone, Debug)]
#[serde(untagged)]
pub enum ProcessedApiData {
    Aircraft(ProcessedAircraft),
    Icao(ProcessedIcaoLookup),
    // Vessel - Support extention later...
}
impl ProcessedApiData {
    pub fn as_aircraft(&self) -> Option<&ProcessedAircraft> {
        match self {
            ProcessedApiData::Aircraft(aircraft) => Some(aircraft),
            _ => None
        }
    }
}