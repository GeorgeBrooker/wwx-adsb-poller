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
    Fused(FusedAircraft)
    // Vessel - Support extention later...
}
impl ProcessedApiData {
    pub fn as_aircraft(&self) -> Option<&ProcessedAircraft> {
        match self {
            ProcessedApiData::Aircraft(aircraft) => Some(aircraft),
            _ => None
        }
    }
    pub fn as_icao(&self) -> Option<&ProcessedIcaoLookup> {
        match self {
            ProcessedApiData::Icao(icao) => Some(icao),
            _ => None
        }
    }
}

#[derive(Serialize, Clone, Debug)]
pub struct FusedAircraft {
    pub hex: String,
    pub callsign: String, // Callsign
    pub lat: f64,
    pub lon: f64,
    pub category: String,
    pub registration: String,
    pub alt_baro_m: f64, // DCS scripting api uses fp metric values 
    pub gs_ms: f64, // DCS scripting api uses metric 
    pub track: f64,
    pub origin: String, // ICAO data
    pub destination: String // ICAO data
}