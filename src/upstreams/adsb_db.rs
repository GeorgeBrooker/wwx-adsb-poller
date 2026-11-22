use std::time::Instant;
use super::UpstreamSource;
use serde::{Deserialize, Serialize};
use crate::data_model::ProcessedApiData;

pub const ICAO_CACHE_KEY: &str = "icao";

#[derive(Debug, Deserialize)]
pub struct AircraftAndCallsignFeed {
    pub aircraft: Option<IcaoAircraft>,
    pub flightroute: Option<FlightRoute>
}

#[derive(Debug, Deserialize)]
pub struct FlightRoute {
    pub callsign: Option<String>,
    pub origin: Option<FlightOrigin>,
    pub destination: Option<FlightDestination>
}
#[derive(Debug, Deserialize)]
pub struct IcaoAircraft {
    pub registration: Option<String>,
}
#[derive(Debug, Deserialize)]
pub struct FlightOrigin{
    icao_code: Option<String>
}
#[derive(Debug, Deserialize)]
pub struct FlightDestination {
    icao_code: Option<String>
}
#[derive(Debug, Serialize, Clone)]
pub struct ProcessedIcaoLookup {
    pub callsign: String,
    pub registration: String,
    pub origin: String,
    pub destination: String,
    #[serde(skip)] // Only for internal cache management
    pub lookup_time: Instant
}
impl From<AircraftAndCallsignFeed> for Option<ProcessedIcaoLookup> {
    fn from(raw_ac: AircraftAndCallsignFeed) -> Self {
        let route = raw_ac.flightroute?;
        let icao_aircraft = raw_ac.aircraft?;
        let callsign = route.callsign?;
        let origin = route.origin?;
        let destination = route.destination?;

        Some(ProcessedIcaoLookup {
            callsign: callsign,
            registration: icao_aircraft.registration?,
            origin: origin.icao_code?,
            destination: destination.icao_code?,
            lookup_time: Instant::now()
        })
    }
}

pub struct IcaoUpstream {
    pub url: String,
}

impl UpstreamSource for IcaoUpstream {
    fn poll(&self, agent: &ureq::Agent) -> Result<Vec<ProcessedApiData>, String> {
        let feed: AircraftAndCallsignFeed = agent
            .get(&self.url)
            .call()
            .map_err(|e| e.to_string())?
            .into_json()
            .map_err(|e| e.to_string())?;

        let icao_objet = Option::<ProcessedIcaoLookup>::from(feed);

        // wrap into vector for API compatibility
        Ok(icao_objet
            .into_iter()
            .map(ProcessedApiData::Icao)
            .collect())
    }
}