use std::time::Instant;
use super::UpstreamSource;
use serde::{Deserialize, Serialize};
use crate::data_model::ProcessedApiData;

#[derive(Debug, Deserialize)]
pub struct AircraftAndCallsign {
    pub flightroute: Option<FlightRoute>
}
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum AdsbDbResponseValue {
    Aircraft(AircraftAndCallsign),
    Unknown(String),
}

#[derive(Debug, Deserialize)]
pub struct AdsbDbResponse {
    pub response: Option<AdsbDbResponseValue>
}
#[derive(Debug, Deserialize)]
pub struct FlightRoute {
    pub callsign: Option<String>,
    pub origin: Option<FlightOrigin>,
    pub destination: Option<FlightDestination>
}
/*
#[derive(Debug, Deserialize)]
pub struct IcaoAircraft {
    pub registration: Option<String>,
}
*/
#[derive(Debug, Deserialize)]
pub struct FlightOrigin{
    icao_code: Option<String>
}
#[derive(Debug, Deserialize)]
pub struct FlightDestination {
    icao_code: Option<String>
}
#[derive(Debug, Serialize, Clone)]
pub struct IcaoCacheEntry {
    pub route: Option<ProcessedIcaoLookup>,
    #[serde(skip)] // Only for internal cache management
    pub lookup_time: Instant
}
impl IcaoCacheEntry {
    pub fn new(route: Option<ProcessedIcaoLookup>) -> Self {
        Self { route, lookup_time: Instant::now() }
    }
}
#[derive(Debug, Serialize, Clone)]
pub struct ProcessedIcaoLookup {
    pub callsign: String,
    pub origin: String,
    pub destination: String,
}
impl From<AircraftAndCallsign> for Option<ProcessedIcaoLookup> {
    fn from(raw_ac: AircraftAndCallsign) -> Self {
        let route = raw_ac.flightroute?;
        let callsign = route.callsign?;
        let origin = route.origin?;
        let destination = route.destination?;

        Some(ProcessedIcaoLookup {
            callsign: callsign,
            origin: origin.icao_code?,
            destination: destination.icao_code?
        })
    }
}

pub struct IcaoUpstream {
    pub url: String,
}

impl UpstreamSource for IcaoUpstream {
    fn poll(
        &self,
        agent: &ureq::Agent,
        api_calls: &super::ApiCallCounts,
        endpoint: &str,
    ) -> Result<Vec<ProcessedApiData>, String> {
        api_calls.increment(endpoint);

        let response = match agent.get(&self.url).call() {
            Ok(response) => response,
            Err(ureq::Error::Status(_, response)) => response,
            Err(error) => return Err(error.to_string()),
        };

        let envelope: AdsbDbResponse = response
            .into_json()
            .map_err(|e| e.to_string())?;

        let icao_objet = match envelope.response {
            Some(AdsbDbResponseValue::Aircraft(aircraft)) => {
                Option::<ProcessedIcaoLookup>::from(aircraft)
            }
            Some(AdsbDbResponseValue::Unknown(message))
                if message == "unknown callsign" => None,
            Some(AdsbDbResponseValue::Unknown(message)) => {
                return Err(format!("Unexpected ADSBDB response: {message}"));
            }
            None => None,
        };

        // wrap into vector for API compatibility
        Ok(icao_objet
            .into_iter()
            .map(ProcessedApiData::Icao)
            .collect())
    }
}