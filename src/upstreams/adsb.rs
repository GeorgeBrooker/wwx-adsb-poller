use super::UpstreamSource;
use serde::{Deserialize, Serialize};
use crate::data_model::ProcessedApiData;
use crate::helpers::{ft_to_m, knt_to_ms};

#[derive(Debug, Deserialize)]
pub struct AdsbFeed {
    pub ac: Option<Vec<AdsbFeedAC>>
}

#[derive(Debug, Deserialize)]
pub struct AdsbFeedAC {
    pub hex: Option<String>,
    pub flight: Option<String>, // Callsign
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub alt_baro: Option<serde_json::Value>, // Api can return a string or an int because they are evil
    pub gs: Option<f64>,
    pub track: Option<f64>, // When setting waypoints for a unit in DCS the track is the relevant information
}
#[derive(Debug, Serialize, Clone)]
pub struct ProcessedAircraft {
    pub hex: String,
    pub callsign: String, // Callsign
    pub lat: f64,
    pub lon: f64,
    pub alt_baro_m: f64, // DCS scripting api uses fp metric values 
    pub gs_ms: f64, // DCS scripting api uses metric 
    pub track: f64

}
impl From<AdsbFeedAC> for Option<ProcessedAircraft> {
    fn from(raw_ac: AdsbFeedAC) -> Self {
        // Drop any aircraft without an integer alt or with an alt < 0
        let alt_ft  = match raw_ac.alt_baro? { // Handle ints and string ints (123 & "123")
            serde_json::Value::Number(value) => value.as_i64(),
            serde_json::Value::String(value) => value.parse::<i64>().ok(),
            _ => None,
        }?;
        if alt_ft <= 0 {
            return None
        }
        // ? drops any null value and skips that aircraft
        Some(ProcessedAircraft {
            hex: raw_ac.hex?,
            callsign: raw_ac.flight?.trim().to_string(), // even though trim is a string method it returns a borrowed string, to_string() creates a string owned by us.
            lat: raw_ac.lat?,
            lon: raw_ac.lon?,
            alt_baro_m: ft_to_m(alt_ft),
            gs_ms: knt_to_ms(raw_ac.gs?),
            track: raw_ac.track?,
        })
    }
}
pub struct AdsbUpstream {
    pub url: String,
}

impl UpstreamSource for AdsbUpstream {
    fn poll(&self, agent: &ureq::Agent) -> Result<Vec<ProcessedApiData>, String> {
        let feed: AdsbFeed = agent
            .get(&self.url)
            .call()
            .map_err(|e| e.to_string())?
            .into_json()
            .map_err(|e| e.to_string())?;

        let aircraft = feed
            .ac
            .unwrap_or_default()
            .into_iter()
            .filter_map(Option::<ProcessedAircraft>::from)
            .map(ProcessedApiData::Aircraft)
            .collect();
        Ok(aircraft)
    }
}
