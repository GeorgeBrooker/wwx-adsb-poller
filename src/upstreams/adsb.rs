use super::UpstreamSource;
use serde::{Deserialize, Serialize};
use crate::data_model::ProcessedApiData;
use crate::helpers::{ft_to_m, knt_to_ms};

pub const ADSB_CACHE_KEY: &str = "adsb";

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
    pub category: Option<String>, // Aircraft Type: https://support.adsbexchange.com/hc/en-us/articles/44705224053517-Emitter-Category-ADS-B-DO-260B-2-2-3-2-5-2
    pub r: Option<String>, // Registration
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
    pub category: String,
    pub registration: String,
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

        // remove unsupported aircraft types, and aircrat with no type listed
        let category = raw_ac.category?;
        let supported_types = ["A1", "A2", "A3", "A4", "A5"]; // Fixed wing GA -> Large Airliners, no helicopters
        if !supported_types.contains(&&*category) {
            return None
        }

        // ? drops any null value and skips that aircraft
        Some(ProcessedAircraft {
            hex: raw_ac.hex?,
            callsign: raw_ac.flight?.trim().to_string(), // even though trim is a string method it returns a borrowed string, to_string() creates a string owned by us.
            lat: raw_ac.lat?,
            lon: raw_ac.lon?,
            category: category,
            registration: raw_ac.r?,
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
    fn poll(
        &self,
        agent: &ureq::Agent,
        api_calls: &super::ApiCallCounts,
        endpoint: &str,
    ) -> Result<Vec<ProcessedApiData>, String> {
        api_calls.increment(endpoint);

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
