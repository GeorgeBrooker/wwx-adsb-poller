use crate::{
    data_model::ProcessedApiData, 
    poll_worker::SharedState, 
    upstreams::{UpstreamSource, adsb::ProcessedAircraft, adsb_db::{IcaoCacheEntry, IcaoUpstream}},
};
use std::{
    collections::HashMap,
    sync::{Arc, mpsc}
};
use ureq::Agent;

pub(crate) fn ft_to_m(ft:i64) -> f64 {
    ft as f64 * 0.3048
}
pub(crate) fn knt_to_ms(knts:f64) -> f64 {
    knts * 0.514444
}

pub(crate) fn get_icao_for_adsb(items: &[ProcessedApiData], state:Arc<SharedState>, agent: &Agent) -> Result<HashMap<String, IcaoCacheEntry>, String> {
    let mut lookups: Vec<ProcessedAircraft> = Vec::new();
    let icao_cache = state.icao_cache.read().unwrap();
    for item in items {
        let Some(aircraft) = item.as_aircraft() else {
            continue;
        };
        // check aircraft not in items first.
        if !icao_cache.contains_key(&aircraft.callsign) {
            lookups.push(aircraft.clone());
        }
    }
    drop(icao_cache);
    let (sender, receiver) = mpsc::channel();
    std::thread::scope(|scope| {
        for aircraft in lookups {
            let sender = sender.clone();
            let state = state.clone();
            scope.spawn(move || {
                let lookup_url = format!("https://api.adsbdb.com/v0/callsign/{}", aircraft.callsign);
                let upstream = IcaoUpstream{url: lookup_url};
                let result = upstream.poll(&agent, &state.api_calls, crate::upstreams::ICAO_ENDPOINT);
                sender.send((aircraft.callsign, result))
                    .expect("Lookup receiver was dropped");
            });
        }
    });
    drop(sender);
    let mut icao_cache_additions: HashMap<String, IcaoCacheEntry> = HashMap::new();
    for (callsign, result) in receiver {
        match result {
            Ok(results) => {
                let route = match results.as_slice() {
                    [ProcessedApiData::Icao(route)] => Some(route.clone()),
                    [] => None,
                    _ => {
                        eprintln!("[Poller] Unexpected ICAO result for {callsign}");
                        continue;
                    }
                };
                icao_cache_additions.insert(callsign, IcaoCacheEntry::new(route));
            }
            Err(error) => {
                eprintln!("[Poller] ICAO lookup for {callsign} failed: {error}");
                // No cache entry: retry on a later poll.
            }
        }
    }
    Ok(icao_cache_additions)
}

/* We need this for later
let registration = &aircraft.registration;
let callsign = &aircraft.callsign;
let lookup_url = format!("https://api.adsbdb.com/v0/aircraft/{registration}?callsign={callsign}");
let endpoint = Box::new(IcaoUpstream {url: lookup_url.to_owned()}) as Box<dyn UpstreamSource>;
match endpoint.poll(&agent) {
    Ok(icao_data) => {
        Some(FusedAircraft {
            hex: aircraft.hex,
            callsign: aircraft.callsign,
            lat: aircraft.lat,
            lon: aircraft.lon,
            category: aircraft.category,
            registration: aircraft.registration,
            gs_ms: aircraft.gs_ms,
            track: aircraft.track,
            origin: icao_data.origin,
            destination: icao_data.destination
        }) as ProcessedApiData;
    }
    Err(e) => {
        eprintln!("[Poller ICAO for ADSB] Upstream 'Icao' failed '{e}'");
    }
}
*/