use crate::{
    data_model::ProcessedApiData, poll_worker::SharedState, upstreams::{UpstreamSource, adsb_db::{ICAO_CACHE_KEY, IcaoUpstream}},
};
use std::sync::Arc;
use ureq::Agent;

pub(crate) fn ft_to_m(ft:i64) -> f64 {
    ft as f64 * 0.3048
}
pub(crate) fn knt_to_ms(knts:f64) -> f64 {
    knts * 0.514444
}

pub(crate) fn get_icao_for_adsb(items: &[ProcessedApiData], state:Arc<SharedState>, agent: &Agent) -> Result<Vec<ProcessedApiData>, String> {
    let mut icao_cache_additions: Vec<ProcessedApiData> = Vec::new();
    for item in items {
        let Some(aircraft) = item.as_aircraft() else {
            continue;
        };
        // check aircraft not in items first.
        let cached = {
            let latest_data = state.latest_data.read().unwrap();
            latest_data
                .get(ICAO_CACHE_KEY)
                .map(|records|{
                    records.iter().any(|record: &ProcessedApiData| {
                        record
                            .as_icao()
                            .is_some_and(|icao| icao.callsign == aircraft.callsign && icao.registration == aircraft.registration) 
                    })
                })
                .unwrap_or(false)
        };
        if cached {
            continue;
        }
        let registration = &aircraft.registration;
        let callsign = &aircraft.callsign;
        let lookup_url = format!("https://api.adsbdb.com/v0/aircraft/{registration}?callsign={callsign}");
        let endpoint = IcaoUpstream {url: lookup_url};
        match endpoint.poll(&agent) {
            Ok(mut icao_data) => icao_cache_additions.append(&mut icao_data),
            Err(error) => {
                eprintln!("[Poller ICAO for ADSB] Upstream 'Icao' failed '{error}'");
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