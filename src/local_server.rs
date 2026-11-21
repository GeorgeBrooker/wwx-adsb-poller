use tiny_http::{self, Header, Response};
use std::sync::Arc;
use crate::poll_worker::SharedState;

pub fn serve(state: Arc<SharedState>, address: &str) {
    let server = tiny_http::Server::http(address).expect("failed to start local server...");

    for request in server.incoming_requests() {
        *state.last_local_ping.write().unwrap() = std::time::Instant::now();

        let latest_data = state.latest_data.read().unwrap();
        let json = serde_json::to_string(&*latest_data)
            .unwrap_or_else(|_| "{}".to_owned());

        let mut response = Response::from_string(json);
        response.add_header(
            Header::from_bytes(
                &b"Content-Type"[..],
                &b"application/json"[..],
            )
            .unwrap(),
        );

        let _ = request.respond(response);
    }
}