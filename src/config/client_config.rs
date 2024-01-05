use crate::SecurityProtocol;

// ref: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md
#[derive(Debug, Default)]
pub(super) struct ClientConfig {
    pub(super) client_id: Option<String>,
    pub(super) bootstrap_servers: Option<String>,
    pub(super) security_protocol: SecurityProtocol,
    pub(super) max_in_flight_requests_per_connection: Option<u32>,
    pub(super) message_max_bytes: Option<u32>,
    pub(super) reconnect_backoff_ms: Option<u32>,
    pub(super) reconnect_backoff_max_ms: Option<u32>,
    pub(super) broker_address_ttl: Option<u32>,
}
