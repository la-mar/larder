use crate::{
    IsolationLevel, KafkaError, OffsetResetAction, PartitionAssignmentStrategy, SecurityProtocol,
};

use super::client_config::ClientConfig;

use rdkafka::consumer::{ConsumerContext, StreamConsumer};

#[derive(Debug, Default)]
pub struct ConsumerConfig {
    client: ClientConfig,
    group_id: Option<String>,
    group_instance_id: Option<String>,
    session_timeout_ms: Option<u32>,
    heartbeat_interval_ms: Option<u32>,
    partition_assignment_strategy: Option<PartitionAssignmentStrategy>,
    max_poll_interval_ms: Option<u32>,
    enable_auto_offset_store: Option<bool>,
    auto_commit_interval_ms: Option<u32>,
    auto_offset_reset: Option<OffsetResetAction>,
    enable_partition_eof: Option<bool>,
    queued_min_messages: Option<u32>,
    queued_max_messages_kbytes: Option<u32>,
    fetch_wait_max_ms: Option<u32>,
    fetch_message_max_bytes: Option<u32>,
    fetch_min_bytes: Option<u32>,
    fetch_max_bytes: Option<u32>,
    fetch_error_backoff_ms: Option<u32>,
    isolation_level: Option<IsolationLevel>,
    enable_auto_commit: Option<bool>,
    allow_auto_create_topics: Option<bool>,
    // consume_cb: Option<()>,
    // rebalance_cb: Option<()>,
    // offset_commit_cb: Option<()>,
}

impl ConsumerConfig {
    /** Initialize a consumer configuration that is configured for at-least-once message delivery semantics. only commit the offsets explicitly stored via `consumer.store_offset`. */
    pub fn at_least_once() -> Self {
        Self::default()
            .set_enable_auto_commit(true)
            .set_enable_auto_offset_store(false)
    }

    /** Initialize a consumer configuration that is configured for exactly-once message delivery semantics. */
    pub fn exactly_once() -> Self {
        Self::default()
            .set_isolation_level(IsolationLevel::ReadCommitted)
            .set_enable_auto_commit(false)
            .set_enable_auto_offset_store(false)
    }

    // Get the consumer's group id.
    pub fn group_id(&self) -> Option<&str> {
        self.group_id.as_deref()
    }

    // consumer config

    /** Client group id string. All clients sharing the same group.id belong to the same group. */
    pub fn set_group_id(mut self, value: impl Into<String>) -> Self {
        self.group_id = Some(value.into());
        self
    }

    /** Enable static group membership. Static group members are able to leave and rejoin a group within the configured `session.timeout.ms` without prompting a group rebalance. This should be used in combination with a larger `session.timeout.ms` to avoid group rebalances caused by transient unavailability (e.g. process restarts). Requires broker version >= 2.3.0. */
    pub fn set_group_instance_id(mut self, value: impl Into<String>) -> Self {
        self.group_instance_id = Some(value.into());
        self
    }

    /** Client group session and failure detection timeout. The consumer sends periodic heartbeats (heartbeat.interval.ms) to indicate its liveness to the broker. If no hearts are received by the broker for a group member within the session timeout, the broker will remove the consumer from the group and trigger a rebalance. The allowed range is configured with the **broker** configuration properties `group.min.session.timeout.ms` and `group.max.session.timeout.ms`. Also see `max.poll.interval.ms`. */
    pub fn set_session_timeout_ms(mut self, value: u32) -> Self {
        self.session_timeout_ms = Some(value);
        self
    }

    /** Group session keepalive heartbeat interval. */
    pub fn set_heartbeat_interval_ms(mut self, value: u32) -> Self {
        self.heartbeat_interval_ms = Some(value);
        self
    }

    /** The name of one or more partition assignment strategies. The elected group leader will use a strategy supported by all members of the group to assign partitions to group members. If there is more than one eligible strategy, preference is determined by the order of this list (strategies earlier in the list have higher priority). Cooperative and non-cooperative (eager) strategies must not be mixed. Available strategies: range, roundrobin, cooperative-sticky. */
    pub fn set_partition_assignment_strategy(mut self, value: PartitionAssignmentStrategy) -> Self {
        self.partition_assignment_strategy = Some(value);
        self
    }

    /** Maximum allowed time between calls to consume messages (e.g., rd_kafka_consumer_poll()) for high-level consumers. If this interval is exceeded the consumer is considered failed and the group will rebalance in order to reassign the partitions to another consumer group member. Warning: Offset commits may be not possible at this point. Note: It is recommended to set `enable.auto.offset.store=false` for long-time processing applications and then explicitly store offsets (using offsets_store()) *after* message processing, to make sure offsets are not auto-committed prior to processing has finished. The interval is checked two times per second. See KIP-62 for more information. */
    pub fn set_max_poll_interval_ms(mut self, value: u32) -> Self {
        self.max_poll_interval_ms = Some(value);
        self
    }

    /** Automatically store offset of last message provided to application. The offset store is an in-memory store of the next offset to (auto-)commit for each partition. */
    pub fn set_enable_auto_offset_store(mut self, value: bool) -> Self {
        self.enable_auto_offset_store = Some(value);
        self
    }

    /** The frequency in milliseconds that the consumer offsets are committed (written) to offset storage. (0 = disable). This setting is used by the high-level consumer. */
    pub fn set_auto_commit_interval_ms(mut self, value: u32) -> Self {
        self.auto_commit_interval_ms = Some(value);
        self
    }

    /** Action to take when there is no initial offset in offset store or the desired offset is out of range: 'smallest','earliest' - automatically reset the offset to the smallest offset, 'largest','latest' - automatically reset the offset to the largest offset, 'error' - trigger an error (ERR__AUTO_OFFSET_RESET) which is retrieved by consuming messages and checking 'message->err'. */
    pub fn set_auto_offset_reset(mut self, value: OffsetResetAction) -> Self {
        self.auto_offset_reset = Some(value);
        self
    }

    /** Emit RD_KAFKA_RESP_ERR__PARTITION_EOF event whenever the consumer reaches the end of a partition. */
    pub fn set_enable_partition_eof(mut self, value: bool) -> Self {
        self.enable_partition_eof = Some(value);
        self
    }

    /** Minimum number of messages per topic+partition librdkafka tries to maintain in the local consumer queue. */
    pub fn set_queued_min_messages(mut self, value: u32) -> Self {
        self.queued_min_messages = Some(value);
        self
    }

    /** Maximum number of kilobytes of queued pre-fetched messages in the local consumer queue. If using the high-level consumer this setting applies to the single consumer queue, regardless of the number of partitions. When using the legacy simple consumer or when separate partition queues are used this setting applies per partition. This value may be overshot by fetch.message.max.bytes. This property has higher priority than queued.min.messages. */
    pub fn set_queued_max_messages_kbytes(mut self, value: u32) -> Self {
        self.queued_max_messages_kbytes = Some(value);
        self
    }

    /** Maximum time the broker may wait to fill the Fetch response with fetch.min.bytes of messages. */
    pub fn set_fetch_wait_max_ms(mut self, value: u32) -> Self {
        self.fetch_wait_max_ms = Some(value);
        self
    }

    /** Initial maximum number of bytes per topic+partition to request when fetching messages from the broker. If the client encounters a message larger than this value it will gradually try to increase it until the entire message can be fetched. */
    pub fn set_fetch_message_max_bytes(mut self, value: u32) -> Self {
        self.fetch_message_max_bytes = Some(value);
        self
    }

    /** Minimum number of bytes the broker responds with. If fetch.wait.max.ms expires the accumulated data will be sent to the client regardless of this setting. */
    pub fn set_fetch_min_bytes(mut self, value: u32) -> Self {
        self.fetch_min_bytes = Some(value);
        self
    }

    /** Maximum amount of data the broker shall return for a Fetch request. Messages are fetched in batches by the consumer and if the first message batch in the first non-empty partition of the Fetch request is larger than this value, then the message batch will still be returned to ensure the consumer can make progress. The maximum message batch size accepted by the broker is defined via `message.max.bytes` (broker config) or `max.message.bytes` (broker topic config). `fetch.max.bytes` is automatically adjusted upwards to be at least `message.max.bytes` (consumer config). */
    pub fn set_fetch_max_bytes(mut self, value: u32) -> Self {
        self.fetch_max_bytes = Some(value);
        self
    }

    /** How long to postpone the next fetch request for a topic+partition in case of a fetch error. */
    pub fn set_fetch_error_backoff_ms(mut self, value: u32) -> Self {
        self.fetch_error_backoff_ms = Some(value);
        self
    }

    /** Controls how to read messages written transactionally: `read_committed` - only return transactional messages which have been committed. `read_uncommitted` - return all messages, even transactional messages which have been aborted. */
    pub fn set_isolation_level(mut self, value: IsolationLevel) -> Self {
        self.isolation_level = Some(value);
        self
    }

    /** Automatically and periodically commit offsets in the background. Note: setting this to false does not prevent the consumer from fetching previously committed start offsets. To circumvent this behaviour set specific start offsets per partition in the call to assign(). */
    pub fn set_enable_auto_commit(mut self, value: bool) -> Self {
        self.enable_auto_commit = Some(value);
        self
    }

    /** Allow automatic topic creation on the broker when subscribing to or assigning non-existent topics. The broker must also be configured with `auto.create.topics.enable=true` for this configuration to take effect. Note: the default value (true) for the producer is different from the default value (false) for the consumer. Further, the consumer default value is different from the Java consumer (true), and this property is not supported by the Java producer. Requires broker version >= 0.11.0.0, for older broker versions only the broker configuration applies. */
    pub fn set_allow_auto_create_topics(mut self, value: bool) -> Self {
        self.allow_auto_create_topics = Some(value);
        self
    }

    // client config

    /** Client identifier. */
    pub fn set_client_id(mut self, value: impl Into<String>) -> Self {
        self.client.client_id = Some(value.into());
        self
    }

    /** Initial list of brokers as a CSV list of broker host or host:port. The application may also use `rd_kafka_brokers_add()` to add brokers during runtime. */
    pub fn set_bootstrap_servers(mut self, value: impl Into<String>) -> Self {
        self.client.bootstrap_servers = Some(value.into());
        self
    }

    /** Protocol used to communicate with brokers. */
    pub fn set_security_protocol(mut self, value: SecurityProtocol) -> Self {
        self.client.security_protocol = value;
        self
    }

    /** Maximum number of in-flight requests per broker connection. This is a generic property applied to all broker communication, however it is primarily relevant to produce requests. In particular, note that other mechanisms limit the number of outstanding consumer fetch request per broker to one. */
    pub fn set_max_in_flight_requests_per_connection(mut self, value: u32) -> Self {
        self.client.max_in_flight_requests_per_connection = Some(value);
        self
    }

    /** Maximum Kafka protocol request message size. Due to differing framing overhead between protocol versions the producer is unable to reliably enforce a strict max message limit at produce time and may exceed the maximum size by one message in protocol ProduceRequests, the broker will enforce the the topic's `max.message.bytes` limit (see Apache Kafka documentation). */
    pub fn set_message_max_bytes(mut self, value: u32) -> Self {
        self.client.message_max_bytes = Some(value);
        self
    }

    /** The initial time to wait before reconnecting to a broker after the connection has been closed. The time is increased exponentially until `reconnect.backoff.max.ms` is reached. -25% to +50% jitter is applied to each reconnect backoff. A value of 0 disables the backoff and reconnects immediately. */
    pub fn set_reconnect_backoff_ms(mut self, value: u32) -> Self {
        self.client.reconnect_backoff_ms = Some(value);
        self
    }

    /** The maximum time to wait before reconnecting to a broker after the connection has been closed. */
    pub fn set_reconnect_backoff_max_ms(mut self, value: u32) -> Self {
        self.client.reconnect_backoff_max_ms = Some(value);
        self
    }

    /** How long to cache the broker address resolving results (milliseconds). */
    pub fn set_broker_address_ttl(mut self, value: u32) -> Self {
        self.client.broker_address_ttl = Some(value);
        self
    }

    fn render_rdkafka_config(&self) -> rdkafka::ClientConfig {
        let mut config = rdkafka::ClientConfig::new();

        // consumer config
        if let Some(value) = self.group_id.as_ref() {
            config.set("group.id", value);
        }

        if let Some(value) = &self.group_instance_id {
            config.set("group.instance.id", value.to_string());
        }

        if let Some(value) = self.session_timeout_ms {
            config.set("session.timeout.ms", value.to_string());
        }

        if let Some(value) = self.heartbeat_interval_ms {
            config.set("heartbeat.interval.ms", value.to_string());
        }

        if let Some(value) = self.partition_assignment_strategy {
            config.set("partition.assignment.strategy", value.to_string());
        }

        if let Some(value) = self.max_poll_interval_ms {
            config.set("max.poll.interval.ms", value.to_string());
        }

        if let Some(value) = self.enable_auto_offset_store {
            config.set("enable.auto.offset.store", value.to_string());
        }

        if let Some(value) = self.auto_commit_interval_ms {
            config.set("auto.commit.interval.ms", value.to_string());
        }
        if let Some(value) = self.auto_offset_reset {
            config.set("auto.offset.reset", value.to_string());
        }
        if let Some(value) = self.enable_partition_eof {
            config.set("enable.partition.eof", value.to_string());
        }
        if let Some(value) = self.queued_min_messages {
            config.set("queued.min.messages", value.to_string());
        }
        if let Some(value) = self.queued_max_messages_kbytes {
            config.set("queued.max.messages.kbytes", value.to_string());
        }
        if let Some(value) = self.fetch_wait_max_ms {
            config.set("fetch.wait.max.ms", value.to_string());
        }
        if let Some(value) = self.fetch_message_max_bytes {
            config.set("fetch.message.max.bytes", value.to_string());
        }
        if let Some(value) = self.fetch_min_bytes {
            config.set("fetch.min.bytes", value.to_string());
        }
        if let Some(value) = self.fetch_max_bytes {
            config.set("fetch.max.bytes", value.to_string());
        }
        if let Some(value) = self.fetch_error_backoff_ms {
            config.set("fetch.error.backoff.ms", value.to_string());
        }
        if let Some(value) = self.isolation_level {
            config.set("isolation.level", value.to_string());
        }
        if let Some(value) = self.enable_auto_commit {
            config.set("enable.auto.commit", value.to_string());
        }

        if let Some(value) = self.allow_auto_create_topics {
            config.set("allow.auto.create.topics", value.to_string());
        }

        // client config
        if let Some(value) = &self.client.client_id {
            config.set("client.id", value);
        }

        if let Some(value) = &self.client.bootstrap_servers {
            config.set("bootstrap.servers", value);
        }

        config.set(
            "security.protocol",
            self.client.security_protocol.to_string(),
        );

        if let Some(value) = self.client.max_in_flight_requests_per_connection {
            config.set("max.in.flight.requests.per.connection", value.to_string());
        }

        if let Some(value) = self.client.message_max_bytes {
            config.set("message.max.bytes", value.to_string());
        }

        if let Some(value) = self.client.reconnect_backoff_ms {
            config.set("reconnect.backoff.ms", value.to_string());
        }

        if let Some(value) = self.client.reconnect_backoff_max_ms {
            config.set("reconnect.backoff.max.ms", value.to_string());
        }

        if let Some(value) = self.client.broker_address_ttl {
            config.set("broker.address.ttl", value.to_string());
        }

        config
    }

    pub fn create(&self) -> Result<StreamConsumer, KafkaError> {
        let config = self.render_rdkafka_config();
        config.create::<StreamConsumer>().map_err(|e| e.into())
    }

    pub fn create_with_context<C: ConsumerContext + 'static>(
        &self,
        context: C,
    ) -> Result<StreamConsumer<C>, KafkaError> {
        let config = self.render_rdkafka_config();
        config
            .create_with_context::<_, StreamConsumer<C>>(context)
            .map_err(|e| e.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use test_log::test;

    #[test]
    fn test_at_least_once() {
        let cfg = ConsumerConfig::at_least_once();
        assert_eq!(cfg.enable_auto_commit, Some(true));
        assert_eq!(cfg.enable_auto_offset_store, Some(false));
    }

    #[test]
    fn test_exactly_once() {
        let cfg = ConsumerConfig::exactly_once();

        assert_eq!(
            cfg.isolation_level,
            Some(IsolationLevel::ReadCommitted),
            "isolation_level mismatch"
        );

        assert_eq!(
            cfg.enable_auto_commit,
            Some(false),
            "enable_auto_commit mismatch"
        );

        assert_eq!(
            cfg.enable_auto_offset_store,
            Some(false),
            "enable_auto_offset_store mismatch"
        );
    }

    #[test]
    fn test_all_options() {
        let cfg = ConsumerConfig::default()
            .set_group_id("test_group_id")
            .set_group_instance_id("test_instance_id")
            .set_session_timeout_ms(1000)
            .set_heartbeat_interval_ms(2000)
            .set_partition_assignment_strategy(PartitionAssignmentStrategy::RoundRobin)
            .set_max_poll_interval_ms(3000)
            .set_enable_auto_offset_store(true)
            .set_auto_commit_interval_ms(4000)
            .set_auto_offset_reset(OffsetResetAction::Earliest)
            .set_enable_partition_eof(true)
            .set_queued_min_messages(5000)
            .set_queued_max_messages_kbytes(6000)
            .set_fetch_wait_max_ms(7000)
            .set_fetch_message_max_bytes(8000)
            .set_fetch_min_bytes(8100)
            .set_fetch_max_bytes(8200)
            .set_fetch_error_backoff_ms(9000)
            .set_isolation_level(IsolationLevel::ReadCommitted)
            .set_enable_auto_commit(true)
            .set_allow_auto_create_topics(true)
            .set_client_id("test_client_id")
            .set_bootstrap_servers("localhost:9092")
            .set_security_protocol(SecurityProtocol::PLAINTEXT)
            .set_max_in_flight_requests_per_connection(10)
            .set_message_max_bytes(4000)
            .set_reconnect_backoff_ms(5000)
            .set_reconnect_backoff_max_ms(6000)
            .set_broker_address_ttl(8000);

        // validate configurator

        // consumer config
        assert_eq!(
            cfg.group_id,
            Some("test_group_id".to_string()),
            "group_id mismatch"
        );
        assert_eq!(
            cfg.group_instance_id,
            Some("test_instance_id".to_string()),
            "group_instance_id mismatch"
        );
        assert_eq!(
            cfg.session_timeout_ms,
            Some(1000),
            "session_timeout_ms mismatch"
        );
        assert_eq!(
            cfg.heartbeat_interval_ms,
            Some(2000),
            "heartbeat_interval_ms mismatch"
        );
        assert_eq!(
            cfg.partition_assignment_strategy,
            Some(PartitionAssignmentStrategy::RoundRobin),
            "partition_assignment_strategy mismatch"
        );
        assert_eq!(
            cfg.max_poll_interval_ms,
            Some(3000),
            "max_poll_interval_ms mismatch"
        );
        assert_eq!(
            cfg.enable_auto_offset_store,
            Some(true),
            "enable_auto_offset_store mismatch"
        );
        assert_eq!(
            cfg.auto_commit_interval_ms,
            Some(4000),
            "auto_commit_interval_ms mismatch"
        );
        assert_eq!(
            cfg.auto_offset_reset,
            Some(OffsetResetAction::Earliest),
            "auto_offset_reset mismatch"
        );
        assert_eq!(
            cfg.enable_partition_eof,
            Some(true),
            "enable_partition_eof mismatch"
        );
        assert_eq!(
            cfg.queued_min_messages,
            Some(5000),
            "queued_min_messages mismatch"
        );
        assert_eq!(
            cfg.queued_max_messages_kbytes,
            Some(6000),
            "queued_max_messages_kbytes mismatch"
        );
        assert_eq!(
            cfg.fetch_wait_max_ms,
            Some(7000),
            "fetch_wait_max_ms mismatch"
        );
        assert_eq!(
            cfg.fetch_message_max_bytes,
            Some(8000),
            "fetch_message_max_bytes mismatch"
        );
        assert_eq!(cfg.fetch_min_bytes, Some(8100), "fetch_min_bytes mismatch");
        assert_eq!(cfg.fetch_max_bytes, Some(8200), "fetch_max_bytes mismatch");
        assert_eq!(
            cfg.fetch_error_backoff_ms,
            Some(9000),
            "fetch_error_backoff_ms mismatch"
        );
        assert_eq!(
            cfg.isolation_level,
            Some(IsolationLevel::ReadCommitted),
            "isolation_level mismatch"
        );
        assert_eq!(
            cfg.enable_auto_commit,
            Some(true),
            "enable_auto_commit mismatch"
        );
        assert_eq!(
            cfg.allow_auto_create_topics,
            Some(true),
            "allow_auto_create_topics mismatch"
        );

        // client config
        assert_eq!(
            cfg.client.client_id,
            Some("test_client_id".to_string()),
            "client_id mismatch"
        );
        assert_eq!(
            cfg.client.bootstrap_servers,
            Some("localhost:9092".to_string()),
            "bootstrap_servers mismatch"
        );
        assert_eq!(
            cfg.client.security_protocol,
            SecurityProtocol::PLAINTEXT,
            "security_protocol mismatch"
        );
        assert_eq!(
            cfg.client.max_in_flight_requests_per_connection,
            Some(10),
            "max_in_flight_requests_per_connection mismatch"
        );
        assert_eq!(
            cfg.client.message_max_bytes,
            Some(4000),
            "message_max_bytes mismatch"
        );
        assert_eq!(
            cfg.client.reconnect_backoff_ms,
            Some(5000),
            "reconnect_backoff_ms mismatch"
        );
        assert_eq!(
            cfg.client.reconnect_backoff_max_ms,
            Some(6000),
            "reconnect_backoff_max_ms mismatch"
        );
        assert_eq!(
            cfg.client.broker_address_ttl,
            Some(8000),
            "broker_address_ttl mismatch"
        );

        // validate rendered configuration
        let cfg = cfg.render_rdkafka_config();

        // consumer config
        let opt = cfg.get("group.id").expect("expected group.id option");
        assert_eq!(opt, "test_group_id", "group.id mismatch");

        let opt = cfg
            .get("group.instance.id")
            .expect("expected group.instance.id option");
        assert_eq!(opt, "test_instance_id", "group.instance.id mismatch");

        let opt = cfg
            .get("session.timeout.ms")
            .expect("expected session.timeout.ms option");
        assert_eq!(opt, "1000", "session.timeout.ms mismatch");

        let opt = cfg
            .get("heartbeat.interval.ms")
            .expect("expected heartbeat.interval.ms option");
        assert_eq!(opt, "2000", "heartbeat.interval.ms mismatch");

        let opt = cfg
            .get("partition.assignment.strategy")
            .expect("expected partition.assignment.strategy option");
        assert_eq!(opt, "roundrobin", "partition.assignment.strategy mismatch");

        let opt = cfg
            .get("max.poll.interval.ms")
            .expect("expected max.poll.interval.ms option");
        assert_eq!(opt, "3000", "max.poll.interval.ms mismatch");

        let opt = cfg
            .get("enable.auto.offset.store")
            .expect("expected enable.auto.offset.store option");
        assert_eq!(opt, "true", "enable.auto.offset.store mismatch");

        let opt = cfg
            .get("auto.commit.interval.ms")
            .expect("expected auto.commit.interval.ms option");
        assert_eq!(opt, "4000", "auto.commit.interval.ms mismatch");

        let opt = cfg
            .get("auto.offset.reset")
            .expect("expected auto.offset.reset option");
        assert_eq!(opt, "earliest", "auto.offset.reset mismatch");

        let opt = cfg
            .get("enable.partition.eof")
            .expect("expected enable.partition.eof option");
        assert_eq!(opt, "true", "enable.partition.eof mismatch");

        let opt = cfg
            .get("queued.min.messages")
            .expect("expected queued.min.messages option");
        assert_eq!(opt, "5000", "queued.min.messages mismatch");

        let opt = cfg
            .get("queued.max.messages.kbytes")
            .expect("expected queued.max.messages.kbytes option");
        assert_eq!(opt, "6000", "queued.max.messages.kbytes mismatch");

        let opt = cfg
            .get("fetch.wait.max.ms")
            .expect("expected fetch.wait.max.ms option");
        assert_eq!(opt, "7000", "fetch.wait.max.ms mismatch");

        let opt = cfg
            .get("fetch.message.max.bytes")
            .expect("expected fetch.message.max.bytes option");
        assert_eq!(opt, "8000", "fetch.message.max.bytes mismatch");

        let opt = cfg
            .get("fetch.min.bytes")
            .expect("expected fetch.min.bytes option");
        assert_eq!(opt, "8100", "fetch.min.bytes mismatch");

        let opt = cfg
            .get("fetch.max.bytes")
            .expect("expected fetch.max.bytes option");
        assert_eq!(opt, "8200", "fetch.max.bytes mismatch");

        let opt = cfg
            .get("fetch.error.backoff.ms")
            .expect("expected fetch.error.backoff.ms option");
        assert_eq!(opt, "9000", "fetch.error.backoff.ms mismatch");

        let opt = cfg
            .get("isolation.level")
            .expect("expected isolation.level option");
        assert_eq!(opt, "read_committed", "isolation.level mismatch");

        let opt = cfg
            .get("enable.auto.commit")
            .expect("expected enable.auto.commit option");
        assert_eq!(opt, "true", "enable.auto.commit mismatch");

        let opt = cfg
            .get("allow.auto.create.topics")
            .expect("expected allow.auto.create.topics option");
        assert_eq!(opt, "true", "allow.auto.create.topics mismatch");

        // client config
        let opt = cfg.get("client.id").expect("expected client.id option");
        assert_eq!(opt, "test_client_id", "client.id mismatch");

        let opt = cfg
            .get("bootstrap.servers")
            .expect("expected bootstrap.servers option");
        assert_eq!(opt, "localhost:9092", "bootstrap.servers mismatch");

        let opt = cfg
            .get("security.protocol")
            .expect("expected security.protocol option");
        assert_eq!(opt, "plaintext", "security.protocol mismatch");

        let opt = cfg
            .get("max.in.flight.requests.per.connection")
            .expect("expected max.in.flight.requests.per.connection option");
        assert_eq!(opt, "10", "max.in.flight.requests.per.connection mismatch");

        let opt = cfg
            .get("message.max.bytes")
            .expect("expected message.max.bytes option");
        assert_eq!(opt, "4000", "message.max.bytes mismatch");

        let opt = cfg
            .get("reconnect.backoff.ms")
            .expect("expected reconnect.backoff.ms option");
        assert_eq!(opt, "5000", "reconnect.backoff.ms mismatch");

        let opt = cfg
            .get("reconnect.backoff.max.ms")
            .expect("expected reconnect.backoff.max.ms option");
        assert_eq!(opt, "6000", "reconnect.backoff.max.ms mismatch");

        let opt = cfg
            .get("broker.address.ttl")
            .expect("expected broker.address.ttl option");
        assert_eq!(opt, "8000", "broker.address.ttl mismatch");
    }
}
