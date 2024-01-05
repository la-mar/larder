use super::client_config::ClientConfig;

use crate::{CompressionCodec, KafkaError, SecurityProtocol};

use rdkafka::producer::{FutureProducer, ProducerContext};

#[derive(Debug, Default)]
pub struct ProducerConfig {
    client: ClientConfig,
    acks: Option<i16>, // request_required_acks
    request_timeout_ms: Option<u32>,
    delivery_timeout_ms: Option<u32>,
    transactional_id: Option<String>,
    transaction_timeout_ms: Option<u32>,
    enable_idempotence: Option<bool>,
    compression_codec: Option<CompressionCodec>,
    compression_level: Option<i8>,
    queue_buffering_max_messages: Option<u32>,
    queue_buffering_max_kbytes: Option<u32>,
    linger_ms: Option<u32>, // queue.buffering.max.ms
    batch_num_messages: Option<u32>,
    batch_size: Option<u32>,
    retries: Option<u32>,
    retry_backoff_ms: Option<u32>,
    sticky_partition_linger_ms: Option<u32>,
}

impl ProducerConfig {
    /** When set to `true`, the producer will ensure that messages are successfully produced exactly once and in the original produce order. The following configuration properties are adjusted automatically (if not modified by the user) when idempotence is enabled: `max.in.flight.requests.per.connection=5` (must be less than or equal to 5), `retries=INT32_MAX` (must be greater than 0), `acks=all`, `queue.buffering.max.messages=INT32_MAX` (must be greater than 0), `linger.ms=5` (must be greater or equal to 0), `enable.idempotence=true`. Producer instantation will fail if user-supplied configuration is incompatible. Alias for `enable.idempotence`. */
    pub fn exactly_once() -> Self {
        Self::default().set_enable_idempotence(true)
    }

    // producer config

    /** This field indicates the number of acknowledgements the leader broker must receive from ISR brokers before responding to the request: *0*=Broker does not send any response/ack to client, *-1* or *all*=Broker will block until message is committed by all in sync replicas (ISRs). If there are less than `min.insync.replicas` (broker configuration) in the ISR set the produce request will fail. Alias for `request.required.acks` */
    pub fn set_acks(mut self, value: i16) -> Self {
        self.acks = Some(value);
        self
    }

    /** The ack timeout of the producer request in milliseconds. This value is only enforced by the broker and relies on `request.required.acks` being != 0. */
    pub fn set_request_timeout_ms(mut self, value: u32) -> Self {
        self.request_timeout_ms = Some(value);
        self
    }

    /** Local message timeout. This value is only enforced locally and limits the time a produced message waits for successful delivery. A time of 0 is infinite. This is the maximum time librdkafka may use to deliver a message (including retries). Delivery error occurs when either the retry count or the message timeout are exceeded. The message timeout is automatically adjusted to `transaction.timeout.ms` if `transactional.id` is configured. Alias for `message.timeout.ms`*/
    pub fn set_delivery_timeout_ms(mut self, delivery_timeout_ms: u32) -> Self {
        self.delivery_timeout_ms = Some(delivery_timeout_ms);
        self
    }

    /** The transactional id to use for transactional delivery. This enables reliability semantics which span multiple producer sessions since it allows the client to guarantee that transactions using the same transactional.id have been completed prior to starting any new transactions. If no transactional.id is provided, then the producer is limited to idempotent delivery. Must be at least 1 character and at most 249 characters long. */
    pub fn set_transactional_id(mut self, transactional_id: impl Into<String>) -> Self {
        self.transactional_id = Some(transactional_id.into());
        self
    }

    /** The maximum amount of time in ms that the transaction coordinator will wait for a transaction status update from the producer before proactively aborting the ongoing transaction. If this value is larger than the `transaction.max.timeout.ms` setting in the broker, the init_transactions() call will fail with ERR_INVALID_TRANSACTION_TIMEOUT. */
    pub fn set_transaction_timeout_ms(mut self, transaction_timeout_ms: u32) -> Self {
        self.transaction_timeout_ms = Some(transaction_timeout_ms);
        self
    }

    /** When set to `true`, the producer will ensure that messages are successfully produced exactly once and in the original produce order. The following configuration properties are adjusted automatically (if not modified by the user) when idempotence is enabled: `max.in.flight.requests.per.connection=5` (must be less than or equal to 5), `retries=INT32_MAX` (must be greater than 0), `acks=all`, `queue.buffering.max.messages=INT32_MAX` (must be greater than 0), `linger.ms=5` (must be greater or equal to 0), `enable.idempotence=true`. Producer instantation will fail if user-supplied configuration is incompatible. */
    pub fn set_enable_idempotence(mut self, enable_idempotence: bool) -> Self {
        self.enable_idempotence = Some(enable_idempotence);
        self
    }

    /** Compression codec to use for compressing message sets. This is the default value for all topics, may be overridden by the topic configuration property `compression.codec`. */
    pub fn set_compression_codec(mut self, compression_codec: CompressionCodec) -> Self {
        self.compression_codec = Some(compression_codec);
        self
    }

    /** Compression level parameter for algorithm selected by configuration property `compression.codec`. Higher values will result in better compression at the cost of more CPU usage. Usable range is algorithm-dependent: [0-9] for gzip; [0-12] for lz4; only 0 for snappy; -1 = codec-dependent default compression level. */
    pub fn set_compression_level(mut self, compression_level: i8) -> Self {
        self.compression_level = Some(compression_level);
        self
    }

    /** Maximum number of messages allowed on the producer queue. This queue is shared by all topics and partitions. */
    pub fn set_queue_buffering_max_messages(mut self, value: u32) -> Self {
        self.queue_buffering_max_messages = Some(value);
        self
    }

    /** Maximum total message size sum allowed on the producer queue. This queue is shared by all topics and partitions. This property has higher priority than queue.buffering.max.messages. */
    pub fn set_queue_buffering_max_kbytes(mut self, value: u32) -> Self {
        self.queue_buffering_max_kbytes = Some(value);
        self
    }

    /** Delay in milliseconds to wait for messages in the producer queue to accumulate before constructing message batches (MessageSets) to transmit to brokers. A higher value allows larger and more effective (less overhead, improved compression) batches of messages to accumulate at the expense of increased message delivery latency. */
    pub fn set_linger_ms(mut self, value: u32) -> Self {
        self.linger_ms = Some(value);
        self
    }

    /** Maximum number of messages batched in one MessageSet. The total MessageSet size is also limited by batch.size and message.max.bytes. */
    pub fn set_batch_num_messages(mut self, value: u32) -> Self {
        self.batch_num_messages = Some(value);
        self
    }

    /** Maximum size (in bytes) of all messages batched in one MessageSet, including protocol framing overhead. This limit is applied after the first message has been added to the batch, regardless of the first message's size, this is to ensure that messages that exceed batch.size are produced. The total MessageSet size is also limited by batch.num.messages and message.max.bytes. */
    pub fn set_batch_size(mut self, value: u32) -> Self {
        self.batch_size = Some(value);
        self
    }

    /** How many times to retry sending a failing Message. **Note:** retrying may cause reordering unless `enable.idempotence` is set to true. Alias for `message.send.max.retries`*/
    pub fn set_retries(mut self, value: u32) -> Self {
        self.retries = Some(value);
        self
    }

    /** The backoff time in milliseconds before retrying a protocol request. */
    pub fn set_retry_backoff_ms(mut self, value: u32) -> Self {
        self.retry_backoff_ms = Some(value);
        self
    }

    /** Delay in milliseconds to wait to assign new sticky partitions for each topic. By default, set to double the time of linger.ms. To disable sticky behavior, set to 0. This behavior affects messages with the key NULL in all cases, and messages with key lengths of zero when the consistent_random partitioner is in use. These messages would otherwise be assigned randomly. A higher value allows for more effective batching of these messages. */
    pub fn set_sticky_partition_linger_ms(mut self, value: u32) -> Self {
        self.sticky_partition_linger_ms = Some(value);
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

        // producer config
        if let Some(value) = self.acks {
            config.set("acks", value.to_string());
        }

        if let Some(value) = self.request_timeout_ms {
            config.set("request.timeout.ms", value.to_string());
        }

        if let Some(value) = self.delivery_timeout_ms {
            config.set("delivery.timeout.ms", value.to_string());
        }

        if let Some(value) = &self.transactional_id {
            config.set("transactional.id", value);
        }

        if let Some(value) = self.transaction_timeout_ms {
            config.set("transaction.timeout.ms", value.to_string());
        }

        if let Some(value) = self.enable_idempotence {
            config.set("enable.idempotence", value.to_string());
        }

        if let Some(value) = self.compression_codec {
            config.set("compression.codec", value.to_string());
        }

        if let Some(value) = self.compression_level {
            config.set("compression.level", value.to_string());
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

        if let Some(value) = self.queue_buffering_max_messages {
            config.set("queue.buffering.max.messages", value.to_string());
        }

        if let Some(value) = self.queue_buffering_max_kbytes {
            config.set("queue.buffering.max.kbytes", value.to_string());
        }

        if let Some(value) = self.linger_ms {
            config.set("linger.ms", value.to_string());
        }

        if let Some(value) = self.batch_num_messages {
            config.set("batch.num.messages", value.to_string());
        }

        if let Some(value) = self.batch_size {
            config.set("batch.size", value.to_string());
        }

        if let Some(value) = self.retries {
            config.set("retries", value.to_string());
        }

        if let Some(value) = self.retry_backoff_ms {
            config.set("retry.backoff.ms", value.to_string());
        }

        if let Some(value) = self.sticky_partition_linger_ms {
            config.set("sticky.partition.linger.ms", value.to_string());
        }

        config
    }

    pub fn create(&self) -> Result<FutureProducer, KafkaError> {
        let config = self.render_rdkafka_config();
        config.create::<FutureProducer>().map_err(|e| e.into())
    }

    pub fn create_with_context<C: ProducerContext + 'static>(
        &self,
        context: C,
    ) -> Result<FutureProducer<C>, KafkaError> {
        let config = self.render_rdkafka_config();
        config
            .create_with_context::<_, FutureProducer<C>>(context)
            .map_err(|e| e.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use test_log::test;

    #[test]
    fn test_exactly_once() {
        let cfg = ProducerConfig::exactly_once();
        assert_eq!(cfg.enable_idempotence, Some(true));
    }

    #[test]
    fn test_all_options() {
        let cfg = ProducerConfig::default()
            .set_acks(1)
            .set_request_timeout_ms(1000)
            .set_delivery_timeout_ms(2000)
            .set_transactional_id("test_transaction_id")
            .set_transaction_timeout_ms(2500)
            .set_enable_idempotence(true)
            .set_compression_codec(CompressionCodec::Gzip)
            .set_compression_level(5)
            .set_queue_buffering_max_messages(9000)
            .set_queue_buffering_max_kbytes(11000)
            .set_linger_ms(12000)
            .set_batch_num_messages(13000)
            .set_batch_size(14000)
            .set_retries(3)
            .set_retry_backoff_ms(3000)
            .set_sticky_partition_linger_ms(7000)
            .set_client_id("test_client_id")
            .set_bootstrap_servers("localhost:9092")
            .set_security_protocol(SecurityProtocol::PLAINTEXT)
            .set_max_in_flight_requests_per_connection(10)
            .set_message_max_bytes(4000)
            .set_reconnect_backoff_ms(5000)
            .set_reconnect_backoff_max_ms(6000)
            .set_broker_address_ttl(8000);

        // validate configurator

        // producer config
        assert_eq!(cfg.acks, Some(1), "acks mismatch");
        assert_eq!(
            cfg.request_timeout_ms,
            Some(1000),
            "request_timeout_ms mismatch"
        );
        assert_eq!(
            cfg.delivery_timeout_ms,
            Some(2000),
            "delivery_timeout_ms mismatch"
        );
        assert_eq!(
            cfg.transactional_id,
            Some("test_transaction_id".to_string()),
            "transactional_id mismatch"
        );
        assert_eq!(
            cfg.transaction_timeout_ms,
            Some(2500),
            "transaction_timeout_ms mismatch"
        );
        assert_eq!(
            cfg.enable_idempotence,
            Some(true),
            "enable_idempotence mismatch"
        );
        assert_eq!(
            cfg.compression_codec,
            Some(CompressionCodec::Gzip),
            "compression_codec mismatch"
        );
        assert_eq!(cfg.compression_level, Some(5), "compression_level mismatch");
        assert_eq!(
            cfg.queue_buffering_max_messages,
            Some(9000),
            "queue_buffering_max_messages mismatch"
        );
        assert_eq!(
            cfg.queue_buffering_max_kbytes,
            Some(11000),
            "queue_buffering_max_kbytes mismatch"
        );
        assert_eq!(cfg.linger_ms, Some(12000), "linger_ms mismatch");
        assert_eq!(
            cfg.batch_num_messages,
            Some(13000),
            "batch_num_messages mismatch"
        );
        assert_eq!(cfg.batch_size, Some(14000), "batch_size mismatch");
        assert_eq!(
            cfg.sticky_partition_linger_ms,
            Some(7000),
            "sticky_partition_linger_ms mismatch"
        );
        assert_eq!(cfg.retries, Some(3), "retries mismatch");
        assert_eq!(
            cfg.retry_backoff_ms,
            Some(3000),
            "retry_backoff_ms mismatch"
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

        // producer config
        let opt = cfg.get("acks").expect("expected acks option");
        assert_eq!(opt, "1", "acks mismatch in rdkafka config");

        let opt = cfg
            .get("request.timeout.ms")
            .expect("expected request.timeout.ms option");
        assert_eq!(opt, "1000", "request.timeout.ms mismatch");

        let opt = cfg
            .get("delivery.timeout.ms")
            .expect("expected delivery.timeout.ms option");
        assert_eq!(opt, "2000", "delivery.timeout.ms mismatch");

        let opt = cfg
            .get("transactional.id")
            .expect("expected transactional.id option");
        assert_eq!(opt, "test_transaction_id", "transactional.id mismatch");

        let opt = cfg
            .get("transaction.timeout.ms")
            .expect("expected transaction.timeout.ms option");

        assert_eq!(opt, "2500", "transaction.timeout.ms mismatch");

        let opt = cfg
            .get("enable.idempotence")
            .expect("expected enable.idempotence option");
        assert_eq!(opt, "true", "enable.idempotence mismatch");

        let opt = cfg
            .get("compression.codec")
            .expect("expected compression.codec option");
        assert_eq!(opt, "gzip", "compression.codec mismatch");

        let opt = cfg
            .get("compression.level")
            .expect("expected compression.level option");
        assert_eq!(opt, "5", "compression.level mismatch");

        let opt = cfg
            .get("queue.buffering.max.messages")
            .expect("expected queue.buffering.max.messages option");
        assert_eq!(opt, "9000", "queue.buffering.max.messages mismatch");

        let opt = cfg
            .get("queue.buffering.max.kbytes")
            .expect("expected queue.buffering.max.kbytes option");
        assert_eq!(opt, "11000", "queue.buffering.max.kbytes mismatch");

        let opt = cfg.get("linger.ms").expect("expected linger.ms option");
        assert_eq!(opt, "12000", "linger.ms mismatch");

        let opt = cfg
            .get("batch.num.messages")
            .expect("expected batch.num.messages option");
        assert_eq!(opt, "13000", "batch.num.messages mismatch");

        let opt = cfg.get("batch.size").expect("expected batch.size option");
        assert_eq!(opt, "14000", "batch.size mismatch");

        let opt = cfg.get("retries").expect("expected retries option");
        assert_eq!(opt, "3", "retries mismatch");

        let opt = cfg
            .get("retry.backoff.ms")
            .expect("expected retry.backoff.ms option");
        assert_eq!(opt, "3000", "retry.backoff.ms mismatch");

        let opt = cfg
            .get("sticky.partition.linger.ms")
            .expect("expected sticky.partition.linger.ms option");
        assert_eq!(opt, "7000", "sticky.partition.linger.ms mismatch");

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
