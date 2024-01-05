use std::time::Duration;

use rdkafka::{
    client::DefaultClientContext,
    message::ToBytes,
    producer::{DefaultProducerContext, FutureProducer, FutureRecord, Producer, ProducerContext},
    ClientContext, Message,
};
use tracing::Span;

use crate::{config::ProducerConfig, util, KafkaError};

#[derive(Debug, Clone, Copy)]
pub struct ProduceResponse {
    pub partition: i32,
    pub offset: i64,
}

pub struct KafkaProducer<C = DefaultProducerContext>
where
    C: ClientContext + 'static,
{
    inner: FutureProducer<C>,
}

// implement instantiation with the default context
impl KafkaProducer<DefaultClientContext> {
    pub fn from_config(
        config: ProducerConfig,
    ) -> Result<KafkaProducer<DefaultClientContext>, KafkaError> {
        let producer = KafkaProducer {
            inner: config.create()?,
        };

        Ok(producer)
    }
}

// implement instantiation with any custom producer context
impl<C: ProducerContext + 'static> KafkaProducer<C> {
    pub fn from_config_and_context(config: ProducerConfig, context: C) -> Result<Self, KafkaError> {
        let producer = KafkaProducer {
            inner: config.create_with_context(context)?,
        };

        Ok(producer)
    }
}

// implement generic methods shared by producers with any type of client context
impl<C: ClientContext + 'static> KafkaProducer<C> {
    /// Get a reference to the inner [`rdkafka::producer::FutureProducer`].
    pub fn inner(&self) -> &FutureProducer<C> {
        &self.inner
    }

    #[instrument(name="Produce Topic",
        skip_all,
        fields(messaging.system = "kafka", span.kind = "producer",
               message.kafka.topic = %record.topic, messaging.kafka.partition, messaging.kafka.offset)
    )]
    pub async fn produce_record<K: ToBytes + ?Sized, P: ToBytes + ?Sized>(
        &self,
        record: FutureRecord<'_, K, P>,
        queue_timeout: Option<Duration>,
    ) -> Result<ProduceResponse, KafkaError> {
        // Default to timeout of 0 seconds if queue_timeout is not specified
        let queue_timeout = queue_timeout.unwrap_or_default();

        let topic = record.topic.to_string();
        let result = self.inner.send(record, queue_timeout).await;

        let (partition, offset) = match result {
            Ok((partition, offset)) => (partition, offset),
            Err((err, m)) => {
                let key = util::key_view_as_str(&m);

                error!(
                    key,
                    topic = %m.topic(),
                    partition = %m.partition(),
                    offset = %m.offset(),
                    timestamp = ?m.timestamp(),
                    "Error producing message to topic {}:{} @ offset {} -- {err}",
                    m.topic(), m.partition(), m.offset()
                );

                Span::current()
                    .record("messaging.kafka.partition", m.partition())
                    .record("messaging.kafka.offset", m.offset());

                return Err(err.into());
            }
        };

        Span::current()
            .record("messaging.kafka.partition", partition)
            .record("messaging.kafka.offset", offset);

        trace!(
            topic,
            partition,
            offset,
            "Produced message to topic {topic}:{partition} @ offset {offset}"
        );

        Ok(ProduceResponse { partition, offset })
    }

    pub async fn produce(
        &self,
        topic_name: &str,
        key: Option<&[u8]>,
        message: impl ToBytes,
        queue_timeout: Option<Duration>,
    ) -> Result<ProduceResponse, KafkaError> {
        let mut record = FutureRecord::to(topic_name).payload(message.to_bytes());

        if let Some(key) = key {
            record = record.key(key);
        }

        self.produce_record(record, queue_timeout).await
    }

    /// Flushes any pending messages. This method should be called before termination to ensure
    /// delivery of all enqueued messages. It will call poll() internally. If not specified, `timeout`
    /// defaults to 5s.
    pub fn flush(&self, timeout: Option<Duration>) -> Result<(), KafkaError> {
        self.inner
            .flush(timeout.unwrap_or(Duration::from_secs(5)))
            .map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use std::any::{Any, TypeId};

    use crate::{consumer::KafkaConsumer, ConsumerConfig, OffsetResetAction};

    use super::*;
    use anyhow::Result;
    use pretty_assertions::assert_eq;
    use rdkafka::{
        message::{Header, Headers, OwnedHeaders},
        producer::DeliveryResult,
    };
    use test_log::test;
    use uuid::Uuid;

    fn get_bootstrap_servers() -> String {
        match std::env::var("BOOTSTRAP_SERVERS") {
            Ok(servers) => {
                trace!("BOOTSTRAP_SERVERS set to: {}", servers);
                servers
            }
            Err(_) => {
                trace!("BOOTSTRAP_SERVERS not set, using default localhost:29092");
                "localhost:29092".to_string()
            }
        }
    }

    fn get_producer() -> KafkaProducer<DefaultClientContext> {
        KafkaProducer::from_config(
            ProducerConfig::default()
                .set_bootstrap_servers(get_bootstrap_servers())
                .set_acks(1),
        )
        .unwrap()
    }

    fn get_consumer() -> KafkaConsumer {
        let config = ConsumerConfig::default()
            .set_bootstrap_servers(get_bootstrap_servers())
            .set_group_id("test_group")
            .set_auto_offset_reset(OffsetResetAction::Earliest)
            .set_allow_auto_create_topics(true)
            .set_enable_auto_commit(true);
        KafkaConsumer::from_config(config).unwrap()
    }

    fn short_uuid() -> String {
        Uuid::new_v4()
            .as_simple()
            .to_string()
            .split_at(8)
            .0
            .to_string()
    }

    fn get_random_topic() -> String {
        format!("test_{}", short_uuid())
    }

    #[test]
    fn test_get_topic_name() {
        for _ in 0..10 {
            let topic_name = get_random_topic();
            trace!("topic_name: {}", topic_name);
            assert_eq!(topic_name.len(), 13);
        }
    }

    #[test(tokio::test)]
    async fn test_create_with_context() {
        struct ProducerTestContext;

        impl ClientContext for ProducerTestContext {}
        impl ProducerContext for ProducerTestContext {
            type DeliveryOpaque = ();

            fn delivery(&self, r: &DeliveryResult<'_>, _: Self::DeliveryOpaque) {
                info!("delivered message: {:?}", r);
            }
        }

        let producer = KafkaProducer::from_config_and_context(
            ProducerConfig::default()
                .set_bootstrap_servers(get_bootstrap_servers())
                .set_acks(1),
            ProducerTestContext,
        )
        .unwrap();

        let topic_name = get_random_topic();

        let response = producer
            .produce(&topic_name, None, b"test message 1", None)
            .await
            .unwrap();

        debug!("response: {:?}", response);
    }

    #[test(tokio::test)]
    async fn test_produce() -> Result<()> {
        let topic_name = get_random_topic();
        // produce a message to the kafka broker defined in docker-compose.yaml
        let producer = get_producer();
        let response = producer
            .produce(&topic_name, None, b"test message 1", None)
            .await?;

        debug!("response: {:?}", response);
        assert_eq!(response.partition, 0, "partition should be 0");
        assert_eq!(response.offset, 0, "offset should be 0");

        let response = producer
            .produce(&topic_name, None, b"test message 2", None)
            .await?;

        debug!("response: {:?}", response);
        assert_eq!(response.partition, 0, "partition should be 0");
        assert_eq!(response.offset, 1, "offset should be 1");

        Ok(())
    }

    #[test(tokio::test)]
    async fn test_inner_accessor() {
        let producer = get_producer();
        // test that inner is an rdkafka FutureProducer
        assert_eq!(TypeId::of::<FutureProducer>(), producer.inner().type_id());
    }

    #[test(tokio::test)]
    async fn test_produce_record() -> Result<()> {
        let topic_name = get_random_topic();
        // produce a message to the kafka broker defined in docker-compose.yaml
        let producer = get_producer();

        let headers = OwnedHeaders::new()
            .insert(Header {
                key: "header",
                value: Some(&"value".to_string()),
            })
            .insert(Header {
                key: "header2",
                value: Some(&"value2".to_string()),
            });

        let record = FutureRecord::to(&topic_name)
            .key(b"my_key")
            .payload(b"test message 1")
            .headers(headers);
        let response = producer.produce_record(record, None).await?;

        debug!("response: {:?}", response);
        assert_eq!(response.partition, 0, "partition should be 0");
        assert_eq!(response.offset, 0, "offset should be 0");

        let consumer = get_consumer();
        consumer.subscribe(&[&topic_name])?;

        let message = consumer.recv().await?;

        let headers = message.headers().expect("headers should be present");
        assert_eq!(headers.count(), 2);

        if let Some(Ok(header)) = headers.try_get_as::<str>(0) {
            assert_eq!(header.key, "header", "header key should be header");
            assert_eq!(header.value, Some("value"), "header value should be value");
        } else {
            panic!("header not found");
        }

        if let Some(Ok(header)) = headers.try_get_as::<str>(1) {
            assert_eq!(header.key, "header2", "header key should be header2");
            assert_eq!(
                header.value,
                Some("value2"),
                "header value should be value2"
            );
        } else {
            panic!("header2 not found");
        }
        Ok(())
    }
}
