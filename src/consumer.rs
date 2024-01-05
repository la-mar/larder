use rdkafka::{
    consumer::{Consumer, ConsumerContext, DefaultConsumerContext, MessageStream, StreamConsumer},
    message::BorrowedMessage,
    Message,
};
use tracing::Span;

use crate::{config::ConsumerConfig, util, KafkaError};

pub struct KafkaConsumer<C = DefaultConsumerContext>
where
    C: ConsumerContext + 'static,
{
    inner: StreamConsumer<C>,
    group_id: Option<String>,
}

// implement instantiation with the default consumer context
impl KafkaConsumer<DefaultConsumerContext> {
    pub fn from_config(config: ConsumerConfig) -> Result<Self, KafkaError> {
        let consumer = KafkaConsumer {
            inner: config.create()?,
            group_id: config.group_id().map(|s| s.to_owned()),
        };

        Ok(consumer)
    }
}

// implement instantiation with any custom consumer context
impl<C: ConsumerContext + 'static> KafkaConsumer<C> {
    pub fn from_config_and_context(config: ConsumerConfig, context: C) -> Result<Self, KafkaError> {
        let consumer = KafkaConsumer {
            inner: config.create_with_context(context)?,
            group_id: config.group_id().map(|s| s.to_owned()),
        };

        Ok(consumer)
    }
}

// implement generic methods shared by consumers with any consumer context
impl<C: ConsumerContext + 'static> KafkaConsumer<C> {
    pub fn inner(&self) -> &StreamConsumer<C> {
        &self.inner
    }

    pub fn group_id(&self) -> Option<&str> {
        self.group_id.as_deref()
    }

    pub fn store_offset_from_message(
        &self,
        message: &BorrowedMessage<'_>,
    ) -> Result<(), KafkaError> {
        match self.inner.store_offset_from_message(message) {
            Ok(_) => {
                trace!(
                    topic = %message.topic(),
                    partition = %message.partition(),
                    offset = %message.offset(),
                    "Stored offset for message"
                );
                Ok(())
            }
            Err(err) => Err(KafkaError::from(err)),
        }
    }

    pub fn subscribe(&self, topics: &[&str]) -> Result<(), KafkaError> {
        match self.inner.subscribe(topics) {
            Ok(_) => {
                debug!("Subscribed to topic(s): {:?}", topics);
                Ok(())
            }
            Err(err) => Err(KafkaError::from(err)),
        }
    }

    pub fn stream(&self) -> MessageStream<'_, C> {
        self.inner.stream()
    }

    #[instrument(name="Consume Topic",
        skip_all,
        fields(messaging.system = "kafka", span.kind = "consumer",
        messaging.kafka.topic, messaging.kafka.partition, messaging.kafka.offset,
               messaging.kafka.consumer_group = %self.group_id().unwrap_or_default()))]
    pub async fn recv(&self) -> Result<BorrowedMessage, KafkaError> {
        trace!("Waiting for message...");
        match self.inner.recv().await {
            Ok(m) => {
                Span::current()
                    .record("messaging.kafka.topic", m.topic())
                    .record("messaging.kafka.partition", m.partition())
                    .record("messaging.kafka.offset", m.offset());

                trace!(
                    key = %util::key_view_as_str(&m),
                    topic = %m.topic(),
                    partition = %m.partition(),
                    offset = %m.offset(),
                    timestamp = ?m.timestamp(),
                    "Received message from topic {}:{} @ offset {}",
                    m.topic(), m.partition(), m.offset()
                );

                Ok(m)
            }
            Err(err) => {
                error!("Error consume message: {:?}", err);
                Err(KafkaError::from(err))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::{config::ProducerConfig, KafkaProducer, OffsetResetAction};

    use anyhow::Result;
    use futures::stream::StreamExt;
    use pretty_assertions::assert_eq;
    use rdkafka::{
        client::DefaultClientContext, error::KafkaResult, types::RDKafkaErrorCode, ClientContext,
        Statistics, TopicPartitionList,
    };
    use test_log::test;
    use uuid::Uuid;

    pub struct ConsumerTestContext;

    impl ClientContext for ConsumerTestContext {
        // Access stats
        fn stats(&self, stats: Statistics) {
            let stats_str = format!("{:?}", stats);
            warn!("Stats received: {} bytes", stats_str.len());
        }
    }

    impl ConsumerContext for ConsumerTestContext {
        fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
            trace!("Committing offsets: {:?}", result);
        }
    }

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

    fn get_consumer() -> KafkaConsumer<ConsumerTestContext> {
        let config = ConsumerConfig::default()
            .set_bootstrap_servers(get_bootstrap_servers())
            .set_group_id(random_group_id())
            .set_auto_offset_reset(OffsetResetAction::Earliest)
            .set_allow_auto_create_topics(true)
            .set_enable_auto_commit(true);
        KafkaConsumer::from_config_and_context(config, ConsumerTestContext).unwrap()
    }

    fn get_producer() -> KafkaProducer<DefaultClientContext> {
        KafkaProducer::from_config(
            ProducerConfig::default().set_bootstrap_servers(get_bootstrap_servers()),
        )
        .unwrap()
    }

    fn short_uuid() -> String {
        Uuid::new_v4()
            .as_simple()
            .to_string()
            .split_at(8)
            .0
            .to_string()
    }

    fn random_group_id() -> String {
        format!("group_{}", short_uuid())
    }

    fn get_random_topic() -> String {
        format!("test_{}", short_uuid())
    }

    #[test(tokio::test)]
    async fn test_consumer() -> Result<()> {
        let topic = get_random_topic();
        let producer = get_producer();

        producer
            .produce(&topic, Some(b"my_key"), b"test message 1", None)
            .await
            .unwrap();

        producer
            .produce(&topic, None, "test message 2".to_string(), None)
            .await?;

        producer
            .produce(
                &topic,
                Some(&934203292840362623_u64.to_be_bytes()),
                934203292840362623_u64.to_be_bytes(),
                None,
            )
            .await?;

        let consumer = get_consumer();
        consumer.subscribe(&[&topic]).unwrap();

        // parse message 1 (key is String, payload is String)
        let message = consumer.recv().await.unwrap();
        let key = util::key_view_as_str(&message);
        let payload = std::str::from_utf8(message.payload().unwrap()).unwrap();
        info!("message1: key={key}, payload={payload}");
        assert_eq!(key, "my_key", "mismatched key on message 1");
        assert_eq!(payload, "test message 1", "mismatched payload on message 1");
        assert_eq!(message.partition(), 0, "mismatched partition on message 1");
        assert_eq!(message.offset(), 0, "mismatched offset on message 1");

        // parse message 2 (key is None, payload is String)
        let message = consumer.stream().next().await.unwrap().unwrap();
        let key = util::key_view_as_str(&message);
        let payload = std::str::from_utf8(message.payload().unwrap()).unwrap();
        info!("message2: key={key}, payload={payload}");
        assert_eq!(key, "none", "mismatched key on message 2");
        assert_eq!(payload, "test message 2", "mismatched payload on message 2");
        assert_eq!(message.partition(), 0, "mismatched partition on message 2");
        assert_eq!(message.offset(), 1, "mismatched offset on message 2");

        // parse message 3 (key and payload are both u64)
        let message = consumer.recv().await.unwrap();
        let key = util::key_as_u64(&message);
        let payload = message.payload().unwrap();
        trace!("payload bytes: {payload:#04x?}");
        let fixed: [u8; 8] = payload.try_into().unwrap();
        let payload = u64::from_be_bytes(fixed);
        info!("message3: key={key}, payload={payload}");
        assert_eq!(key, 934203292840362623, "mismatched key on message 3");
        assert_eq!(
            payload, 934203292840362623,
            "mismatched payload on message 3"
        );
        assert_eq!(message.partition(), 0, "mismatched partition on message 3");
        assert_eq!(message.offset(), 2, "mismatched offset on message 3");

        Ok(())
    }

    #[test(tokio::test)]
    async fn test_consumer_auto_create_topic() -> Result<()> {
        let topic = get_random_topic();

        let consumer = get_consumer();
        consumer.subscribe(&[&topic]).unwrap();

        // Transient UnknownTopicOrPartition errors are expected shortly after subscribing
        // to a topic that is auto created by the consumer.
        //
        // ref: https://github.com/confluentinc/librdkafka/blob/4837e34bc5d173b10934e874adadf39f98dd2b94/tests/0109-auto_create_topics.cpp#L165
        info!("consume attempt #1");
        match consumer.recv().await {
            Ok(_) => panic!("consumer.recv should have returned an error"),
            Err(e) => {
                info!("consume attempt #1 returned error: {}", e);

                match e {
                    KafkaError::RdKafkaError(rdkafka::error::KafkaError::MessageConsumption(
                        RDKafkaErrorCode::UnknownTopicOrPartition,
                    )) => warn!("consume attempt #1 returned UnknownTopicOrPartition"),
                    err => error!("consume attempt #1 returned unexpected error: {}", err),
                }
            }
        }

        // timeout if consumer.recv doesnt return after 3 seconds
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(2)) => {
                info!("consume attempt #2 successful");
            }
            result = consumer.recv() => {
                if let Err(e) = result {
                    panic!("consumer.recv returned error: {}", e);
                }
                info!("consume attempt #2 received message");

            }
        };

        Ok(())
    }
}
