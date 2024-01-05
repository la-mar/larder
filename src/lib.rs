pub mod config;
pub mod consumer;
mod error;
pub mod producer;
pub mod types;
pub mod util;

#[macro_use]
extern crate tracing;

// re-export rdkafka for consumer convenience
pub use rdkafka;

pub use config::{ConsumerConfig, ProducerConfig};
pub use consumer::KafkaConsumer;
pub use error::{KafkaConfigError, KafkaError};
pub use producer::{KafkaProducer, ProduceResponse};
pub use rdkafka::{
    client::DefaultClientContext,
    consumer::DefaultConsumerContext,
    message::{BorrowedHeaders, BorrowedMessage, Header, Headers, OwnedHeaders, OwnedMessage},
    producer::{DefaultProducerContext, FutureRecord},
    ClientConfig, ClientContext, Message,
};
pub use types::*;
