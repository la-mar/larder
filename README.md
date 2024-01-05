# larder

This crate provides standard Kafka producer and consumer wrappers around [rust-rdkafka](https://github.com/fede1024/rust-rdkafka) in addition to strongly typed configuration for Kafka clients, producers, and consumers. All Kafka clients come with built-in tracing integration.


## Usage

#### Producer

Basic Producer:
```rust
// create a basic producer
let config = ProducerConfig::default().set_bootstrap_servers("localhost:29092");
let producer = KafkaProducer::from_config(config)?;

// produce a message with a partition key
producer.produce(&topic, Some(b"my_key"), b"test message 1", None).await?;

// produce a message with no partition key
producer.produce(&topic, None, "test message 2".to_string(), None).await?;

// produce a message with integer partition key and payload
producer.produce( &topic, Some(&934203292840362623_u64.to_be_bytes()), 934203292840362623_u64.to_be_bytes(), None).await?;
```

#### Consumer

Basic Consumer:
```rust
// create a basic consumer
let config = ConsumerConfig::default()
    .set_bootstrap_servers("localhost:29092")
    .set_group_id("my_consumer_group")
    .set_enable_auto_commit(true);
let consumer = KafkaConsumer::from_config(config)?

// subscribe to one or more topics
consumer.subscribe(&[&topic])?;

// consume one message from one of the subscribed topics
let message = consumer.recv().await?;

// unmarshal the message key and payload into string representations
let key = util::key_view_as_str(&message);
let payload = std::str::from_utf8(message.payload()?)?;
info!("message1: key={key}, payload={payload}");
```

Typically, you'll want to consume the stream in a loop:
```rust
loop {
    match consumer.recv().await {
        Ok(message) => {
            // process message
        },
        Err(e) => {
            eprintln!("do stupid things, win stupid prizes");
        }
    }
}
```
