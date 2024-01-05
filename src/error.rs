pub type BoxedError = Box<dyn std::error::Error + Send + Sync>;

#[derive(thiserror::Error, Debug)]
pub enum KafkaError {
    #[error("error unmarshalling kafka message: {message}")]
    Deserialize { message: String, source: BoxedError },
    #[error("error marshalling kafka message: {message}")]
    Serialize { message: String, source: BoxedError },
    #[error(transparent)]
    RdKafkaError(#[from] rdkafka::error::KafkaError),
    #[error(transparent)]
    ConfigError(#[from] KafkaConfigError),
}

#[derive(thiserror::Error, Debug)]
pub enum KafkaConfigError {
    #[error("{name} has no variant: {variant}")]
    UnknownVariant { name: &'static str, variant: String },
}

#[cfg(test)]
mod tests {
    use std::io::{Error, ErrorKind};

    use super::*;
    use pretty_assertions::assert_eq;
    use test_log::test;

    #[test]
    fn test_kafka_error() {
        let message = "you forgot to escape the leading quote, didn't you.";
        let err = KafkaError::Deserialize {
            message: message.to_string(),
            source: Box::new(Error::new(ErrorKind::Other, message)),
        };
        assert_eq!(
            format!("{}", err),
            format!("error unmarshalling kafka message: {}", message)
        );
    }

    #[test]
    fn test_kafka_config_error() {
        let err = KafkaConfigError::UnknownVariant {
            name: "AmericanFlora",
            variant: "edelweiss".to_string(),
        };
        assert_eq!(
            format!("{}", err),
            "AmericanFlora has no variant: edelweiss"
        );
    }
}
