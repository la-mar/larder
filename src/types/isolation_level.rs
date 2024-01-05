use std::{fmt::Display, str::FromStr};

use crate::KafkaConfigError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
}

impl IsolationLevel {
    pub fn str(&self) -> &'static str {
        match self {
            IsolationLevel::ReadUncommitted => "read_uncommitted",
            IsolationLevel::ReadCommitted => "read_committed",
        }
    }
}

impl FromStr for IsolationLevel {
    type Err = KafkaConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "read_uncommitted" => Ok(IsolationLevel::ReadUncommitted),
            "read_committed" => Ok(IsolationLevel::ReadCommitted),
            _ => Err(KafkaConfigError::UnknownVariant {
                name: "IsolationLevel",
                variant: s.to_string(),
            }),
        }
    }
}

impl Display for IsolationLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use test_log::test;

    #[test]
    fn test_from_str() {
        assert_eq!(
            IsolationLevel::ReadUncommitted,
            "read_uncommitted".parse().unwrap()
        );
        assert_eq!(
            IsolationLevel::ReadCommitted,
            "read_committed".parse().unwrap()
        );
        assert!("foo".parse::<IsolationLevel>().is_err());
    }

    #[test]
    fn test_display() {
        assert_eq!(
            "read_uncommitted",
            format!("{}", IsolationLevel::ReadUncommitted)
        );
        assert_eq!(
            "read_committed",
            format!("{}", IsolationLevel::ReadCommitted)
        );
    }
}
