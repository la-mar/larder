use std::{fmt::Display, str::FromStr};

use crate::KafkaConfigError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionAssignmentStrategy {
    Range,
    RoundRobin,
    CooperativeSticky,
}

impl PartitionAssignmentStrategy {
    pub fn str(&self) -> &'static str {
        match self {
            PartitionAssignmentStrategy::Range => "range",
            PartitionAssignmentStrategy::RoundRobin => "roundrobin",
            PartitionAssignmentStrategy::CooperativeSticky => "cooperative-sticky",
        }
    }
}

impl FromStr for PartitionAssignmentStrategy {
    type Err = KafkaConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "range" => Ok(PartitionAssignmentStrategy::Range),
            "roundrobin" => Ok(PartitionAssignmentStrategy::RoundRobin),
            "cooperative-sticky" => Ok(PartitionAssignmentStrategy::CooperativeSticky),
            _ => Err(KafkaConfigError::UnknownVariant {
                name: "PartitionAssignmentStrategy",
                variant: s.to_string(),
            }),
        }
    }
}

impl Display for PartitionAssignmentStrategy {
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
        assert_eq!(PartitionAssignmentStrategy::Range, "range".parse().unwrap());
        assert_eq!(
            PartitionAssignmentStrategy::RoundRobin,
            "roundrobin".parse().unwrap()
        );
        assert_eq!(
            PartitionAssignmentStrategy::CooperativeSticky,
            "cooperative-sticky".parse().unwrap()
        );
        assert!("foo".parse::<PartitionAssignmentStrategy>().is_err());
    }

    #[test]
    fn test_display() {
        assert_eq!("range", format!("{}", PartitionAssignmentStrategy::Range));
        assert_eq!(
            "roundrobin",
            format!("{}", PartitionAssignmentStrategy::RoundRobin)
        );
        assert_eq!(
            "cooperative-sticky",
            format!("{}", PartitionAssignmentStrategy::CooperativeSticky)
        );
    }
}
