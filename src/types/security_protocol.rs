use std::{fmt::Display, str::FromStr};

use crate::KafkaConfigError;

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecurityProtocol {
    #[default]
    PLAINTEXT,
    SSL,
}

impl SecurityProtocol {
    pub fn str(&self) -> &'static str {
        match self {
            SecurityProtocol::PLAINTEXT => "plaintext",
            SecurityProtocol::SSL => "ssl",
        }
    }
}

impl FromStr for SecurityProtocol {
    type Err = KafkaConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "plaintext" => Ok(SecurityProtocol::PLAINTEXT),
            "ssl" => Ok(SecurityProtocol::SSL),
            _ => Err(KafkaConfigError::UnknownVariant {
                name: "SecurityProtocol",
                variant: s.to_string(),
            }),
        }
    }
}

impl Display for SecurityProtocol {
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
        assert_eq!(SecurityProtocol::PLAINTEXT, "plaintext".parse().unwrap());
        assert_eq!(SecurityProtocol::SSL, "ssl".parse().unwrap());
        assert!("foo".parse::<SecurityProtocol>().is_err());
    }

    #[test]
    fn test_from_str_case_insensitivity() {
        assert_eq!(SecurityProtocol::PLAINTEXT, "PLAINTEXT".parse().unwrap());
        assert_eq!(SecurityProtocol::SSL, "SSL".parse().unwrap());
        assert!("FOO".parse::<SecurityProtocol>().is_err());
    }

    #[test]
    fn test_display() {
        assert_eq!("plaintext", format!("{}", SecurityProtocol::PLAINTEXT));
        assert_eq!("ssl", format!("{}", SecurityProtocol::SSL));
    }
}
