use std::{fmt::Display, str::FromStr};

use crate::KafkaConfigError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionCodec {
    None,
    Gzip,
    Snappy,
    Lz4,
    Zstd,
}

impl CompressionCodec {
    pub fn str(&self) -> &'static str {
        match self {
            CompressionCodec::None => "none",
            CompressionCodec::Gzip => "gzip",
            CompressionCodec::Snappy => "snappy",
            CompressionCodec::Lz4 => "lz4",
            CompressionCodec::Zstd => "zstd",
        }
    }
}

impl FromStr for CompressionCodec {
    type Err = KafkaConfigError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "none" => Ok(CompressionCodec::None),
            "gzip" => Ok(CompressionCodec::Gzip),
            "snappy" => Ok(CompressionCodec::Snappy),
            "lz4" => Ok(CompressionCodec::Lz4),
            "zstd" => Ok(CompressionCodec::Zstd),
            _ => Err(KafkaConfigError::UnknownVariant {
                name: "CompressionCodec",
                variant: s.to_string(),
            }),
        }
    }
}

impl Display for CompressionCodec {
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
        assert_eq!(CompressionCodec::None, "none".parse().unwrap());
        assert_eq!(CompressionCodec::Gzip, "gzip".parse().unwrap());
        assert_eq!(CompressionCodec::Snappy, "snappy".parse().unwrap());
        assert_eq!(CompressionCodec::Lz4, "lz4".parse().unwrap());
        assert_eq!(CompressionCodec::Zstd, "zstd".parse().unwrap());
    }

    #[test]
    fn test_from_str_error() {
        match "unknown".parse::<CompressionCodec>() {
            Ok(_) => panic!("expected error"),
            Err(e) => match e {
                KafkaConfigError::UnknownVariant { name, variant } => {
                    assert_eq!("CompressionCodec", name);
                    assert_eq!("unknown", variant);
                }
            },
        }
    }

    #[test]
    fn test_str() {
        assert_eq!("none", CompressionCodec::None.str());
        assert_eq!("gzip", CompressionCodec::Gzip.str());
        assert_eq!("snappy", CompressionCodec::Snappy.str());
        assert_eq!("lz4", CompressionCodec::Lz4.str());
        assert_eq!("zstd", CompressionCodec::Zstd.str());
    }

    #[test]
    fn test_display_codec() {
        assert_eq!("none", format!("{}", CompressionCodec::None));
        assert_eq!("gzip", format!("{}", CompressionCodec::Gzip));
        assert_eq!("snappy", format!("{}", CompressionCodec::Snappy));
        assert_eq!("lz4", format!("{}", CompressionCodec::Lz4));
        assert_eq!("zstd", format!("{}", CompressionCodec::Zstd));
    }
}
