use std::{fmt::Display, str::FromStr};

/** Action to take when there is no initial offset in offset store or the desired offset is out of range: 'smallest','earliest' - automatically reset the offset to the smallest offset, 'largest','latest' - automatically reset the offset to the largest offset, 'error' - trigger an error (ERR__AUTO_OFFSET_RESET) which is retrieved by consuming messages and checking 'message->err'.

ref: https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md?plain=1#L177
*/
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetResetAction {
    Smallest,
    Largest,
    Earliest,
    Latest,
    End,
    Error,
}

impl OffsetResetAction {
    pub fn str(&self) -> &'static str {
        match self {
            OffsetResetAction::Smallest => "smallest",
            OffsetResetAction::Largest => "largest",
            OffsetResetAction::Earliest => "earliest",
            OffsetResetAction::Latest => "latest",
            OffsetResetAction::End => "end",
            OffsetResetAction::Error => "error",
        }
    }
}

impl FromStr for OffsetResetAction {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "smallest" => Ok(OffsetResetAction::Smallest),
            "largest" => Ok(OffsetResetAction::Largest),
            "earliest" => Ok(OffsetResetAction::Earliest),
            "latest" => Ok(OffsetResetAction::Latest),
            "end" => Ok(OffsetResetAction::End),
            "error" => Ok(OffsetResetAction::Error),
            _ => Err(format!("Unknown offset reset action: {}", s)),
        }
    }
}

impl Display for OffsetResetAction {
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
        assert_eq!(OffsetResetAction::Smallest, "smallest".parse().unwrap());
        assert_eq!(OffsetResetAction::Largest, "largest".parse().unwrap());
        assert_eq!(OffsetResetAction::Earliest, "earliest".parse().unwrap());
        assert_eq!(OffsetResetAction::Latest, "latest".parse().unwrap());
        assert_eq!(OffsetResetAction::End, "end".parse().unwrap());
        assert_eq!(OffsetResetAction::Error, "error".parse().unwrap());
        assert!("foo".parse::<OffsetResetAction>().is_err());
    }

    #[test]
    fn test_display() {
        assert_eq!("smallest", format!("{}", OffsetResetAction::Smallest));
        assert_eq!("largest", format!("{}", OffsetResetAction::Largest));
        assert_eq!("earliest", format!("{}", OffsetResetAction::Earliest));
        assert_eq!("latest", format!("{}", OffsetResetAction::Latest));
        assert_eq!("end", format!("{}", OffsetResetAction::End));
        assert_eq!("error", format!("{}", OffsetResetAction::Error));
    }
}
