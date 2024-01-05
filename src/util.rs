use rdkafka::Message;

pub fn key_view_as_str<M: Message>(message: &M) -> &'_ str {
    match message.key_view::<str>() {
        Some(Ok(k)) => k,
        Some(Err(utf8_err)) => {
            trace!("Error deserializing message key: {:?}", utf8_err);
            "unknown"
        }
        None => "none",
    }
}

// FIXME: handle error cases
pub fn key_as_u64<M: Message>(message: &M) -> u64 {
    message
        .key()
        .map(|k| u64::from_be_bytes(k.try_into().unwrap()))
        .unwrap_or(0)
}
