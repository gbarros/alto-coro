use commonware_codec::ReadExt;
use commonware_cryptography::{ed25519, Signer};
use commonware_formatting::from_hex;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn signer(private_key: &Option<String>, seed: u64) -> ed25519::PrivateKey {
    let Some(private_key) = private_key else {
        return ed25519::PrivateKey::from_seed(seed);
    };
    let bytes = from_hex(private_key).expect("private_key must be hex");
    ed25519::PrivateKey::read(&mut &bytes[..]).expect("private_key must decode as ed25519")
}

pub(crate) fn init_tracing(level: &str) {
    let level = level.parse().unwrap_or(tracing::Level::INFO);
    let _ = tracing_subscriber::fmt().with_max_level(level).try_init();
}

pub(crate) fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_millis()
        .try_into()
        .expect("millisecond timestamp fits in u64")
}

pub(crate) fn hex32(value: [u8; 32]) -> String {
    hex(value.as_slice())
}

pub(crate) fn hex29(value: [u8; 29]) -> String {
    hex(value.as_slice())
}

pub(crate) fn hex(value: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(value.len() * 2);
    for byte in value {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
