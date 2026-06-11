use celestia_client::types::nmt::Namespace;
use celestia_client::Client;
use celestia_grpc::GrpcClient;
use commonware_formatting::from_hex;
use coro::backend::CelestiaClientBackend;
use std::{collections::HashMap, path::Path, sync::Arc};

use crate::config::CelestiaConfig;

pub(crate) struct CelestiaClients {
    pub(crate) backend: CelestiaClientBackend,
    pub(crate) grpc: GrpcClient,
    pub(crate) header: Arc<Client>,
}

pub(crate) async fn read_only_backend(config: &CelestiaConfig) -> CelestiaClientBackend {
    let env_file =
        load_env_file(config.env_file.as_deref()).expect("failed to read celestia.env_file");
    let rpc_url = celestia_config_value(
        config.rpc_url.as_ref(),
        config.rpc_url_env.as_ref(),
        &env_file,
        "celestia.rpc_url",
    );
    let client = Client::builder()
        .rpc_url(&rpc_url)
        .build()
        .await
        .expect("failed to build read-only celestia-client");
    CelestiaClientBackend::new(client)
}

pub(crate) async fn celestia_clients(config: &CelestiaConfig) -> CelestiaClients {
    let env_file =
        load_env_file(config.env_file.as_deref()).expect("failed to read celestia.env_file");
    let rpc_url = celestia_config_value(
        config.rpc_url.as_ref(),
        config.rpc_url_env.as_ref(),
        &env_file,
        "celestia.rpc_url",
    );
    let grpc_url = celestia_config_value(
        config.grpc_url.as_ref(),
        config.grpc_url_env.as_ref(),
        &env_file,
        "celestia.grpc_url",
    );
    let private_key_hex = private_key_hex(config, &env_file);
    let client = Client::builder()
        .rpc_url(&rpc_url)
        .grpc_url(&grpc_url)
        .private_key_hex(&private_key_hex)
        .build()
        .await
        .expect("failed to build celestia-client");
    let header = Arc::new(
        Client::builder()
            .rpc_url(&rpc_url)
            .grpc_url(&grpc_url)
            .build()
            .await
            .expect("failed to build celestia header client"),
    );
    let grpc = GrpcClient::builder()
        .url(grpc_url)
        .private_key_hex(&private_key_hex)
        .build()
        .expect("failed to build celestia-grpc client");
    CelestiaClients {
        backend: CelestiaClientBackend::with_submit_client(client, grpc.clone()),
        grpc,
        header,
    }
}

fn private_key_hex(config: &CelestiaConfig, env_file: &HashMap<String, String>) -> String {
    config
        .private_key_hex
        .clone()
        .or_else(|| {
            let path = config.private_key_file.as_ref()?;
            private_key_hex_from_file(path).ok()
        })
        .or_else(|| {
            let name = config.private_key_env.as_ref()?;
            env_value(name, env_file)
        })
        .expect(
            "set celestia.private_key_hex, celestia.private_key_file, or CELESTIA_PRIVATE_KEY_HEX",
        )
}

fn celestia_config_value(
    inline: Option<&String>,
    env_name: Option<&String>,
    env_file: &HashMap<String, String>,
    label: &str,
) -> String {
    inline
        .filter(|value| !value.is_empty())
        .cloned()
        .or_else(|| env_name.and_then(|name| env_value(name, env_file)))
        .unwrap_or_else(|| panic!("set {label} or its configured env var"))
}

fn env_value(name: &str, env_file: &HashMap<String, String>) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(|| {
            env_file
                .get(name)
                .filter(|value| !value.is_empty())
                .cloned()
        })
}

fn load_env_file(path: Option<&Path>) -> Result<HashMap<String, String>, std::io::Error> {
    let Some(path) = path else {
        return Ok(HashMap::new());
    };
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let contents = std::fs::read_to_string(path)?;
    Ok(parse_env_file(&contents))
}

fn parse_env_file(contents: &str) -> HashMap<String, String> {
    contents
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            let (name, value) = line.split_once('=')?;
            let value = value.trim().trim_matches('"').trim_matches('\'');
            Some((name.trim().to_string(), value.to_string()))
        })
        .collect()
}

fn private_key_hex_from_file(path: &Path) -> Result<String, std::io::Error> {
    let contents = std::fs::read_to_string(path)?;
    let trimmed = contents.trim();
    if let Some((_, value)) = trimmed
        .lines()
        .filter_map(|line| line.split_once('='))
        .find(|(name, _)| name.trim() == "CELESTIA_PRIVATE_KEY_HEX")
    {
        let value = value.trim().trim_matches('"').trim_matches('\'');
        if !value.is_empty() {
            return Ok(value.to_string());
        }
    }
    Ok(trimmed.to_string())
}

pub(crate) fn namespace(value: &str) -> coro::NamespaceId {
    let bytes = from_hex(value).expect("namespace must be hex");
    let mut namespace = [0u8; 29];
    match bytes.len() {
        10 => namespace[19..].copy_from_slice(&bytes),
        29 => namespace.copy_from_slice(&bytes),
        other => {
            panic!("namespace must be 10-byte suffix or full 29-byte namespace, got {other} bytes")
        }
    }
    coro::NamespaceId(namespace)
}

pub(crate) fn celestia_namespace(namespace: coro::NamespaceId) -> Result<Namespace, String> {
    namespace.validate().map_err(|err| err.to_string())?;
    Namespace::from_raw(&namespace.0).map_err(|err| err.to_string())
}
