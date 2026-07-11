use std::{net::SocketAddr, path::PathBuf, time::Duration};

use zoocooker_server::{
    config::ServerConfig, serve_persistent_single_node, serve_single_node_with_config,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse(std::env::args().skip(1))?;
    let config = ServerConfig {
        session_ttl: Duration::from_millis(args.session_ttl_ms),
        watch_channel_capacity: args.watch_channel_capacity,
        max_watches: args.max_watches,
    };

    if let Some(wal_path) = args.wal_path {
        serve_persistent_single_node(args.addr, wal_path, args.snapshot_path, config).await
    } else {
        serve_single_node_with_config(args.addr, config).await?;
        Ok(())
    }
}

#[derive(Debug)]
struct Args {
    addr: SocketAddr,
    wal_path: Option<PathBuf>,
    snapshot_path: Option<PathBuf>,
    session_ttl_ms: u64,
    watch_channel_capacity: usize,
    max_watches: usize,
}

impl Args {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut parsed = Self {
            addr: "127.0.0.1:50051"
                .parse()
                .map_err(|err| format!("invalid default addr: {err}"))?,
            wal_path: None,
            snapshot_path: None,
            session_ttl_ms: 10_000,
            watch_channel_capacity: 32,
            max_watches: 10_000,
        };

        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--addr" => {
                    parsed.addr = next_value(&mut args, "--addr")?
                        .parse()
                        .map_err(|err| format!("invalid --addr: {err}"))?;
                }
                "--wal" => parsed.wal_path = Some(PathBuf::from(next_value(&mut args, "--wal")?)),
                "--snapshot" => {
                    parsed.snapshot_path =
                        Some(PathBuf::from(next_value(&mut args, "--snapshot")?));
                }
                "--session-ttl-ms" => {
                    parsed.session_ttl_ms = next_value(&mut args, "--session-ttl-ms")?
                        .parse()
                        .map_err(|err| format!("invalid --session-ttl-ms: {err}"))?;
                }
                "--watch-channel-capacity" => {
                    parsed.watch_channel_capacity =
                        next_value(&mut args, "--watch-channel-capacity")?
                            .parse()
                            .map_err(|err| format!("invalid --watch-channel-capacity: {err}"))?;
                }
                "--max-watches" => {
                    parsed.max_watches = next_value(&mut args, "--max-watches")?
                        .parse()
                        .map_err(|err| format!("invalid --max-watches: {err}"))?;
                }
                "--help" | "-h" => return Err(usage()),
                other => return Err(format!("unknown argument: {other}\n\n{}", usage())),
            }
        }

        if parsed.snapshot_path.is_some() && parsed.wal_path.is_none() {
            return Err("--snapshot requires --wal".to_string());
        }
        if parsed.session_ttl_ms == 0 {
            return Err("--session-ttl-ms must be greater than 0".to_string());
        }
        if parsed.watch_channel_capacity == 0 {
            return Err("--watch-channel-capacity must be greater than 0".to_string());
        }

        Ok(parsed)
    }
}

fn next_value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn usage() -> String {
    [
        "Usage: zoocooker-server [options]",
        "",
        "Options:",
        "  --addr <ip:port>                 listen address (default 127.0.0.1:50051)",
        "  --wal <path>                     enable persistent mode with WAL",
        "  --snapshot <path>                restore/save snapshot path; requires --wal",
        "  --session-ttl-ms <ms>            session TTL (default 10000)",
        "  --watch-channel-capacity <n>     per-watch stream channel capacity (default 32)",
        "  --max-watches <n>                max active watches (default 10000)",
    ]
    .join("\n")
}
