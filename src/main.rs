mod auth;
mod backend;
mod error;
mod model;
mod sql;
mod store;

use backend::Backend;
use clap::Parser;
use opensrv_mysql::{AsyncMysqlIntermediary, IntermediaryOptions};
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};
use store::Store;
use tokio::net::TcpListener;

#[derive(Debug, Parser)]
#[command(name = "rusty-mini-mysql")]
#[command(about = "A minimal MySQL protocol-compatible server in Rust (MVP)")]
struct Args {
    /// Listen address, e.g. 127.0.0.1:3307
    #[arg(long, default_value = "127.0.0.1:3307")]
    listen: String,

    /// Data directory for sled
    #[arg(long, default_value = "./data")]
    data: PathBuf,

    /// Root password (root@%) used on first boot; ignored if root already exists
    #[arg(long, default_value = "root")]
    root_password: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let store = Store::open(&args.data)?;
    store.ensure_root_user(&args.root_password)?;

    let listener = TcpListener::bind(&args.listen).await?;
    let local_addr = listener.local_addr()?;
    let conn_id = Arc::new(AtomicU32::new(1));

    eprintln!("rusty-mini-mysql listening on {}", local_addr);
    eprintln!(
        "Connect with: mysql -h {} -P {} -u root -p{}",
        local_addr.ip(),
        local_addr.port(),
        args.root_password
    );

    loop {
        let (stream, _addr) = listener.accept().await?;
        let store_cloned = store.clone();
        let id = conn_id.fetch_add(1, Ordering::Relaxed);

        tokio::spawn(async move {
            let (r, w) = tokio::io::split(stream);
            let backend = Backend::new(store_cloned, id);
            let opts = IntermediaryOptions {
                process_use_statement_on_query: false,
                reject_connection_on_dbname_absence: false,
            };
            if let Err(e) = AsyncMysqlIntermediary::run_with_options(backend, r, w, &opts).await {
                eprintln!("connection ended: {e}");
            }
        });
    }
}
