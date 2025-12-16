mod auth;
mod backend;
mod error;
mod model;
mod sql;
mod store;

use crate::backend::Backend;
use crate::store::Store;
use opensrv_mysql::{AsyncMysqlIntermediary, IntermediaryOptions};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let store = Store::open("data_dir")?;
    // Ensure default user
    store.ensure_root_user("password")?;

    let listener = TcpListener::bind("0.0.0.0:3306").await?;
    let local_addr = listener.local_addr()?;
    let conn_id = Arc::new(AtomicU32::new(1));
    info!("Server listening on {}", local_addr);

    while let Ok((stream, _)) = listener.accept().await {
        let store = store.clone();
        let id = conn_id.fetch_add(1, Ordering::Relaxed);
        tokio::spawn(async move {
            let (r, w) = tokio::io::split(stream);
            let backend = Backend::new(store, id);
             let opts = IntermediaryOptions {
                process_use_statement_on_query: false,
                reject_connection_on_dbname_absence: false,
            };
            if let Err(e) = AsyncMysqlIntermediary::run_with_options(
                backend,
                r, 
                w,
                &opts
            )
            .await
            {
                eprintln!("Connection error: {:?}", e);
            }
        });
    }
    Ok(())
}
