use mysql::{Opts, OptsBuilder, Pool, PooledConn};
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

pub struct ServerGuard {
    child: Child,
    _data_dir: tempfile::TempDir,
    stderr_thread: Option<thread::JoinHandle<()>>,
}

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if let Some(handle) = self.stderr_thread.take() {
            let _ = handle.join();
        }
    }
}

pub fn spawn_server() -> anyhow::Result<(ServerGuard, SocketAddr)> {
    let bin = env!("CARGO_BIN_EXE_rusty-mini-mysql");
    let data_dir = tempfile::tempdir()?;

    let mut child = Command::new(bin)
        .args([
            "--listen",
            "127.0.0.1:0",
            "--data",
            data_dir.path().to_str().unwrap_or("./data"),
            "--root-password",
            "root",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;

    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow::anyhow!("failed to capture server stderr"))?;

    let (addr_tx, addr_rx) = mpsc::channel::<SocketAddr>();
    let stderr_thread = thread::spawn(move || {
        let mut reader = BufReader::new(stderr);
        let mut line = String::new();
        while reader
            .read_line(&mut line)
            .ok()
            .filter(|n| *n > 0)
            .is_some()
        {
            if let Some(rest) = line.strip_prefix("rusty-mini-mysql listening on ") {
                if let Ok(addr) = rest.trim().parse::<SocketAddr>() {
                    let _ = addr_tx.send(addr);
                }
            }
            eprint!("{}", line); // Relay output
            line.clear();
        }
    });

    let addr = match addr_rx.recv_timeout(Duration::from_secs(5)) {
        Ok(addr) => addr,
        Err(err) => {
            if let Some(status) = child.try_wait()? {
                anyhow::bail!("server exited before reporting listen address: {status} ({err})");
            }
            anyhow::bail!("timed out waiting for server listen address: {err}");
        }
    };

    Ok((
        ServerGuard {
            child,
            _data_dir: data_dir,
            stderr_thread: Some(stderr_thread),
        },
        addr,
    ))
}

pub fn pool_for_url(url: &str) -> anyhow::Result<Pool> {
    let opts = OptsBuilder::from_opts(Opts::from_url(url)?)
        .tcp_connect_timeout(Some(Duration::from_secs(1)));
    Ok(Pool::new(opts)?)
}

pub fn get_conn_with_retry(pool: &Pool, url: &str) -> anyhow::Result<PooledConn> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        match pool.get_conn() {
            Ok(conn) => return Ok(conn),
            Err(_) => thread::sleep(Duration::from_millis(200)),
        }
    }
    Err(anyhow::anyhow!("could not connect to server at {url}"))
}
