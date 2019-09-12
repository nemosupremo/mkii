use std::env;
use std::thread;
use std::net::SocketAddr;

use log::info;
use tokio::net::TcpListener;
use tokio::prelude::*;

use cpuprofiler::PROFILER;
const DO_PROFILE: bool = false;

mod command;
mod conn;
mod database;
mod resp;

async fn listen(addr: SocketAddr, worker_pool: tokio_io_pool::Handle) {
    let listener = TcpListener::bind(&addr).await.expect("unable to bind TCP listener");
    info!("Database is listening on {}", &addr);
    let mut incoming = listener.incoming();
    let mut i = 0;
    while let Some(stream) = incoming.next().await {
        let stream = stream.unwrap();
        let _ = stream.set_nodelay(true);

        conn::new(stream, i, &worker_pool);
        i = i.wrapping_add(1)
    }
}

fn main() {
    match env::var("RUST_LOG").ok() {
        Some(_) => (),
        None => env::set_var("RUST_LOG", "mkii"),
    };
    env_logger::init();
    info!("Starting mkii v{} database...", env!("CARGO_PKG_VERSION"));

    // Bind the server's socket.
    let addr = "0.0.0.0:6379".parse().unwrap();

    let mut iopool_builder = tokio_io_pool::Builder::default();
    iopool_builder.name_prefix("pool-worker-");

    let args: Vec<String> = env::args().collect();
    let pool_size = if args.len() > 1 {
        args[1].parse().expect("invalid pool_size")
    } else {
        0
    };

    if pool_size > 0 {
        iopool_builder.pool_size(pool_size);
        info!("Thread pool size: {}", pool_size);
    } else {
        info!("Thread pool size: default");
    }

    let core_ids = core_affinity::get_core_ids().unwrap();
    info!("CPU has {} cores", core_ids.len());
    {
        let i = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        iopool_builder.after_start(move || {
            let v = std::sync::Arc::clone(&i).fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if core_ids.len() > 1 {
                // setting the core affinity is slightly better for perf
                core_affinity::set_for_current(core_ids[v % core_ids.len()]);
            }
            if v == 0 && DO_PROFILE {
                if pool_size == 0 {
                    PROFILER.lock().unwrap().start("./my-prof.profile").unwrap();
                } else {
                    PROFILER
                        .lock()
                        .unwrap()
                        .start("./my-prof-single.profile")
                        .unwrap();
                }
            }
        });
    }

    let mut iopool = iopool_builder.build().unwrap();
    {
        let server_fut = listen(addr, iopool.handle().clone());
        if DO_PROFILE {
            iopool.spawn(server_fut).unwrap();

            thread::sleep(std::time::Duration::from_secs(30));
            PROFILER.lock().unwrap().stop().unwrap();
            std::process::exit(0);
        // iopool.shutdown_on_idle();
        } else {
            iopool.block_on(server_fut);
            iopool.shutdown_on_idle();
        }
    }
}
