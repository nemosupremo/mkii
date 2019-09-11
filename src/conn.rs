use tokio::codec::Framed;
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::sync::oneshot;

use super::command::{self, Command};
use super::database;
use super::resp;

pub fn new(stream: TcpStream, conn_no: usize, worker_pool: &tokio_io_pool::Handle) {
    let conn_fut = conn(stream, worker_pool.clone(), conn_no);
    let _ = worker_pool.spawn_on(conn_no as u64, conn_fut);
}

async fn conn(stream: TcpStream, worker_pool: tokio_io_pool::Handle, conn_no: usize) {
    let framed = Framed::new(stream, resp::Codec::new());
    let (mut resp_out, mut resp_in) = framed.split();

    let mut requested_disconnect = false;
    let conn_worker_shard = worker_pool.worker_id(conn_no as u64);
    while let Some(frame) = resp_in.next().await {
        let resp = match frame {
            Ok(msg) => {
                match process_req(msg) {
                    Ok(request) => {
                        let cmd = request.to_execute();
                        let shard = cmd.shard();
                        if conn_worker_shard == worker_pool.worker_id(shard)
                            || shard == std::u64::MAX
                        {
                            // fast path
                            match database::execute(cmd) {
                                Ok(r) => r,
                                Err(command::Error::Quit) => {
                                    // enabling the following will cause command::Execute
                                    // to not longer be Sync???
                                    // let _ = await!(resp_out.send(resp::Msg::Str("OK")));
                                    requested_disconnect = true;
                                    break;
                                }
                                Err(e) => resp::Msg::Error(format!("{}", e)),
                            }
                        } else {
                            let (p, c) = oneshot::channel::<resp::Msg>();
                            // tokio::spawn_async(async {
                            let fut = async move {
                                    let resp = match database::execute(request.to_execute()) {
                                        Ok(r) => r,
                                        Err(e) => resp::Msg::Error(format!("{}", e)),
                                    };
                                    p.send(resp).unwrap();
                                };
                            let _ = worker_pool.spawn_on(shard, fut);
                            c.await.unwrap()
                        }
                    }
                    // command::Error err (don't need ERR)
                    Err(e) => resp::Msg::Error(format!("{}", e)),
                }
            }
            // decode error
            Err(e) => resp::Msg::Error(format!("ERR {}", e)),
        };

        // if the send fails, the connection was dropped
        match resp_out.send(resp).await {
            Ok(_) => (),
            Err(_) => return,
        }
    }
    if requested_disconnect {
        let _ = resp_out.send(resp::Msg::Str("OK")).await;
    }
}

fn process_req(msg: resp::Msg) -> Result<Command, command::Error> {
    match msg {
        resp::Msg::Array(Some(args)) => {
            if args.len() > 0 {
                let args = command::Args(args);
                let command_name = match &args[0] {
                    resp::Msg::String(command) | resp::Msg::BulkString(Some(command)) => {
                        command.as_ref()
                    }
                    _ => return Err(command::Error::Err("invalid command")),
                };

                // This unsafe block converts arg(1) to ascii uppercase.
                // The reason this unsafe blocks exists is I really wanted
                // to implement process_req with minimal allocations. I don't
                // think I could acheive this in safe rust (well, I could add
                // a number of flags to Decoder to auto capitalize the first
                // argument), and I was so far down the rabbit hole I just
                // decided to do it unsafe.
                //
                // We should be safe here as we should be the only ones holding
                // a reference to the command buffer right as it comes out of
                // decode.
                unsafe {
                    let len = command_name.len();
                    let cmd_ptr = command_name.as_ptr() as *mut u8;
                    for i in 0..len {
                        let c = cmd_ptr.offset(i as isize);
                        // make lowercase ascii chars uppercase by flipping
                        // 5th bit (0b100000)
                        *c = if (*c > 96) && (*c < 123) {
                            *c ^ 0x20
                        } else {
                            *c
                        };
                    }
                }

                match command::COMMANDS.get(command_name) {
                    Some(c) => c(args),
                    _ => Err(command::Error::Error(format!(
                        "unknown command '{}'",
                        match std::str::from_utf8(command_name.as_ref()) {
                            Ok(s) => s,
                            Err(_) => return Err(command::Error::Err("invalid non-ascii command")),
                        }
                    ))),
                }
            } else {
                Err(command::Error::Err("invalid command"))
            }
        }
        _ => Err(command::Error::Err(
            "invalid type for command (simple strings are not supported)",
        )),
    }
}
