extern crate clap;
#[macro_use]
extern crate slog;
extern crate slog_term;

use clap::{App, Arg, SubCommand};
use std::process::exit;
use slog::*;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::io::Read;
use std::io::Write;
use std::io::BufRead;
use std::io::BufReader;
use kvs::proto::{ReqProto, RespProto};
use kvs::engine::{KvError, Result, KvsEngine};
use kvs::kvs_engine::KvStore;
use kvs::sled_engine::SledStore;

///
/// slog doc: https://docs.rs/slog/2.5.2/slog/
/// clap doc: https://docs.rs/clap/2.33.0/clap/
///
fn main() -> Result<()> {
    // init logger to stderr
    let plain = slog_term::PlainSyncDecorator::new(std::io::stderr());
    let logger = Logger::root(
        slog_term::FullFormat::new(plain).build().fuse(),
        o!()
    );

    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .arg(Arg::with_name("addr")
            .long("addr")
            .value_name("IP-PORT")
            .help("IP address(either v4 or v6) and port number, with the format IP:PORT, If not specified then listen on 127.0.0.1:4000")
            .takes_value(true)
        )
        .arg(Arg::with_name("engine")
            .long("engine")
            .value_name("ENGINE-NAME")
            .help("must be either \"kvs\", in which case the built-in engine is used, or \"sled\"")
            .takes_value(true)
        )
        .arg(Arg::with_name("version")
            .short("V")
            .help("Prints version information")
        )
        .get_matches();

    if matches.is_present("version") {
        println!("{}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    info!(logger, "kvs-server {}", env!("CARGO_PKG_VERSION"));

    let addr: SocketAddr = matches.value_of("addr").unwrap_or("127.0.0.1:4000").parse()?;
    // TODO: limit only kvs or sled, convert to enum
    let engine_name = matches.value_of("engine").unwrap_or("kvs");
    info!(logger, "storage engine `{}`, listen on `{}`...", engine_name, addr);

    info!(logger, "initializing storage engine");
    match engine_name {
        "kvs" => {
            let store = KvStore::default();
            run_with(store, addr, &logger)?;
        },
        "sled" => {
            let store = SledStore::default();
            run_with(store, addr, &logger)?;
        },
        _ => {
            error!(logger, "Unrecognized storage engine: `{}`", engine_name);
            exit(1);
        }
    }
    Ok(())
}

fn run_with(engine: impl KvsEngine, addr: SocketAddr, logger: &Logger) -> Result<()> {
    let listener = TcpListener::bind(addr)?;
    loop {
        match listener.accept() {
            Ok((mut stream, peer_addr)) => {
                debug!(logger, "accept remote stream from {}", peer_addr);
                let mut raw = Vec::new();
                let mut buf_stream = BufReader::new(&stream);
                buf_stream.read_until(b'\n', &mut raw);
                let proto: ReqProto = serde_json::from_slice(raw.as_slice())?;
                debug!(logger, "received command => `{:?}`", proto);
                match proto {
                    ReqProto::Get(key) => {
                        let val_opt = engine.get(key)?;
                        let resp = RespProto::OK(val_opt);
                        send_response(&mut stream, resp)?;
                    },
                    ReqProto::Set(key, value) => {
                        engine.set(key, value)?;
                    },
                    ReqProto::Remove(key) => {
                        match engine.remove(key) {
                            Err(KvError::KeyNotFound) => {
                                let resp = RespProto::Error("Key not found".to_string());
                                send_response(&mut stream, resp)?;
                            },
                            _ => {}
                        }
                    }
                }
            },
            Err(e) => error!(logger, "couldn't get remote stream: {:?}", e),
        }
    }
}

fn send_response(stream: &mut TcpStream, resp: RespProto) -> Result<()> {
    let raw = serde_json::to_string(&resp)?;
    stream.write(raw.as_bytes())?;
    stream.flush()?;
    Ok(())
}
