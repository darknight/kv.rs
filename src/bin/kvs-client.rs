extern crate clap;

use clap::{App, Arg, SubCommand};
use std::process::exit;
use std::net::{TcpStream, SocketAddr};
use kvs::proto::{ReqProto, RespProto};
use std::io::{Write, Read};

use kvs::engine::{KvError, Result, KvsEngine};
use kvs::kvs_engine::KvStore;

fn main() -> Result<()> {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(SubCommand::with_name("set")
            .arg(Arg::with_name("set_arg")
                .value_names(&["KEY", "VALUE"])
                .required(true)
                .help("kvs set <KEY> <VALUE>")
                .number_of_values(2)
            )
            .arg(Arg::with_name("addr")
                .long("addr")
                .value_name("IP-PORT")
                .help("If not specified then listen on 127.0.0.1:4000")
                .takes_value(true)
            )
        )
        .subcommand(SubCommand::with_name("get")
            .arg(Arg::with_name("get_arg")
                .value_name("KEY")
                .required(true)
                .help("kvs get <KEY>")
                .number_of_values(1)
            )
            .arg(Arg::with_name("addr")
                .long("addr")
                .value_name("IP-PORT")
                .help("If not specified then listen on 127.0.0.1:4000")
                .takes_value(true)
            )
        )
        .subcommand(SubCommand::with_name("rm")
            .arg(Arg::with_name("rm_arg")
                .value_name("KEY")
                .required(true)
                .help("kvs rm <KEY>")
                .number_of_values(1)
            )
            .arg(Arg::with_name("addr")
                .long("addr")
                .value_name("IP-PORT")
                .help("If not specified then listen on 127.0.0.1:4000")
                .takes_value(true)
            )
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

    match matches.subcommand() {
        ("set", Some(sub_m)) => {
            let input: Vec<&str> = sub_m.values_of("set_arg").unwrap().collect();
            let proto = ReqProto::Set(input[0].to_string(), input[1].to_string());
            let addr: SocketAddr = sub_m.value_of("addr").unwrap_or("127.0.0.1:4000").parse()?;
            send_command(proto, addr)?;
        }
        ("get", Some(sub_m)) => {
            let key = sub_m.value_of("get_arg").unwrap();
            let proto = ReqProto::Get(key.to_string());
            let addr: SocketAddr = sub_m.value_of("addr").unwrap_or("127.0.0.1:4000").parse()?;
            send_command(proto, addr)?;
        }
        ("rm", Some(sub_m)) => {
            let key = sub_m.value_of("rm_arg").unwrap();
            let proto = ReqProto::Remove(key.to_string());
            let addr: SocketAddr = sub_m.value_of("addr").unwrap_or("127.0.0.1:4000").parse()?;
            send_command(proto, addr)?;
        }
        _ => {
            panic!(matches.usage().to_string());
        }
    }

    Ok(())
}

fn send_command(proto: ReqProto, addr: SocketAddr) -> Result<()> {
    let mut raw = serde_json::to_string(&proto)?;
    raw.push('\n');

    let mut stream = TcpStream::connect(addr)?;
    stream.set_nodelay(true)?;
    stream.write(raw.as_bytes())?;
    stream.flush()?;

    let mut resp = Vec::new();
    stream.read_to_end(&mut resp);
    if resp.is_empty() {
        return Ok(()); // EOF
    }
    let proto: RespProto = serde_json::from_slice(resp.as_slice())?;
    match proto {
        RespProto::OK(Some(mut v)) => {
            println!("{}", v);
            Ok(())
        },
        RespProto::OK(None) => {
            println!("Key not found");
            Ok(())
        },
        RespProto::Error(err) => {
            eprintln!("{}", err);
            Err(KvError::KeyNotFound)
        },
    }
}
