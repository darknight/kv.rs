extern crate clap;

use clap::{App, Arg, SubCommand};
use kvs::{KvStore, KvError, Result};
use std::process::exit;
use std::net::{TcpStream, SocketAddr};

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
        return Ok(()); //FIXME
    }

    match matches.subcommand() {
        ("set", Some(sub_m)) => {
            let input: Vec<&str> = sub_m.values_of("set_arg").unwrap().collect();
            let addr: SocketAddr = matches.value_of("addr").unwrap_or("127.0.0.1:4000").parse()?;
            let mut stream = TcpStream::connect(addr)?;
        }
        ("get", Some(sub_m)) => {
            let key = sub_m.value_of("get_arg").unwrap();
            let addr: SocketAddr = matches.value_of("addr").unwrap_or("127.0.0.1:4000").parse()?;
            let mut stream = TcpStream::connect(addr)?;
        }
        ("rm", Some(sub_m)) => {
            let key = sub_m.value_of("rm_arg").unwrap();
            let addr: SocketAddr = matches.value_of("addr").unwrap_or("127.0.0.1:4000").parse()?;
            let mut stream = TcpStream::connect(addr)?;
        }
        _ => {
            panic!(matches.usage().to_string());
        }
    }

    Ok(())
}
