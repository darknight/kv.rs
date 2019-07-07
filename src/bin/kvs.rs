extern crate clap;

use clap::{App, Arg, SubCommand};
use kvs::KvStore;

fn main() {
    let matches = App::new(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .subcommand(SubCommand::with_name("set")
            .arg(Arg::with_name("set_arg")
                .value_names(&["KEY", "VALUE"])
                .help("kvs set <KEY> <VALUE>")
                .number_of_values(2)
            )
        )
        .subcommand(SubCommand::with_name("get")
            .arg(Arg::with_name("get_arg")
                .value_name("KEY")
                .help("kvs get <KEY>")
                .number_of_values(1)
            )
        )
        .subcommand(SubCommand::with_name("rm")
            .arg(Arg::with_name("rm_arg")
                .value_name("KEY")
                .help("kvs rm <KEY>")
                .number_of_values(1)
            )
        )
        .arg(Arg::with_name("version")
            .short("V")
        )
        .get_matches();

    if matches.is_present("version") {
        println!("{}", env!("CARGO_PKG_VERSION"));
        return;
    }

    let mut kv_store = KvStore::new();

    match matches.subcommand() {
        ("set", Some(sub_m)) => {
            let input: Vec<&str> = sub_m.values_of("set_arg").unwrap().collect();
            kv_store.set(input[0].to_owned(), input[1].to_owned());
        },
        ("get", Some(sub_m)) => {
            let key = sub_m.value_of("get_arg").unwrap();
            let value = kv_store.get(key.to_owned());
        },
        ("rm", Some(sub_m)) => {
            let key = sub_m.value_of("rm_arg").unwrap();
            let value = kv_store.remove(key.to_owned());
        },
        _ => {
            panic!(format!("{}", matches.usage()));
        }
    }
}
