use std::str;
use bytes::{Bytes};
use seahash::SeaHasher;
use std::hash::{Hash, Hasher};

use super::{resp, Args, Command, Database, Error, Execute};

pub struct Del(bool, Bytes, pub Vec<Bytes>);
pub struct Keys(pub i64);

impl Execute for Keys {
    fn parse(mut args: Args) -> Result<Self, Error> {
        if args.len() == 1 {
            Ok(Keys(-1))
        } else {
            match args.own(1) {
                resp::Msg::BulkString(Some(key)) | resp::Msg::String(key) => {
                    match str::from_utf8(key.as_ref()) {
                        Ok(k) => match k.parse::<i64>() {
                            Ok(n) => Ok(Keys(n)),
                            Err(_) => Err(Error::Err("invalid parameter for 'keys' command")),
                        },
                        Err(_) => Err(Error::Err("invalid parameter for 'keys' command")),
                    }
                }
                resp::Msg::Int(n) => Ok(Keys(n)),
                _ => Err(Error::Err("invalid parameter for 'keys' command")),
            }
        }
    }

    fn shard(&self) -> u64 {
        match self.0 {
            -1 => 0, // TODO, -1 will be all keys
            0..=std::i64::MAX => self.0 as u64,
            // parse should reject commands with invalid shards
            _ => panic!(),
        }
    }

    fn exec(&self, db: &mut Database) -> Result<resp::Msg, Error> {
        Ok(resp::Msg::Array(Some(
            db.keys()
                .map(|key| resp::Msg::BulkString(Some(key.clone())))
                .collect(),
        )))
    }

    fn to_command(self) -> Command {
        Command::Keys(self)
    }
}

impl Execute for Del {
    fn parse(mut args: Args) -> Result<Self, Error> {
        if args.len() < 2 {
            return Err(Error::Err("wrong number of arguments for 'del' command"));
        }
        let keyname = match args.own(0) {
            resp::Msg::String(key) | resp::Msg::BulkString(Some(key)) => key,
            _ => return Err(Error::Err("invalid parameter for 'del' command")),
        };
        let del0 = match args.own(1) {
            resp::Msg::String(key) => key,
            resp::Msg::BulkString(Some(key)) => key,
            _ => return Err(Error::Err("invalid parameter for 'del' command")),
        };
        let mut del = match keyname.as_ref() {
            b"DEL" | b"del" => Del(true, del0, Vec::new()),
            b"UNLINK" | b"unlink" => Del(false, del0, Vec::new()),
            _ => Del(false, del0, Vec::new()),
        };
        for i in 2..args.len() {
            match args.own(i) {
                resp::Msg::String(key) | resp::Msg::BulkString(Some(key)) => del.2.push(key),
                _ => return Err(Error::Err("invalid parameter for 'del' command")),
            };
        }

        Ok(del)
    }

    fn shard(&self) -> u64 {
        // TODO manage other args
        let mut hasher = SeaHasher::new();
        self.1.hash(&mut hasher);
        hasher.finish()
    }

    fn exec(&self, db: &mut Database) -> Result<resp::Msg, Error> {
         // TODO remove other args
        match db.remove(&self.1) {
            Some(_) => {
                db.shrink_to_fit();
                Ok(resp::Msg::Int(1))
            },
            None => Ok(resp::Msg::Int(0)),
        }
    }

    fn to_command(self) -> Command {
        Command::Del(self)
    }
}
