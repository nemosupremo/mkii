use super::{resp, Args, Command, Database, Error, Execute};
use bytes::Bytes;

pub struct Ping(pub Bytes);
pub struct Echo(pub Bytes);

impl Execute for Ping {
    fn parse(mut args: Args) -> Result<Self, Error> {
        match args.len() {
            1 => Ok(Ping(Bytes::with_capacity(0))),
            2 => match args.own(1) {
                resp::Msg::String(key) => Ok(Ping(key)),
                resp::Msg::BulkString(Some(key)) => Ok(Ping(key)),
                _ => Err(Error::Err("invalid parameter for 'ping' command")),
            },
            _ => Err(Error::Err("wrong number of arguments for 'ping' command")),
        }
    }

    fn shard(&self) -> u64 {
        std::u64::MAX
    }

    fn exec(&self, _db: &mut Database) -> Result<resp::Msg, Error> {
        /*
            TODO: If the client is subscribed to a channel or a pattern,
            it will instead return a multi-bulk with a "pong" in the first
            position and an empty bulk in the second position,
            unless an argument is provided in which case it returns a
            copy of the argument.
        */
        if self.0.len() == 0 {
            Ok(resp::Msg::Str("PONG"))
        } else {
            // TODO: We must also check that the utf-8 code does not contain
            // a \r\n
            match std::str::from_utf8(&self.0) {
                Ok(_) => Ok(resp::Msg::String(self.0.clone())),
                Err(_) => Ok(resp::Msg::BulkString(Some(self.0.clone()))),
            }
        }
    }

    fn to_command(self) -> Command {
        Command::Ping(self)
    }
}

impl Execute for Echo {
    fn parse(mut args: Args) -> Result<Self, Error> {
        match args.len() {
            2 => match args.own(1) {
                resp::Msg::String(key) => Ok(Echo(key)),
                resp::Msg::BulkString(Some(key)) => Ok(Echo(key)),
                _ => Err(Error::Err("invalid parameter for 'echo' command")),
            },
            _ => Err(Error::Err("wrong number of arguments for 'echo' command")),
        }
    }

    fn shard(&self) -> u64 {
        std::u64::MAX
    }

    fn exec(&self, _db: &mut Database) -> Result<resp::Msg, Error> {
        Ok(resp::Msg::BulkString(Some(self.0.clone())))
    }

    fn to_command(self) -> Command {
        Command::Echo(self)
    }
}
