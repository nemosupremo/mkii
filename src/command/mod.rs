mod connection;
mod index;
mod keys;
mod string;

use std::error;
use std::fmt;
use std::mem;

pub use self::index::{Command, COMMANDS};
use super::database::{self, Database};
use super::resp;
use bytes::Bytes;

pub trait Execute: Send + Sync {
    fn parse(args: Args) -> Result<Self, Error>
    where
        Self: Sized;
    fn exec(&self, db: &mut Database) -> Result<resp::Msg, Error>;
    fn shard(&self) -> u64;
    fn to_command(self) -> index::Command;

    fn new(args: Args) -> Result<index::Command, Error>
    where
        Self: 'static + Sized,
    {
        match Self::parse(args) {
            Ok(c) => Ok(c.to_command()),
            Err(e) => Err(e),
        }
    }
}

pub struct Args(pub Vec<resp::Msg>);

// http://xion.io/post/code/rust-move-out-of-container.html
impl Args {
    pub fn own(&mut self, i: usize) -> resp::Msg {
        mem::replace(&mut self.0[i], resp::Msg::None)
    }
}

// https://stackoverflow.com/a/32552688/807701
impl std::ops::Deref for Args {
    type Target = [resp::Msg];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct Unimplemented(pub Bytes);

impl Execute for Unimplemented {
    fn parse(mut args: Args) -> Result<Self, Error> {
        match args.own(0) {
            resp::Msg::String(key) => Ok(Unimplemented(key)),
            resp::Msg::BulkString(Some(key)) => Ok(Unimplemented(key)),
            _ => Err(Error::Err("invalid parameter for command")),
        }
    }

    fn shard(&self) -> u64 {
        std::u64::MAX
    }

    fn exec(&self, _db: &mut Database) -> Result<resp::Msg, Error> {
        Ok(resp::Msg::Error(format!(
            "NOIMPL Command '{}' is not implmented",
            match std::str::from_utf8(self.0.as_ref()) {
                Ok(s) => s,
                Err(_) => "_invalid",
            }
        )))
    }

    fn to_command(self) -> Command {
        Command::Unimplemented(self)
    }
}

pub struct Quit;

impl Execute for Quit {
    fn parse(mut _args: Args) -> Result<Self, Error> {
        Ok(Quit)
    }

    fn shard(&self) -> u64 {
        std::u64::MAX
    }

    fn exec(&self, _db: &mut Database) -> Result<resp::Msg, Error> {
        Err(Error::Quit)
    }

    fn to_command(self) -> Command {
        Command::Quit(self)
    }
}

#[derive(Debug, Clone)]
pub enum Error {
    Err(&'static str),
    Error(String),
    WrongType,
    Quit,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::WrongType => write!(
                f,
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            ),
            Error::Err(s) => write!(f, "ERR {}", s),
            Error::Error(s) => write!(f, "ERR {}", s),
            Error::Quit => write!(f, "QUIT"),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match self {
            Error::WrongType => "Operation against a key holding the wrong kind of value",
            Error::Err(s) => s,
            Error::Error(s) => s.as_ref(),
            Error::Quit => "QUIT",
        }
    }

    fn cause(&self) -> Option<&dyn error::Error> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}
