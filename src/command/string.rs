use bytes::{BufMut, Bytes, BytesMut};
use seahash::SeaHasher;
use std::hash::{Hash, Hasher};
use std::str;
use std::time;
use byteorder::{BigEndian, WriteBytesExt};

use super::{database, database::Scalar, database::Value as DBValue, resp, Args, Command, Database, Error, Execute};

pub struct Get(Bytes); //
pub struct Set(Bytes, Bytes, time::Duration, SetOpt);
pub enum SetOpt {
    None,
    NX,
    XX,
}
pub struct Append(Bytes, Bytes);
pub struct Strlen(Bytes);
pub struct Setbit(Bytes, u64, bool); // key offset value
pub struct Getbit(Bytes, u64); // key offset
pub struct Bitfield(Bytes, Vec<BitfieldCommand>);
#[derive(Copy, Clone)]
pub enum BitfieldOverflow {
    Wrap,
    Sat,
    Fail,
}
#[derive(Copy, Clone)]
pub enum BitfieldType {
    Signed(i64),
    Unsigned(u64),
}
#[derive(Copy, Clone)]
pub enum BitfieldCommand {
    Get(BitfieldType, u64),
    Set(BitfieldType, u64, i64),
    IncrBy(BitfieldType, u64, i64),
    Overflow(BitfieldOverflow),
}
pub struct Incr(Bytes, i64); // key offset
pub struct GetRange(Bytes, i64, i64); // key, start, end
pub struct SetRange(Bytes, u64, Bytes); // key, offset, value

impl Execute for Get {
    fn parse(mut args: Args) -> Result<Self, Error> {
        if args.len() != 2 {
            return Err(Error::Err("wrong number of arguments for 'get' command"));
        }
        match args.own(1) {
            resp::Msg::String(key) => Ok(Get(key)),
            resp::Msg::BulkString(Some(key)) => Ok(Get(key)),
            _ => Err(Error::Err("invalid parameter for 'get' command")),
        }
    }

    fn shard(&self) -> u64 {
        let mut hasher = SeaHasher::new();
        self.0.hash(&mut hasher);
        hasher.finish()
    }

    fn exec(&self, db: &mut Database) -> Result<resp::Msg, Error> {
        match db.get(&self.0) {
            Some(val) => match val {
                DBValue::Scalar(Scalar::String(s)) => Ok(resp::Msg::BulkString(Some(s.clone()))),
                DBValue::Scalar(Scalar::Integer(i)) => Ok(resp::Msg::Int(*i)),
                _ => Err(Error::WrongType),
            },
            None => Ok(resp::Msg::BulkString(None)),
        }
    }

    fn to_command(self) -> Command {
        Command::Get(self)
    }
}

impl Execute for Set {
    fn parse(mut args: Args) -> Result<Self, Error> {
        let mut cmd = Set(
            Bytes::with_capacity(0),
            Bytes::with_capacity(0),
            time::Duration::from_secs(0),
            SetOpt::None,
        );
        cmd.0 = match args.own(1) {
            resp::Msg::String(key) => key,
            resp::Msg::BulkString(Some(key)) => key,
            _ => return Err(Error::Err("invalid parameter for 'set' command")),
        };

        let cmd_name = match args.own(0) {
            resp::Msg::String(key) => key,
            resp::Msg::BulkString(Some(key)) => key,
            _ => return Err(Error::Err("invalid parameter for 'set' command")),
        };
        match cmd_name.as_ref() {
            b"SET" => {
                if args.len() < 3 {
                    return Err(Error::Err("wrong number of arguments for 'set' command"));
                }
                cmd.1 = match args.own(2) {
                    resp::Msg::String(key) => key,
                    resp::Msg::BulkString(Some(key)) => key,
                    _ => return Err(Error::Err("invalid parameter for 'set' command")),
                };
                let mut iter = 3..args.len();
                while let Some(i) = iter.next() {
                    match &args[i] {
                        resp::Msg::String(name) | resp::Msg::BulkString(Some(name)) => {
                            match name.as_ref() {
                                b"EX" | b"ex" | b"PX" | b"px" => {
                                    if i + 1 < args.len() {
                                        let n =
                                            match &args[i + 1] {
                                                resp::Msg::BulkString(Some(key))
                                                | resp::Msg::String(key) => {
                                                    match str::from_utf8(key.as_ref()) {
                                                        Ok(k) => k.parse::<u64>(),
                                                        Err(_) => return Err(Error::Err(
                                                            "invalid parameter for 'set' command",
                                                        )),
                                                    }
                                                }
                                                resp::Msg::Int(n) => Ok(*n as u64),
                                                _ => {
                                                    return Err(Error::Err(
                                                        "invalid parameter for 'set' command",
                                                    ))
                                                }
                                            };
                                        if let Ok(n) = n {
                                            if name.as_ref() == b"EX" {
                                                cmd.2 = time::Duration::from_secs(n)
                                            } else {
                                                cmd.2 = time::Duration::from_millis(n)
                                            }
                                        }
                                        iter.next();
                                    } else {
                                        return Err(Error::Err("syntax error."));
                                    }
                                }
                                b"NX" | b"nx" => cmd.3 = SetOpt::NX,
                                b"XX" | b"xx" => cmd.3 = SetOpt::XX,
                                _ => return Err(Error::Err("syntax error..")),
                            };
                        }
                        _ => return Err(Error::Err("invalid parameter for 'set' command")),
                    };
                }
            }
            b"SETNX" => {
                cmd.1 = match args.own(2) {
                    resp::Msg::String(key) => key,
                    resp::Msg::BulkString(Some(key)) => key,
                    _ => return Err(Error::Err("invalid parameter for 'set' command")),
                };
                cmd.3 = SetOpt::NX
            }
            b"SETEX" | b"PSETEX" => {
                cmd.1 = match args.own(3) {
                    resp::Msg::String(key) => key,
                    resp::Msg::BulkString(Some(key)) => key,
                    _ => return Err(Error::Err("invalid parameter for 'set' command")),
                };
                let n = match args.own(2) {
                    resp::Msg::BulkString(Some(key)) | resp::Msg::String(key) => {
                        match str::from_utf8(key.as_ref()) {
                            Ok(k) => k.parse::<u64>(),
                            Err(_) => return Err(Error::Err("invalid parameter for 'set' command")),
                        }
                    }
                    resp::Msg::Int(n) => Ok(n as u64),
                    _ => return Err(Error::Err("invalid parameter for 'set' command")),
                };
                if let Ok(n) = n {
                    if cmd_name.as_ref() == b"EX" {
                        cmd.2 = time::Duration::from_secs(n)
                    } else {
                        cmd.2 = time::Duration::from_millis(n)
                    }
                }
            }
            _ => return Err(Error::Err("invalid name for 'set' command")),
        }
        return Ok(cmd);
    }

    fn shard(&self) -> u64 {
        let mut hasher = SeaHasher::new();
        self.0.hash(&mut hasher);
        hasher.finish()
    }

    fn exec(&self, db: &mut Database) -> Result<resp::Msg, Error> {
        match self.3 {
            SetOpt::NX => {
                if db.contains_key(&self.0) {
                    return Ok(resp::Msg::BulkString(None));
                }
            }
            SetOpt::XX => {
                if !db.contains_key(&self.0) {
                    return Ok(resp::Msg::BulkString(None));
                }
            }
            _ => {}
        };

        // db.insert(self.0.clone(), DBValue::Scalar(Scalar::String(Bytes::from(self.1.as_ref()))));
        db.insert(self.0.clone(), DBValue::Scalar(Scalar::String(self.1.clone())));
        Ok(resp::Msg::Str("OK"))
    }

    fn to_command(self) -> Command {
        Command::Set(self)
    }
}

impl Execute for Append {
    fn parse(mut args: Args) -> Result<Self, Error> {
        if args.len() != 3 {
            return Err(Error::Err("wrong number of arguments for 'append' command"));
        }
        let mut cmd = Append(Bytes::with_capacity(0), Bytes::with_capacity(0));

        cmd.0 = match args.own(1) {
            resp::Msg::BulkString(Some(key)) | resp::Msg::String(key) => key,
            _ => return Err(Error::Err("invalid parameter for 'append' command")),
        };
        cmd.1 = match args.own(2) {
            resp::Msg::BulkString(Some(key)) | resp::Msg::String(key) => key,
            _ => return Err(Error::Err("invalid parameter for 'append' command")),
        };
        Ok(cmd)
    }

    fn shard(&self) -> u64 {
        let mut hasher = SeaHasher::new();
        self.0.hash(&mut hasher);
        hasher.finish()
    }

    fn exec(&self, db: &mut Database) -> Result<resp::Msg, Error> {
        let s = match db.get(&self.0) {
            Some(val) => match val {
                DBValue::Scalar(Scalar::Integer(i)) => Bytes::from(i.to_string()),
                DBValue::Scalar(Scalar::String(s)) => s.clone(),
                _ => return Err(Error::WrongType),
            },
            None => {
                db.insert(self.0.clone(), DBValue::Scalar(Scalar::String(self.1.clone())));
                return Ok(resp::Msg::Int(self.0.len() as i64));
            }
        };

        let new_sz = s.len() + self.0.len();
        let mut nv = BytesMut::with_capacity(new_sz);
        nv.put(s);
        nv.put(self.1.as_ref());
        db.insert(self.0.clone(), DBValue::Scalar(Scalar::String(nv.freeze())));

        Ok(resp::Msg::Int(new_sz as i64))
    }

    fn to_command(self) -> Command {
        Command::Append(self)
    }
}

impl Execute for Strlen {
    fn parse(mut args: Args) -> Result<Self, Error> {
        if args.len() != 2 {
            return Err(Error::Err("wrong number of arguments for 'strlen' command"));
        }
        match args.own(1) {
            resp::Msg::String(key) => Ok(Strlen(key)),
            resp::Msg::BulkString(Some(key)) => Ok(Strlen(key)),
            _ => Err(Error::Err("invalid parameter for 'strlen' command")),
        }
    }

    fn shard(&self) -> u64 {
        let mut hasher = SeaHasher::new();
        self.0.hash(&mut hasher);
        hasher.finish()
    }

    fn exec(&self, db: &mut Database) -> Result<resp::Msg, Error> {
        match db.get(&self.0) {
            Some(val) => match val {
                DBValue::Scalar(Scalar::String(s)) => Ok(resp::Msg::Int(s.len() as i64)),
                DBValue::Scalar(Scalar::Integer(i)) => Ok(resp::Msg::Int(i.to_string().len() as i64)),
                _ => Err(Error::WrongType),
            },
            None => Ok(resp::Msg::Int(0)),
        }
    }

    fn to_command(self) -> Command {
        Command::Strlen(self)
    }
}

impl Execute for Setbit {
    fn parse(mut args: Args) -> Result<Self, Error> {
        if args.len() != 4 {
            return Err(Error::Err("wrong number of arguments for 'setbit' command"));
        }
        let mut cmd = Setbit(Bytes::with_capacity(0), 0, false);

        cmd.0 = match args.own(1) {
            resp::Msg::String(key) => key,
            resp::Msg::BulkString(Some(key)) => key,
            _ => return Err(Error::Err("invalid parameter for 'setbit' command")),
        };
        cmd.1 = match args.own(2) {
            resp::Msg::BulkString(Some(key)) | resp::Msg::String(key) => {
                match str::from_utf8(key.as_ref()) {
                    Ok(k) => if let Ok(i) = k.parse::<u64>() {
                        i
                    } else {
                        return Err(Error::Err("invalid parameter for 'setbit' command"));
                    },
                    Err(_) => return Err(Error::Err("invalid parameter for 'setbit' command")),
                }
            }
            resp::Msg::Int(i) => i as u64,
            _ => return Err(Error::Err("invalid parameter for 'setbit' command")),
        };
        cmd.2 = match args.own(3) {
            resp::Msg::BulkString(Some(key)) | resp::Msg::String(key) => {
                match str::from_utf8(key.as_ref()) {
                    Ok(k) => if let Ok(i) = k.parse::<u8>() {
                        i != 0
                    } else {
                        return Err(Error::Err("invalid parameter for 'setbit' command"));
                    },
                    Err(_) => return Err(Error::Err("invalid parameter for 'setbit' command")),
                }
            }
            resp::Msg::Int(i) => i != 0,
            _ => return Err(Error::Err("invalid parameter for 'setbit' command")),
        };

        Ok(cmd)
    }

    fn shard(&self) -> u64 {
        let mut hasher = SeaHasher::new();
        self.0.hash(&mut hasher);
        hasher.finish()
    }

    fn exec(&self, db: &mut Database) -> Result<resp::Msg, Error> {
        let byte_offset = (self.1 / 8) as usize;
        let bit_offset = (self.1 % 8) as usize;
        match db.remove_entry(&self.0) {
            Some((k, val)) => {
                let mut buff = match val {
                    DBValue::Scalar(Scalar::String(s)) => {
                        // Try to grab a mutable handle to the buffer. try_mut
                        // will usually fail if the handle in the db isn't
                        // exclusively owned by the db - which is likely because
                        // the handle in the db is a reference to a slice
                        // that was allocated from the network request (ex.
                        // a SET command).
                        s.try_mut().unwrap_or_else(|s| BytesMut::from(&s[..]))
                    }
                    DBValue::Scalar(Scalar::Integer(i)) => BytesMut::from(i.to_string()),
                    _ => return Err(Error::WrongType),
                };
                let buff_sz = (self.1 / 8 + 1) as usize;
                if buff.capacity() - buff_sz > 0 {
                    buff.reserve(buff.capacity() - buff_sz as usize);
                }
                unsafe {
                    buff.set_len(buff_sz);
                };

                let byte = buff[byte_offset];
                let curr_value = (byte >> bit_offset) & 1;
                if self.2 {
                    buff[byte_offset] |= 1 << bit_offset;
                } else {
                    buff[byte_offset] &= !(1 << bit_offset)
                }
                db.insert(k, DBValue::Scalar(Scalar::String(buff.freeze())));
                Ok(resp::Msg::Int(curr_value as i64))
            }
            None => {
                if self.2 {
                    let buff_sz = (self.1 / 8 + 1) as usize;
                    let mut buff = BytesMut::with_capacity(buff_sz);
                    unsafe {
                        buff.set_len(buff_sz);
                    };
                    buff[byte_offset] |= 1 << bit_offset;
                    db.insert(self.0.clone(), DBValue::Scalar(Scalar::String(buff.freeze())));
                }

                Ok(resp::Msg::Int(0))
            }
        }
    }

    fn to_command(self) -> Command {
        Command::Setbit(self)
    }
}

impl Execute for Getbit {
    fn parse(mut args: Args) -> Result<Self, Error> {
        if args.len() != 3 {
            return Err(Error::Err("wrong number of arguments for 'getbit' command"));
        }
        let mut cmd = Getbit(Bytes::with_capacity(0), 0);

        cmd.0 = match args.own(1) {
            resp::Msg::String(key) => key,
            resp::Msg::BulkString(Some(key)) => key,
            _ => return Err(Error::Err("invalid parameter for 'getbit' command")),
        };
        cmd.1 = match args.own(2) {
            resp::Msg::BulkString(Some(key)) | resp::Msg::String(key) => {
                match str::from_utf8(key.as_ref()) {
                    Ok(k) => if let Ok(i) = k.parse::<u64>() {
                        i
                    } else {
                        return Err(Error::Err("invalid parameter for 'getbit' command"));
                    },
                    Err(_) => return Err(Error::Err("invalid parameter for 'getbit' command")),
                }
            }
            resp::Msg::Int(i) => i as u64,
            _ => return Err(Error::Err("invalid parameter for 'getbit' command")),
        };

        Ok(cmd)
    }

    fn shard(&self) -> u64 {
        let mut hasher = SeaHasher::new();
        self.0.hash(&mut hasher);
        hasher.finish()
    }

    fn exec(&self, db: &mut Database) -> Result<resp::Msg, Error> {
        let byte_offset = (self.1 / 8) as usize;
        let bit_offset = (self.1 % 8) as usize;
        match db.get(&self.0) {
            Some(val) => {
                let byte = match val {
                    DBValue::Scalar(Scalar::String(s)) => {
                        if byte_offset < s.len() {
                            s[byte_offset]
                        } else {
                            0
                        }
                    }
                    DBValue::Scalar(Scalar::Integer(i)) => {
                        let s = &Bytes::from(i.to_string());
                        if byte_offset < s.len() {
                            s[byte_offset]
                        } else {
                            0
                        }
                    }
                    _ => return Err(Error::WrongType),
                };

                let curr_value = (byte >> bit_offset) & 1;
                Ok(resp::Msg::Int(curr_value as i64))
            }
            None => Ok(resp::Msg::Int(0)),
        }
    }

    fn to_command(self) -> Command {
        Command::Getbit(self)
    }
}

impl Bitfield {
    fn parse_type(resp_arg: Option<resp::Msg>) -> Option<BitfieldType> {
        match resp_arg {
            Some(resp::Msg::String(arg)) | Some(resp::Msg::BulkString(Some(arg))) => {
                if arg.len() < 2 {
                    return None;
                }
                let arg_s = match str::from_utf8(arg.as_ref()) {
                    Ok(s) => s,
                    Err(_) => return None,
                };
                let bit_sz = match arg_s[1..].parse::<u64>() {
                    Ok(i) => i,
                    Err(_) => return None,
                };
                if bit_sz == 0 || bit_sz > 64 {
                    return None;
                }
                match arg[0] {
                    b'i' => Some(BitfieldType::Signed(bit_sz as i64)),
                    b'u' => {
                        if bit_sz == 64 {
                            None
                        } else {
                            Some(BitfieldType::Unsigned(bit_sz))
                        }
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    fn read_field(arr: &[u8], t: BitfieldType, offset: usize) -> BitfieldType {
        if offset > (arr.len()) {
            println!("offset len was greater than array len {:} {:}", arr.len(), offset);
            match t {
                BitfieldType::Unsigned(_) => {
                    BitfieldType::Unsigned(0)
                },
                BitfieldType::Signed(_) => {
                    BitfieldType::Signed(0)
                },
            }
        } else {
            let o = offset/8;
            match t {
                BitfieldType::Unsigned(i_sz) => {
                    let sz = ((i_sz+7)/8) as usize;
                    let upper = std::cmp::min(o+sz, arr.len());
                    let bit_array = &arr[o..(upper)];

                    let mut j = 0 as u64;
                    let bo = (offset % 8) as u64;
                    println!("offset: {} o: {}", offset, o);
                    // final but pos
                    let fbp = i_sz+bo;
                    for i in (0+bo..fbp).rev() {
                        let byte = bit_array[(i / 8) as usize];
                        let bit = (byte >> (8 - (fbp - i))) & 1;
                        j += (bit << (i)) as u64;
                    }
                    BitfieldType::Unsigned(j)
                },
                BitfieldType::Signed(i_sz) => {
                    let sz = ((i_sz+7)/8) as usize;
                    let upper = std::cmp::min(o+sz, arr.len());
                    let bit_array = &arr[o..(upper)];

                    let mut j = 0 as u64;
                    let mut m = 1;
                    let bo = (offset % 8) as i64;
                    println!("offset: {} o: {}", offset, o);
                    // final but pos
                    let fbp = i_sz+bo;
                    for i in (0+bo..fbp).rev() {
                        let byte = bit_array[(i / 8) as usize];
                        // the xor is to flip the bit if we are 2's complement
                        let bit = (byte >> (8 - (fbp - i))) & 1 ^ ((m == -1) as u8);

                        // check for 2's complement
                        if i == (fbp-1) && bit == 1 {
                            m = -1;
                        } else {
                            j += (bit << (i)) as u64;
                        }
                    }
                    if m == -1 {
                        j += 1;
                    }

                    BitfieldType::Signed(m*(j as i64))
                }
            }
        }
    }
}

// TODO: The implementation for Bitfield is not finished.
impl Execute for Bitfield {
    fn parse(mut args: Args) -> Result<Self, Error> {
        let mut cmd = Bitfield(Bytes::new(), Vec::new());
        cmd.0 = match args.own(1) {
            resp::Msg::String(key) | resp::Msg::BulkString(Some(key)) => key,
            _ => return Err(Error::Err("invalid parameter for 'bitfield' command 1")),
        };

        let arg_iter = &mut args.0.drain(2..);
        while let Some(option) = arg_iter.next() {
            let subcommand = match option {
                resp::Msg::String(key) | resp::Msg::BulkString(Some(key)) => match key.as_ref() {
                    b"GET" | b"get" => BitfieldCommand::Get(BitfieldType::Signed(0), 0),
                    b"SET" | b"set" => BitfieldCommand::Set(BitfieldType::Signed(0), 0, 0),
                    b"INCRBY" | b"incrby" => BitfieldCommand::IncrBy(BitfieldType::Signed(0), 0, 0),
                    b"OVERFLOW" | b"overflow" => BitfieldCommand::Overflow(BitfieldOverflow::Wrap),
                    _ => return Err(Error::Err("invalid parameter for 'bitfield' command 2")),
                },
                _ => return Err(Error::Err("invalid parameter for 'bitfield' command 3")),
            };

            let (field_type, field_offset) = match subcommand {
                BitfieldCommand::Overflow(_) => (BitfieldType::Signed(0), 0),
                _ => {
                    let field_type = match Bitfield::parse_type(arg_iter.next()) {
                        Some(f) => f,
                        None => return Err(Error::Err("invalid parameter for 'bitfield' command 4")),
                    };
                    let field_offset = match arg_iter.next() {
                        Some(resp::Msg::String(key)) | Some(resp::Msg::BulkString(Some(key))) => {
                            match str::from_utf8(key.as_ref()) {
                                Ok(k) => if let Ok(i) = k.parse::<u64>() {
                                    i
                                } else {
                                    return Err(Error::Err(
                                        "invalid parameter for 'bitfield' command 5",
                                    ));
                                },
                                Err(_) => {
                                    return Err(Error::Err(
                                        "invalid parameter for 'bitfield' command 6",
                                    ))
                                }
                            }
                        }
                        Some(resp::Msg::Int(i)) => i as u64,
                        _ => return Err(Error::Err("invalid parameter for 'bitfield' command 7")),
                    };
                    (field_type, field_offset)
                }
            };

            let parsed_command = match subcommand {
                BitfieldCommand::Get(_, _) => BitfieldCommand::Get(field_type, field_offset),
                BitfieldCommand::Set(_, _, _) | BitfieldCommand::IncrBy(_, _, _) => {
                    let field_value = match arg_iter.next() {
                        Some(resp::Msg::String(key)) | Some(resp::Msg::BulkString(Some(key))) => {
                            match str::from_utf8(key.as_ref()) {
                                Ok(k) => if let Ok(i) = k.parse::<i64>() {
                                    i
                                } else {
                                    return Err(Error::Err(
                                        "invalid parameter for 'bitfield' command 8",
                                    ));
                                },
                                Err(_) => {
                                    return Err(Error::Err(
                                        "invalid parameter for 'bitfield' command 9",
                                    ))
                                }
                            }
                        }
                        Some(resp::Msg::Int(i)) => i as i64,
                        _ => return Err(Error::Err("invalid parameter for 'bitfield' command 10")),
                    };
                    if let BitfieldCommand::Set(_, _, _) = subcommand {
                        BitfieldCommand::Set(field_type, field_offset, field_value)
                    } else {
                        if field_value == 0 {
                            BitfieldCommand::Get(field_type, field_offset)
                        } else {
                            BitfieldCommand::IncrBy(field_type, field_offset, field_value)
                        }
                    }
                }
                BitfieldCommand::Overflow(_) => {
                    let value = match arg_iter.next() {
                        Some(resp::Msg::String(key)) | Some(resp::Msg::BulkString(Some(key))) => {
                            key
                        }
                        _ => return Err(Error::Err("invalid parameter for 'bitfield' command 11")),
                    };
                    match value.as_ref() {
                        b"WRAP" | b"wrap" => BitfieldCommand::Overflow(BitfieldOverflow::Wrap),
                        b"SAT" | b"sat" => BitfieldCommand::Overflow(BitfieldOverflow::Sat),
                        b"FAIL" | b"fail" => BitfieldCommand::Overflow(BitfieldOverflow::Fail),
                        _ => return Err(Error::Err("invalid parameter for 'bitfield' command 12")),
                    }
                }
            };
            cmd.1.push(parsed_command)
        }
        if cmd.1.len() == 0 {
            return Err(Error::Err("invalid parameter for 'bitfield' command 13"));
        }

        Ok(cmd)
    }

    fn shard(&self) -> u64 {
        let mut hasher = SeaHasher::new();
        self.0.hash(&mut hasher);
        hasher.finish()
    }

    fn exec(&self, db: &mut Database) -> Result<resp::Msg, Error> {
        let mut tbuf = [0 as u8; 8];
        // let mut stored_buffer = None;
        // let mut new_stored_buffer = BytesMut::with_capacity(0);
        let mut has_stored = false;

        let mut arr = match db.remove_entry(&self.0) {
            Some((k, val)) => match val {
                DBValue::Scalar(Scalar::String(s)) => {
                    //let mut s2 = s.try_mut().unwrap();
                    // stored_buffer = Some(BytesMut::with_capacity(0));
                    has_stored = true;
                    (k, s.try_mut().unwrap_or_else(|s| BytesMut::from(&s[..])))
                },
                DBValue::Scalar(Scalar::Integer(i)) => {
                    (&mut tbuf as &mut[u8]).write_i64::<BigEndian>(i as i64).unwrap();
                    (k, BytesMut::from(&mut tbuf as &[u8]))
                },
                _ => return Err(Error::WrongType),
            },
            None => (self.0.clone(), BytesMut::from(&mut tbuf as &[u8])),
        };
        // let arr = &arrt.1;

        let mut overflow_opt = BitfieldOverflow::Wrap;
        let mut resp = Vec::new();
        // let mut first_mod = true;
        for command in self.1.iter() {
            match command {
                BitfieldCommand::Overflow(o) => {
                    overflow_opt = *o;
                },
                BitfieldCommand::Get(t, o64) => {
                    let val = Bitfield::read_field(&arr.1.as_ref(), *t, *o64 as usize);
                    match val {
                        BitfieldType::Unsigned(v) => resp.push(resp::Msg::Int(v as i64)),
                        BitfieldType::Signed(v) => resp.push(resp::Msg::Int(v as i64)),
                    }
                },
                BitfieldCommand::Set(t, o64, value) => {
                    has_stored = true;
                    let old_val = Bitfield::read_field(&arr.1.as_ref(), *t, *o64 as usize);
                    match old_val {
                        BitfieldType::Unsigned(v) => resp.push(resp::Msg::Int(v as i64)),
                        BitfieldType::Signed(v) => resp.push(resp::Msg::Int(v as i64)),
                    };

                    let _min_size = (match t {
                        BitfieldType::Unsigned(t) => *t as usize,
                        BitfieldType::Signed(t) => *t as usize,
                    }+7)/8 + (*o64 as usize)+7/8;

                    let mut offset = *o64;
                    let bits = match t {
                        BitfieldType::Unsigned(t) => *t,
                        BitfieldType::Signed(t) => *t as u64,
                    };
                    //for (j = 0; j < bits; j++) {
                    for j in 0..bits {
                        let bitval = (((*value as u64) & ((1 as u64)<<(bits-1-j))) != 0) as u8;
                        let byte = offset >> 3;
                        let bit = 7 - (offset & 0x7);
                        let mut byteval = *arr.1.get(byte as usize).unwrap(); // p[byte];
                        byteval &= !(1 << bit);
                        byteval |= bitval << bit;
                        // p[byte] = byteval & 0xff;
                        arr.1[byte as usize] = byteval & 0xff;
                        println!("Setting pos {:?} to {:?}", byte, byteval & 0xff);
                        offset += 1;
                    }
                    println!("{:?}", arr.1);
                },
                BitfieldCommand::IncrBy(t, o64, value) => {
                    has_stored = true;
                    let old_val = Bitfield::read_field(&arr.1.as_ref(), *t, *o64 as usize);
                    let curr_val = match old_val {
                        BitfieldType::Unsigned(v) => v as u64,
                        BitfieldType::Signed(v) => v as u64
                    };
                    let (bits, signed) = match t {
                        BitfieldType::Unsigned(v) => (*v, false),
                        BitfieldType::Signed(v) => (*v as u64, true),
                    };
                    let mut new_val = 0;
                    let mut push_null = false;
                    match overflow_opt {
                        BitfieldOverflow::Wrap => {
                            if *value > 0 {
                                new_val = curr_val.wrapping_add(*value as u64);
                            } else {
                                new_val = curr_val.wrapping_sub((-1*value) as u64);
                            }
                        },
                        BitfieldOverflow::Sat => {
                            if *value > 0 {
                                new_val = curr_val.saturating_add(*value as u64);
                                if bits < 64 {
                                    if signed && new_val >= ((1 << bits)/2) {
                                        new_val = ((1 << bits)/2) - 1
                                    } else if !signed && new_val >= (1 << bits) {
                                        new_val = (1 << bits) - 1;
                                    }
                                }
                            } else {
                                new_val = curr_val.saturating_sub((-1*value) as u64);
                                if bits < 64 {
                                    if signed && (new_val as i64) <= !0 >> (64 - bits) {
                                        new_val = !0 >> (64 - bits);
                                    }
                                }
                            }
                        },
                        BitfieldOverflow::Fail => {
                            if *value > 0 {
                                if let Some(checked_new_val) = curr_val.checked_add(*value as u64) {
                                    new_val = checked_new_val;
                                    if bits < 64 {
                                        if signed && new_val >= ((1 << bits)/2) {
                                            push_null = true;
                                        } else if !signed && new_val >= (1 << bits) {
                                            push_null = true;
                                        }
                                    }
                                } else {
                                    push_null = true;
                                }
                            } else {
                                if let Some(checked_new_val) = curr_val.checked_sub((-1*value) as u64) {
                                    new_val = checked_new_val;
                                    if bits < 64 {
                                        if signed && (new_val as i64) <= !0 >> (64 - bits) {
                                            push_null = true;
                                        }
                                    }
                                } else {
                                    push_null = true;
                                }
                            }

                        },
                    }

                    if push_null {
                        resp.push(resp::Msg::BulkString(None));
                    } else {
                        let mut offset = *o64;
                        for j in 0..bits {
                            let bitval = (((new_val as u64) & ((1 as u64)<<(bits-1-j))) != 0) as u8;
                            let byte = offset >> 3;
                            let bit = 7 - (offset & 0x7);
                            let mut byteval = *arr.1.get(byte as usize).unwrap(); // p[byte];
                            byteval &= !(1 << bit);
                            byteval |= bitval << bit;
                            // p[byte] = byteval & 0xff;
                            arr.1[byte as usize] = byteval & 0xff;
                            //println!("Setting pos {:?} to {:?}", byte, byteval & 0xff);
                            offset += 1;
                        }

                        match Bitfield::read_field(&arr.1.as_ref(), *t, *o64 as usize) {
                            BitfieldType::Unsigned(v) => resp.push(resp::Msg::Int(v as i64)),
                            BitfieldType::Signed(v) => resp.push(resp::Msg::Int(v as i64))
                        };
                    }

                }
            }
        }

        if has_stored {
            println!("sz: {:?}; into {:?}", arr.1.len(), &arr.0);
            db.insert(arr.0, DBValue::Scalar(Scalar::String(arr.1.freeze())));
        }

        return Ok(resp::Msg::Array(Some(resp)))
    }

    fn to_command(self) -> Command {
        Command::Bitfield(self)
    }
}

impl Execute for Incr {
    fn parse(mut args: Args) -> Result<Self, Error> {
        if args.len() < 2 {
            return Err(Error::Err("wrong number of arguments for 'incr' command"));
        }
        let mut cmd = match args.own(1) {
            resp::Msg::String(key) | resp::Msg::BulkString(Some(key)) => Incr(key, 1),
            _ => return Err(Error::Err("invalid parameter for 'incr' command")),
        };
        if args.len() == 3 {
            cmd.1 = match args.own(2) {
                resp::Msg::BulkString(Some(key)) | resp::Msg::String(key) => {
                    match str::from_utf8(key.as_ref()) {
                        Ok(k) => if let Ok(i) = k.parse::<i64>() {
                            i
                        } else {
                            return Err(Error::Err("invalid parameter for 'incrby' command"));
                        },
                        Err(_) => return Err(Error::Err("invalid parameter for 'incrby' command")),
                    }
                }
                resp::Msg::Int(i) => i as i64,
                _ => return Err(Error::Err("invalid parameter for 'incrby' command")),
            };
        } else if args.len() > 3 {
            return Err(Error::Err("invalid parameter for 'incr' command"));
        }
        match args.own(0) {
            resp::Msg::String(key) | resp::Msg::BulkString(Some(key)) => match key.as_ref() {
                b"DECR" | b"DECRBY" => cmd.1 *= -1,
                _ => {}
            },
            _ => return Err(Error::Err("invalid parameter for 'incr' command")),
        };

        Ok(cmd)
    }

    fn shard(&self) -> u64 {
        let mut hasher = SeaHasher::new();
        self.0.hash(&mut hasher);
        hasher.finish()
    }

    fn exec(&self, db: &mut Database) -> Result<resp::Msg, Error> {
        match db.remove_entry(&self.0) {
            Some((k, val)) => {
                let val = match val {
                    DBValue::Scalar(Scalar::String(s)) => match str::from_utf8(s.as_ref()) {
                        Ok(k) => if let Ok(i) = k.parse::<i64>() {
                            i
                        } else {
                            return Err(Error::WrongType);
                        },
                        Err(_) => return Err(Error::WrongType),
                    },
                    DBValue::Scalar(Scalar::Integer(i)) => i,
                    _ => return Err(Error::WrongType),
                };
                let new_val = val + self.1;

                db.insert(k, DBValue::Scalar(Scalar::Integer(new_val)));
                Ok(resp::Msg::Int(new_val))
            }
            None => {
                db.insert(self.0.clone(), DBValue::Scalar(Scalar::Integer(self.1)));
                Ok(resp::Msg::Int(self.1))
            }
        }
    }

    fn to_command(self) -> Command {
        Command::Incr(self)
    }
}

impl Execute for GetRange {
    fn parse(mut args: Args) -> Result<Self, Error> {
        if args.len() != 4 {
            return Err(Error::Err("wrong number of arguments for 'getrange' command"));
        }
        let mut cmd = match args.own(1) {
            resp::Msg::String(key) => GetRange(key, 0, 0),
            resp::Msg::BulkString(Some(key)) => GetRange(key, 0, 0),
            _ => return Err(Error::Err("invalid parameter for 'getrange' command")),
        };
        cmd.1 = match args.own(2) {
            resp::Msg::BulkString(Some(key)) | resp::Msg::String(key) => {
                match str::from_utf8(key.as_ref()) {
                    Ok(k) => match k.parse::<i64>() {
                        Ok(n) => n,
                        Err(_) =>  return Err(Error::Err("invalid parameter for 'getrange' command")),
                    },
                    Err(_) => return Err(Error::Err("invalid parameter for 'getrange' command")),
                }
            }
            resp::Msg::Int(n) => n as i64,
            _ => return Err(Error::Err("invalid parameter for 'getrange' command")),
        };
        cmd.2 = match args.own(3) {
            resp::Msg::BulkString(Some(key)) | resp::Msg::String(key) => {
                match str::from_utf8(key.as_ref()) {
                    Ok(k) => match k.parse::<i64>() {
                        Ok(n) => n,
                        Err(_) => return Err(Error::Err("invalid parameter for 'getrange' command")),
                    },
                    Err(_) => return Err(Error::Err("invalid parameter for 'getrange' command")),
                }
            }
            resp::Msg::Int(n) => n as i64,
            _ => return Err(Error::Err("invalid parameter for 'getrange' command")),
        };

        Ok(cmd)
    }

    fn shard(&self) -> u64 {
        let mut hasher = SeaHasher::new();
        self.0.hash(&mut hasher);
        hasher.finish()
    }

    fn exec(&self, db: &mut Database) -> Result<resp::Msg, Error> {
        let haystack = match db.get(&self.0) {
            Some(val) => match val {
                database::Value::Scalar(val) => {
                    let val = match val {
                        Scalar::String(s) => s.clone(),
                        Scalar::Integer(i) => Bytes::from(i.to_string()),
                    };
                    let val_len = val.len() as i64;
                    let left = if self.1 < 0 {
                        val_len + self.1
                    } else {
                        self.1
                    };
                    if left > val_len || left < 0 {
                        Some(Bytes::new())
                    } else {
                        let right = if self.2 < 0 {
                            val_len + self.2
                        } else if self.2 > val_len {
                            val_len
                        } else {
                            self.2
                        };

                        Some(val.slice(left as usize, right as usize))
                    }
                },
                _ => return Err(Error::WrongType),
            },
            None => None,
        };
        Ok(resp::Msg::BulkString(haystack))
    }

    fn to_command(self) -> Command {
        Command::GetRange(self)
    }
}

impl Execute for SetRange {
    fn parse(mut args: Args) -> Result<Self, Error> {
        if args.len() != 4 {
            return Err(Error::Err("wrong number of arguments for 'setrange' command"));
        }
        let mut cmd = match args.own(1) {
            resp::Msg::String(key) => SetRange(key, 0, Bytes::new()),
            resp::Msg::BulkString(Some(key)) => SetRange(key, 0, Bytes::new()),
            _ => return Err(Error::Err("invalid parameter for 'setrange' command")),
        };
        cmd.1 = match args.own(2) {
            resp::Msg::BulkString(Some(key)) | resp::Msg::String(key) => {
                match str::from_utf8(key.as_ref()) {
                    Ok(k) => match k.parse::<u64>() {
                        Ok(n) => n,
                        Err(_) =>  return Err(Error::Err("invalid parameter for 'setrange' command")),
                    },
                    Err(_) => return Err(Error::Err("invalid parameter for 'setrange' command")),
                }
            }
            resp::Msg::Int(n) => n as u64,
            _ => return Err(Error::Err("invalid parameter for 'setrange' command")),
        };
        cmd.2 = match args.own(3) {
            resp::Msg::BulkString(Some(key)) | resp::Msg::String(key) => {
                key
            }
            _ => return Err(Error::Err("invalid parameter for 'setrange' command")),
        };

        Ok(cmd)
    }

    fn shard(&self) -> u64 {
        let mut hasher = SeaHasher::new();
        self.0.hash(&mut hasher);
        hasher.finish()
    }

    fn exec(&self, db: &mut Database) -> Result<resp::Msg, Error> {
        let (key, haystack) = match db.remove_entry(&self.0) {
            Some((k, val)) => match val {
                DBValue::Scalar(Scalar::String(s)) => {
                    (k, s)
                },
                DBValue::Scalar(Scalar::Integer(i)) => {
                    (k, Bytes::from(i.to_string()))
                },
                _ => return Err(Error::WrongType),
            },
            None => (self.0.clone(), Bytes::new()),
        };

        let mut hs = haystack.try_mut().unwrap_or_else(|s| BytesMut::from(&s[..]));
        let minimum_size = (self.1 as usize) + self.2.len();
        if hs.capacity() < minimum_size {
            hs.reserve(minimum_size);
        }
        if hs.len() < minimum_size {
            unsafe { hs.set_len(minimum_size); }
        }
        hs.as_mut()[self.1 as usize..minimum_size].copy_from_slice(self.2.as_ref());
        let r = hs.len();
        db.insert(key, database::Value::Scalar(Scalar::String(hs.freeze())));

        Ok(resp::Msg::Int(r as i64))
    }

    fn to_command(self) -> Command {
        Command::SetRange(self)
    }
}