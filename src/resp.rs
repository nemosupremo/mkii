use bytes::{BufMut, Bytes, BytesMut};
use memchr::memchr;
use std::io;
use tokio::codec;

#[derive(Clone, Debug)]
pub enum Msg {
    None,
    NotReady,
    Str(&'static str),
    String(Bytes),             // string
    Error(String),             // error, String type so it easier to use with format!
    Int(i64),                  // int
    BulkString(Option<Bytes>), // bulk stirng
    Array(Option<Vec<Msg>>),   // array
}

pub struct Codec {
    curr_kind: Msg,
    idx: usize,
    sz: usize,
    inner: Option<Box<Codec>>,
}

impl Codec {
    pub fn new() -> Codec {
        Codec {
            idx: 0,
            curr_kind: Msg::None,
            sz: 0,
            inner: None,
        }
    }

    fn reset(&mut self) {
        self.idx = 0;
        self.curr_kind = Msg::None;
        self.sz = 0;
        if let Some(inner) = self.inner.as_mut() {
            inner.reset();
        }
    }

    fn reset2(&mut self) -> Msg {
        self.idx = 0;
        self.sz = 0;
        if let Some(inner) = self.inner.as_mut() {
            inner.reset();
        }
        std::mem::replace(&mut self.curr_kind, Msg::None)
    }

    fn read_line(&mut self, buf: &mut BytesMut) -> Option<Bytes> {
        let mut buf_new = buf[self.idx..].as_ref();
        let carriage: usize = loop {
            if let Some(carriage_pos) = memchr(b'\r', buf_new) {
                if carriage_pos == buf_new.len() - 1 {
                    self.idx = buf.len() - 1;
                    return None;
                }
                // we know the found \r wasn't the last char in the buffer
                if unsafe { *buf_new.get_unchecked(carriage_pos + 1) } != b'\n' {
                    self.idx += carriage_pos;
                    buf_new = buf[self.idx..].as_ref();
                    continue;
                }
                break carriage_pos + self.idx;
            } else {
                self.idx = buf.len();
                return None;
            }
        };

        let mut line = buf.split_to(carriage + 2);

        // line raw includes the data type and the \r\n
        line.advance(1);
        line.truncate(line.len() - 2);

        // we have no consumed the buffer, the remaining buffer is now at
        // idx 0
        self.idx = 0;
        return Some(line.freeze());
    }

    fn read_isize(&mut self, buf: &mut BytesMut) -> Option<isize> {
        // let mut iter = buf[self.idx..].windows(2);
        let mut buf_new = buf[self.idx..].as_ref();
        let carriage: usize = loop {
            if let Some(carriage_pos) = memchr(b'\r', buf_new) {
                if carriage_pos == buf_new.len() - 1 {
                    self.idx = buf.len() - 1;
                    return None;
                }
                // we know the found \r wasn't the last char in the buffer
                if unsafe { *buf_new.get_unchecked(carriage_pos + 1) } != b'\n' {
                    self.idx += carriage_pos + 1;
                    buf_new = buf[self.idx..].as_ref();
                    continue;
                }
                break carriage_pos + self.idx;
            } else {
                self.idx = buf.len();
                return None;
            }
        };

        let ret = {
            let mut len = 0;
            for b in buf[1..carriage].iter() {
                len = (len * 10) + ((b - b'0') as isize);
            }
            Some(len)
        };

        buf.advance(carriage + 2);
        self.idx = 0;
        ret
    }
}

impl codec::Decoder for Codec {
    type Item = Msg;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Msg>, io::Error> {
        // loop here instead of recursively calling decode
        loop {
            let r = match &mut self.curr_kind {
                Msg::None => {
                    if buf.len() == 0 {
                        return Ok(None);
                    }
                    self.curr_kind = match buf[self.idx] {
                        b'+' => Msg::String(Bytes::with_capacity(0)),
                        b'-' => Msg::Error(String::with_capacity(0)),
                        b':' => Msg::Int(0),
                        b'$' => Msg::BulkString(None),
                        b'*' => Msg::Array(None),
                        _ => {
                            buf.advance(1);
                            return Err(io::Error::new(io::ErrorKind::Other, "invalid RESP type"));
                        }
                    };
                    self.idx += 1;
                    Ok(Some(Msg::NotReady))
                }
                Msg::Str(_) => unreachable!(),
                Msg::NotReady => unreachable!(),
                Msg::String(_) | Msg::Error(_) => {
                    let line = self.read_line(buf);
                    if let Some(s) = line {
                        if let Msg::String(_) = self.curr_kind {
                            self.reset();
                            Ok(Some(Msg::String(s)))
                        } else {
                            self.reset();
                            match std::str::from_utf8(s.as_ref()) {
                                Ok(s) => Ok(Some(Msg::Error(s.to_string()))),
                                Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
                            }
                        }
                    } else {
                        Ok(None)
                    }
                }
                Msg::Int(_) => {
                    let line = self.read_isize(buf);
                    match line {
                        Some(s) => {
                            self.reset();
                            Ok(Some(Msg::Int(s as i64)))
                        }
                        None => Ok(None),
                    }
                }
                Msg::BulkString(None) => {
                    let line = self.read_isize(buf);
                    match line {
                        Some(s) => {
                            assert_eq!(0, self.idx);
                            if s < 0 {
                                self.reset();
                                return Ok(Some(Msg::BulkString(None)));
                            }
                            self.sz = s as usize;
                            self.curr_kind = Msg::BulkString(Some(Bytes::with_capacity(0)));
                            Ok(Some(Msg::NotReady))
                        }
                        None => Ok(None),
                    }
                }
                Msg::BulkString(_) => {
                    if self.sz + 2 > buf.len() {
                        Ok(None)
                    } else {
                        let fin = buf.split_to(self.sz);
                        buf.advance(2); // carriage return
                        self.reset();
                        Ok(Some(Msg::BulkString(Some(fin.freeze()))))
                    }
                }
                Msg::Array(None) => {
                    let line = self.read_isize(buf);
                    match line {
                        Some(s) => {
                            assert_eq!(0, self.idx);
                            if s < 0 {
                                self.reset();
                                return Ok(Some(Msg::Array(None)));
                            }
                            self.curr_kind = Msg::Array(Some(Vec::with_capacity(s as usize)));
                            if let None = self.inner {
                                self.inner = Some(Box::new(Codec::new()));
                            }
                            Ok(Some(Msg::NotReady))
                        }
                        None => Ok(None),
                    }
                }
                Msg::Array(Some(msgs)) => {
                    if let Some(inner) = self.inner.as_mut() {
                        while msgs.len() < msgs.capacity() {
                            match inner.decode(buf) {
                                Ok(option) => {
                                    if let Some(msg) = option {
                                        msgs.push(msg);
                                    } else {
                                        return Ok(option);
                                    }
                                }
                                Err(e) => {
                                    return Err(e);
                                }
                            }
                        }
                        Ok(Some(self.reset2()))
                    } else {
                        Err(io::Error::new(io::ErrorKind::Other, "invalid datatype."))
                    }
                }
            };

            if let Ok(Some(Msg::NotReady)) = r {
                continue;
            }
            break r;
        }
    }
}

impl codec::Encoder for Codec {
    type Item = Msg;
    type Error = io::Error;

    fn encode(&mut self, msg: Msg, buf: &mut BytesMut) -> Result<(), io::Error> {
        match msg {
            Msg::None | Msg::NotReady => {
                Err(io::Error::new(io::ErrorKind::Other, "invalid datatype."))
            }
            Msg::String(s) => {
                buf.reserve(1 + s.len() + 2);
                buf.put_u8(b'+');
                buf.put(s);
                buf.put_slice(b"\r\n");
                Ok(())
            }
            Msg::Str(s) => {
                buf.reserve(1 + s.len() + 2);
                buf.put_u8(b'+');
                buf.put(s);
                buf.put_slice(b"\r\n");
                Ok(())
            }
            Msg::Error(s) => {
                buf.reserve(1 + s.len() + 2);
                buf.put_u8(b'-');
                buf.put(s);
                buf.put_slice(b"\r\n");
                Ok(())
            }
            Msg::Int(i) => {
                let s = i.to_string();
                buf.reserve(1 + s.len() + 2);
                buf.put_u8(b':');
                buf.put(s);
                buf.put_slice(b"\r\n");
                Ok(())
            }
            Msg::BulkString(None) => {
                buf.reserve(5);
                buf.put_slice(b"$-1\r\n");
                Ok(())
            }
            Msg::BulkString(Some(bs)) => {
                let szs = bs.len().to_string();
                buf.reserve(1 + szs.len() + 2 + bs.len() + 2);
                buf.put_u8(b'$');
                buf.put(szs);
                buf.put_slice(b"\r\n");
                buf.put_slice(&bs);
                buf.put_slice(b"\r\n");
                Ok(())
            }
            Msg::Array(None) => {
                buf.reserve(5);
                buf.put_slice(b"*-1\r\n");
                Ok(())
            }
            Msg::Array(Some(msgs)) => {
                let szs = msgs.len().to_string();
                buf.reserve(1 + szs.len() + 2);
                buf.put_u8(b'*');
                buf.put(szs);
                buf.put_slice(b"\r\n");
                for msg in msgs {
                    if let Err(e) = self.encode(msg, buf) {
                        return Err(e);
                    }
                }
                Ok(())
            }
        }
    }
}
