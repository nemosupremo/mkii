use std::hash::BuildHasherDefault;

use bytes::Bytes;
use seahash;

use super::command::{self, Execute};
use super::resp;

#[derive(Clone, Copy)]
enum DBState {
    None,
    Ready(*mut Database),
}

pub enum Scalar {
    String(Bytes),
    Integer(i64),
}

#[allow(dead_code)]
pub enum HashMapValue {
    Skip(),
    Hash()
}

#[allow(dead_code)]
pub enum SetValue {
    Skip(),
    Hash()
}

#[allow(dead_code)]
pub enum Value {
    Scalar(Scalar),
    List(Vec<Scalar>),
    HashMap(std::collections::HashMap<Bytes, Scalar, BuildHasherDefault<seahash::SeaHasher>>),
    Set(),
    SortedSet(),
}

pub type Database = std::collections::HashMap<Bytes, Value, BuildHasherDefault<seahash::SeaHasher>>;

thread_local! {
    static DB: *mut DBState = Box::into_raw(Box::new(DBState::None));
}

pub fn execute(command: &dyn Execute) -> Result<resp::Msg, command::Error> {
    DB.with(|f| match unsafe { **f } {
        DBState::None => {
            let mut db = Box::new(Database::default());
            let r = command.exec(&mut db);
            unsafe {
                **f = DBState::Ready(Box::into_raw(db));
            }

            r
        }
        DBState::Ready(db_ptr) => {
            let db = unsafe { &mut *db_ptr };
            command.exec(db)
        }
    })
}

#[allow(dead_code)]
pub fn reclaim() {
    DB.with(|f| match unsafe { **f } {
        DBState::None => {}
        DBState::Ready(db_ptr) => {
            let db = unsafe { &mut *db_ptr };
            db.shrink_to_fit();
        }
    });
}
