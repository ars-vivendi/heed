use std::ops::{Deref, DerefMut};
use std::{marker, mem, ptr};

use crate::mdb::error::mdb_result;
use crate::mdb::ffi;
use crate::*;

pub struct RoCursor<'txn> {
    cursor: *mut ffi::MDB_cursor,
    _marker: marker::PhantomData<&'txn ()>,
}

impl<'txn> RoCursor<'txn> {
    pub(crate) fn new(txn: &'txn impl ReadTxn, dbi: ffi::MDB_dbi) -> Result<RoCursor<'txn>> {
        let mut cursor: *mut ffi::MDB_cursor = ptr::null_mut();
        let mut txn = txn.txn_ptr();
        unsafe { mdb_result(ffi::mdb_cursor_open(txn.as_mut(), dbi, &mut cursor))? }
        Ok(RoCursor { cursor, _marker: marker::PhantomData })
    }

    pub fn current(&mut self) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        let mut key_val = mem::MaybeUninit::uninit();
        let mut data_val = mem::MaybeUninit::uninit();

        // Move the cursor on the first database key
        let result = unsafe {
            mdb_result(ffi::mdb_cursor_get(
                self.cursor,
                key_val.as_mut_ptr(),
                data_val.as_mut_ptr(),
                ffi::cursor_op::MDB_GET_CURRENT,
            ))
        };

        match result {
            Ok(()) => {
                let key = unsafe { crate::from_val(key_val.assume_init()) };
                let data = unsafe { crate::from_val(data_val.assume_init()) };
                Ok(Some((key, data)))
            }
            Err(e) if e.not_found() => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn move_on_first(&mut self, op: MoveOperation) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        let mut key_val = mem::MaybeUninit::uninit();
        let mut data_val = mem::MaybeUninit::uninit();

        let flag = match op {
            MoveOperation::Any => ffi::cursor_op::MDB_FIRST,
            MoveOperation::Dup => {
                unsafe {
                    mdb_result(ffi::mdb_cursor_get(
                        self.cursor,
                        ptr::null_mut(),
                        &mut ffi::MDB_val { mv_size: 0, mv_data: ptr::null_mut() },
                        ffi::cursor_op::MDB_FIRST_DUP,
                    ))?
                };
                ffi::cursor_op::MDB_GET_CURRENT
            }
            MoveOperation::NoDup => ffi::cursor_op::MDB_FIRST,
        };

        // Move the cursor on the first database key
        let result = unsafe {
            mdb_result(ffi::mdb_cursor_get(
                self.cursor,
                key_val.as_mut_ptr(),
                data_val.as_mut_ptr(),
                flag,
            ))
        };

        match result {
            Ok(()) => {
                let key = unsafe { crate::from_val(key_val.assume_init()) };
                let data = unsafe { crate::from_val(data_val.assume_init()) };
                Ok(Some((key, data)))
            }
            Err(e) if e.not_found() => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn move_on_last(&mut self, op: MoveOperation) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        let mut key_val = mem::MaybeUninit::uninit();
        let mut data_val = mem::MaybeUninit::uninit();

        let flag = match op {
            MoveOperation::Any => ffi::cursor_op::MDB_LAST,
            MoveOperation::Dup => {
                unsafe {
                    mdb_result(ffi::mdb_cursor_get(
                        self.cursor,
                        ptr::null_mut(),
                        &mut ffi::MDB_val { mv_size: 0, mv_data: ptr::null_mut() },
                        ffi::cursor_op::MDB_LAST_DUP,
                    ))?
                };
                ffi::cursor_op::MDB_GET_CURRENT
            }
            MoveOperation::NoDup => ffi::cursor_op::MDB_LAST,
        };

        // Move the cursor on the first database key
        let result = unsafe {
            mdb_result(ffi::mdb_cursor_get(
                self.cursor,
                key_val.as_mut_ptr(),
                data_val.as_mut_ptr(),
                flag,
            ))
        };

        match result {
            Ok(()) => {
                let key = unsafe { crate::from_val(key_val.assume_init()) };
                let data = unsafe { crate::from_val(data_val.assume_init()) };
                Ok(Some((key, data)))
            }
            Err(e) if e.not_found() => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn move_on_key(&mut self, key: &[u8]) -> Result<bool> {
        let mut key_val = unsafe { crate::into_val(key) };

        // Move the cursor to the specified key
        let result = unsafe {
            mdb_result(ffi::mdb_cursor_get(
                self.cursor,
                &mut key_val,
                &mut ffi::MDB_val { mv_size: 0, mv_data: ptr::null_mut() },
                ffi::cursor_op::MDB_SET,
            ))
        };

        match result {
            Ok(()) => Ok(true),
            Err(e) if e.not_found() => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    pub fn move_on_key_greater_than_or_equal_to(
        &mut self,
        key: &[u8],
    ) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        let mut key_val = unsafe { crate::into_val(key) };
        let mut data_val = mem::MaybeUninit::uninit();

        // Move the cursor to the specified key
        let result = unsafe {
            mdb_result(ffi::mdb_cursor_get(
                self.cursor,
                &mut key_val,
                data_val.as_mut_ptr(),
                ffi::cursor_op::MDB_SET_RANGE,
            ))
        };

        match result {
            Ok(()) => {
                let key = unsafe { crate::from_val(key_val) };
                let data = unsafe { crate::from_val(data_val.assume_init()) };
                Ok(Some((key, data)))
            }
            Err(e) if e.not_found() => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn move_on_prev(&mut self, op: MoveOperation) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        let mut key_val = mem::MaybeUninit::uninit();
        let mut data_val = mem::MaybeUninit::uninit();

        let flag = match op {
            MoveOperation::Any => ffi::cursor_op::MDB_PREV,
            MoveOperation::Dup => ffi::cursor_op::MDB_PREV_DUP,
            MoveOperation::NoDup => ffi::cursor_op::MDB_PREV_NODUP,
        };

        // Move the cursor to the previous non-dup key
        let result = unsafe {
            mdb_result(ffi::mdb_cursor_get(
                self.cursor,
                key_val.as_mut_ptr(),
                data_val.as_mut_ptr(),
                flag,
            ))
        };

        match result {
            Ok(()) => {
                let key = unsafe { crate::from_val(key_val.assume_init()) };
                let data = unsafe { crate::from_val(data_val.assume_init()) };
                Ok(Some((key, data)))
            }
            Err(e) if e.not_found() => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn move_on_next(&mut self, op: MoveOperation) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
        let mut key_val = mem::MaybeUninit::uninit();
        let mut data_val = mem::MaybeUninit::uninit();

        let flag = match op {
            MoveOperation::Any => ffi::cursor_op::MDB_NEXT,
            MoveOperation::Dup => ffi::cursor_op::MDB_NEXT_DUP,
            MoveOperation::NoDup => ffi::cursor_op::MDB_NEXT_NODUP,
        };

        // Move the cursor to the next non-dup key
        let result = unsafe {
            mdb_result(ffi::mdb_cursor_get(
                self.cursor,
                key_val.as_mut_ptr(),
                data_val.as_mut_ptr(),
                flag,
            ))
        };

        match result {
            Ok(()) => {
                let key = unsafe { crate::from_val(key_val.assume_init()) };
                let data = unsafe { crate::from_val(data_val.assume_init()) };
                Ok(Some((key, data)))
            }
            Err(e) if e.not_found() => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

impl Drop for RoCursor<'_> {
    fn drop(&mut self) {
        unsafe { ffi::mdb_cursor_close(self.cursor) }
    }
}

pub struct RwCursor<'txn> {
    cursor: RoCursor<'txn>,
}

impl<'txn> RwCursor<'txn> {
    pub(crate) fn new(txn: &'txn impl WriteTxn, dbi: ffi::MDB_dbi) -> Result<RwCursor<'txn>> {
        Ok(RwCursor { cursor: RoCursor::new(txn, dbi)? })
    }

    /// Delete the entry the cursor is currently pointing to.
    ///
    /// Returns `true` if the entry was successfully deleted.
    ///
    /// # Safety
    ///
    /// It is _[undefined behavior]_ to keep a reference of a value from this database
    /// while modifying it.
    ///
    /// > [Values returned from the database are valid only until a subsequent update operation,
    /// > or the end of the transaction.](http://www.lmdb.tech/doc/group__mdb.html#structMDB__val)
    ///
    /// [undefined behavior]: https://doc.rust-lang.org/reference/behavior-considered-undefined.html
    pub unsafe fn del_current(&mut self) -> Result<bool> {
        // Delete the current entry
        let result = mdb_result(ffi::mdb_cursor_del(self.cursor.cursor, 0));

        match result {
            Ok(()) => Ok(true),
            Err(e) if e.not_found() => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

impl<'txn> Deref for RwCursor<'txn> {
    type Target = RoCursor<'txn>;

    fn deref(&self) -> &Self::Target {
        &self.cursor
    }
}

impl DerefMut for RwCursor<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cursor
    }
}

/// The way the `Iterator::next/prev` method behaves towards DUP data.
#[derive(Debug, Clone, Copy)]
pub enum MoveOperation {
    /// Move on the next/prev entry, wether it's the same key or not.
    Any,
    /// Move on the next/prev data of the current key.
    Dup,
    /// Move on the next/prev entry which is the next/prev key.
    /// Skip the multiple values of the current key.
    NoDup,
}
