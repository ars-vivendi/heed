use std::borrow::Cow;
use std::marker::PhantomData;
use std::ops::Deref;
use std::ptr::{self, NonNull};
use std::sync::Arc;

use crate::envs::{Env, EnvInner};
use crate::mdb::error::mdb_result;
use crate::mdb::ffi;
use crate::Result;

/// A trait for transactions that support read operations.
///
/// # Safety
///
/// Implementors must return a valid, live `MDB_txn` pointer for a read (or
/// read-write) LMDB transaction that is not concurrently accessed from
/// another thread.
pub unsafe trait ReadTxn {
    /// Returns the raw LMDB transaction pointer.
    fn txn_ptr(&self) -> NonNull<ffi::MDB_txn>;

    /// Returns the raw LMDB environment pointer.
    fn env_mut_ptr(&self) -> NonNull<ffi::MDB_env>;
}

/// A marker trait for transactions that support write operations.
///
/// # Safety
///
/// Implementors must ensure the underlying `MDB_txn` was opened without
/// `MDB_RDONLY`.
pub unsafe trait WriteTxn: ReadTxn {}

// Implement ReadTxn generically for all RoTxn<T> — the T marker (AnyTls,
// WithTls, WithoutTls) affects only PhantomData, not the inner layout.
unsafe impl<T> ReadTxn for RoTxn<'_, T> {
    fn txn_ptr(&self) -> NonNull<ffi::MDB_txn> {
        self.inner.txn.unwrap()
    }

    fn env_mut_ptr(&self) -> NonNull<ffi::MDB_env> {
        self.inner.env.env_mut_ptr()
    }
}

unsafe impl ReadTxn for RwTxn<'_> {
    fn txn_ptr(&self) -> NonNull<ffi::MDB_txn> {
        self.txn.inner.txn.unwrap()
    }

    fn env_mut_ptr(&self) -> NonNull<ffi::MDB_env> {
        self.txn.inner.env.env_mut_ptr()
    }
}

unsafe impl WriteTxn for RwTxn<'_> {}

/// A read-only transaction.
///
/// ## LMDB Limitations
///
/// It's a must to keep read transactions short-lived.
///
/// Active Read transactions prevent the reuse of pages freed
/// by newer write transactions, thus the database can grow quickly.
///
/// ## OSX/Darwin Limitation
///
/// At least 10 transactions can be active at the same time in the same process, since only 10 POSIX semaphores can
/// be active at the same time for a process. Threads are in the same process space.
///
/// If the process crashes in the POSIX semaphore locking section of the transaction, the semaphore will be kept locked.
///
/// Note: if your program already use POSIX semaphores, you will have less available for heed/LMDB!
///
/// You may increase the limit by editing it **at your own risk**: `/Library/LaunchDaemons/sysctl.plist`
///
/// ## This struct is covariant
///
/// ```rust
/// #[allow(dead_code)]
/// trait CovariantMarker<'a>: 'static {
///     type R: 'a;
///
///     fn is_covariant(&'a self) -> &'a Self::R;
/// }
///
/// impl<'a, T> CovariantMarker<'a> for heed::RoTxn<'static, T> {
///     type R = heed::RoTxn<'a, T>;
///
///     fn is_covariant(&'a self) -> &'a heed::RoTxn<'a, T> {
///         self
///     }
/// }
/// ```
#[repr(transparent)]
pub struct RoTxn<'e, T = AnyTls> {
    inner: RoTxnInner<'e>,
    _tls_marker: PhantomData<&'e T>,
}

struct RoTxnInner<'e> {
    /// Makes the struct covariant and !Sync
    pub(crate) txn: Option<NonNull<ffi::MDB_txn>>,
    env: Cow<'e, Arc<EnvInner>>,
}

impl<'e, T> RoTxn<'e, T> {
    pub(crate) fn new(env: &'e Env<T>) -> Result<RoTxn<'e, T>> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();

        unsafe {
            mdb_result(ffi::mdb_txn_begin(
                env.env_mut_ptr().as_mut(),
                ptr::null_mut(),
                ffi::MDB_RDONLY,
                &mut txn,
            ))?
        };

        Ok(RoTxn {
            inner: RoTxnInner { txn: NonNull::new(txn), env: Cow::Borrowed(&env.inner) },
            _tls_marker: PhantomData,
        })
    }

    pub(crate) fn static_read_txn(env: Env<T>) -> Result<RoTxn<'static, T>> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();

        unsafe {
            mdb_result(ffi::mdb_txn_begin(
                env.env_mut_ptr().as_mut(),
                ptr::null_mut(),
                ffi::MDB_RDONLY,
                &mut txn,
            ))?
        };

        Ok(RoTxn {
            inner: RoTxnInner { txn: NonNull::new(txn), env: Cow::Owned(env.inner) },
            _tls_marker: PhantomData,
        })
    }

    pub(crate) fn txn_ptr(&self) -> NonNull<ffi::MDB_txn> {
        self.inner.txn.unwrap()
    }

    /// Return the transaction's ID.
    ///
    /// This returns the identifier associated with this transaction. For a
    /// [`RoTxn`], this corresponds to the snapshot being read;
    /// concurrent readers will frequently have the same transaction ID.
    pub fn id(&self) -> usize {
        unsafe { ffi::mdb_txn_id(self.inner.txn.unwrap().as_ptr()) }
    }

    /// Commit a read transaction.
    ///
    /// Synchronizing some [`Env`] metadata with the global handle.
    ///
    /// ## LMDB
    ///
    /// It's mandatory in a multi-process setup to call [`RoTxn::commit`] upon read-only database opening.
    /// After the transaction opening, the database is dropped. The next transaction might return
    /// `Io(Os { code: 22, kind: InvalidInput, message: "Invalid argument" })` known as `EINVAL`.
    pub fn commit(mut self) -> Result<()> {
        // Asserts that the transaction hasn't been already
        // committed/aborter and ensure we cannot use it twice.
        let mut txn = self.inner.txn.take().unwrap();
        let result = unsafe { mdb_result(ffi::mdb_txn_commit(txn.as_mut())) };
        result.map_err(Into::into)
    }
}

impl<'a> Deref for RoTxn<'a, WithTls> {
    type Target = RoTxn<'a, AnyTls>;

    fn deref(&self) -> &Self::Target {
        // SAFETY: OK because repr(transparent) means RoTxn<T> always has the same layout
        // as RoTxnInner.
        unsafe { std::mem::transmute(self) }
    }
}

#[cfg(master3)]
impl std::ops::DerefMut for RoTxn<'_, WithTls> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: OK because repr(transparent) means RoTxn<T> always has the same layout
        // as RoTxnInner.
        unsafe { std::mem::transmute(self) }
    }
}

impl<'a> Deref for RoTxn<'a, WithoutTls> {
    type Target = RoTxn<'a, AnyTls>;

    fn deref(&self) -> &Self::Target {
        // SAFETY: OK because repr(transparent) means RoTxn<T> always has the same layout
        // as RoTxnInner.
        unsafe { std::mem::transmute(self) }
    }
}

#[cfg(master3)]
impl std::ops::DerefMut for RoTxn<'_, WithoutTls> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: OK because repr(transparent) means RoTxn<T> always has the same layout
        // as RoTxnInner.
        unsafe { std::mem::transmute(self) }
    }
}

impl<T> Drop for RoTxn<'_, T> {
    fn drop(&mut self) {
        if let Some(mut txn) = self.inner.txn.take() {
            // Asserts that the transaction hasn't been already
            // committed/aborter and ensure we cannot use it twice.
            unsafe { ffi::mdb_txn_abort(txn.as_mut()) }
        }
    }
}

/// Parameter defining that read transactions are opened with
/// Thread Local Storage (TLS) and cannot be sent between threads
/// `!Send`. It is often faster to open TLS-backed transactions.
///
/// When used to open transactions: A thread can only use one transaction
/// at a time, plus any child (nested) transactions. Each transaction belongs
/// to one thread. A `BadRslot` error will be thrown when multiple read
/// transactions exists on the same thread.
#[derive(Debug, PartialEq, Eq)]
pub enum WithTls {}

/// Parameter defining that read transactions are opened without
/// Thread Local Storage (TLS) and are therefore `Send`.
///
/// When used to open transactions: A thread can use any number
/// of read transactions at a time on the same thread. Read transactions
/// can be moved in between threads (`Send`).
#[derive(Debug, PartialEq, Eq)]
pub enum WithoutTls {}

/// Parameter defining that read transactions might have been opened with or
/// without Thread Local Storage (TLS).
///
/// `RwTxn`s and any `RoTxn` dereference to `&RoTxn<AnyTls>`.
pub enum AnyTls {}

/// Specificies if Thread Local Storage (TLS) must be used when
/// opening transactions. It is often faster to open TLS-backed
/// transactions but makes them `!Send`.
///
/// The `#MDB_NOTLS` flag is set on `Env` opening, `RoTxn`s and
/// iterators implements the `Send` trait. This allows the user to
/// move `RoTxn`s and iterators between threads as read transactions
/// will no more use thread local storage and will tie reader
/// locktable slots to transaction objects instead of to threads.
pub trait TlsUsage {
    /// True if TLS must be used, false otherwise.
    const ENABLED: bool;
}

impl TlsUsage for WithTls {
    const ENABLED: bool = true;
}

impl TlsUsage for WithoutTls {
    const ENABLED: bool = false;
}

impl TlsUsage for AnyTls {
    // Users cannot open environments with AnyTls; therefore, this will never be read.
    // We prefer to put the most restrictive value.
    const ENABLED: bool = false;
}

/// Is sendable only if `MDB_NOTLS` has been used to open this transaction.
unsafe impl Send for RoTxn<'_, WithoutTls> {}

/// A read-write transaction.
///
/// ## LMDB Limitations
///
/// Only one [`RwTxn`] may exist in the same environment at the same time.
/// If two exist, the new one may wait on a mutex for [`RwTxn::commit`] or [`RwTxn::abort`] to
/// be called for the first one.
///
/// ## OSX/Darwin Limitation
///
/// At least 10 transactions can be active at the same time in the same process, since only 10 POSIX semaphores can
/// be active at the same time for a process. Threads are in the same process space.
///
/// If the process crashes in the POSIX semaphore locking section of the transaction, the semaphore will be kept locked.
///
/// Note: if your program already use POSIX semaphores, you will have less available for heed/LMDB!
///
/// You may increase the limit by editing it **at your own risk**: `/Library/LaunchDaemons/sysctl.plist`
///
/// ## This struct is covariant
///
/// ```rust
/// #[allow(dead_code)]
/// trait CovariantMarker<'a>: 'static {
///     type T: 'a;
///
///     fn is_covariant(&'a self) -> &'a Self::T;
/// }
///
/// impl<'a> CovariantMarker<'a> for heed::RwTxn<'static> {
///     type T = heed::RwTxn<'a>;
///
///     fn is_covariant(&'a self) -> &'a heed::RwTxn<'a> {
///         self
///     }
/// }
/// ```
pub struct RwTxn<'p> {
    pub(crate) txn: RoTxn<'p, WithoutTls>,
}

impl<'p> RwTxn<'p> {
    pub(crate) fn new<T>(env: &'p Env<T>) -> Result<RwTxn<'p>> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();

        unsafe {
            mdb_result(ffi::mdb_txn_begin(
                env.env_mut_ptr().as_mut(),
                ptr::null_mut(),
                0,
                &mut txn,
            ))?
        };

        Ok(RwTxn {
            txn: RoTxn {
                inner: RoTxnInner { txn: NonNull::new(txn), env: Cow::Borrowed(&env.inner) },
                _tls_marker: PhantomData,
            },
        })
    }

    pub(crate) fn nested<T>(env: &'p Env<T>, parent: &'p mut RwTxn) -> Result<RwTxn<'p>> {
        let mut txn: *mut ffi::MDB_txn = ptr::null_mut();
        let parent_ptr: *mut ffi::MDB_txn = unsafe { parent.txn.inner.txn.unwrap().as_mut() };

        unsafe {
            mdb_result(ffi::mdb_txn_begin(env.env_mut_ptr().as_mut(), parent_ptr, 0, &mut txn))?
        };

        Ok(RwTxn {
            txn: RoTxn {
                inner: RoTxnInner { txn: NonNull::new(txn), env: Cow::Borrowed(&env.inner) },
                _tls_marker: PhantomData,
            },
        })
    }

    /// Splits this read-write transaction into a [`ReadHalf`] and a [`WriteHalf`].
    ///
    /// This allows reading from one database while writing to another within the
    /// same transaction, something that is not possible with `&mut RwTxn` alone
    /// since it requires an exclusive borrow.
    ///
    /// Both halves share the same underlying LMDB transaction. The caller cannot
    /// call [`commit`](RwTxn::commit) or [`abort`](RwTxn::abort) until both
    /// halves are dropped.
    ///
    /// # Safety Note
    ///
    /// This is safe in LMDB's default (non-`WRITEMAP`) mode because writes
    /// go to heap-allocated copy-on-write pages, leaving existing read cursors
    /// valid.  In `WRITEMAP` mode, writes are done in-place through an
    /// mmap'd region, which *can* invalidate pointers held by read cursors on
    /// the **same** database.  Since `WRITEMAP` is behind an `unsafe` API,
    /// that responsibility falls on the caller.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::error::Error;
    /// # use heed::EnvOpenOptions;
    /// use heed::types::*;
    /// use heed::Database;
    ///
    /// # fn main() -> Result<(), Box<dyn Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = unsafe {
    /// #     EnvOpenOptions::new()
    /// #         .map_size(10 * 1024 * 1024)
    /// #         .max_dbs(3000)
    /// #         .open(dir.path())?
    /// # };
    /// let mut wtxn = env.write_txn()?;
    /// let src: Database<Str, Str> = env.create_database(&mut wtxn, Some("src"))?;
    /// let dst: Database<Str, Str> = env.create_database(&mut wtxn, Some("dst"))?;
    /// src.put(&mut wtxn, "hello", "world")?;
    ///
    /// // Split the transaction to read `src` while writing to `dst`.
    /// {
    ///     let (read, mut write) = wtxn.split();
    ///     let val = src.get(&read, "hello")?.unwrap();
    ///     dst.put(&mut write, "hello", val)?;
    /// }
    ///
    /// wtxn.commit()?;
    ///
    /// let rtxn = env.read_txn()?;
    /// assert_eq!(dst.get(&rtxn, "hello")?, Some("world"));
    /// # Ok(()) }
    /// ```
    ///
    /// You cannot commit while the halves are alive:
    ///
    /// ```compile_fail
    /// # use std::error::Error;
    /// # use heed::EnvOpenOptions;
    /// use heed::types::*;
    /// use heed::Database;
    ///
    /// # fn main() -> Result<(), Box<dyn Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = unsafe {
    /// #     EnvOpenOptions::new()
    /// #         .map_size(10 * 1024 * 1024)
    /// #         .max_dbs(3000)
    /// #         .open(dir.path())?
    /// # };
    /// let mut wtxn = env.write_txn()?;
    /// let db: Database<Str, Str> = env.create_database(&mut wtxn, Some("db"))?;
    /// let (read, mut write) = wtxn.split();
    /// db.put(&mut write, "k", "v")?;
    /// wtxn.commit()?; // ERROR: cannot move `wtxn` while borrowed by split halves
    /// drop(read);
    /// drop(write);
    /// # Ok(()) }
    /// ```
    pub fn split(&mut self) -> (ReadHalf<'_>, WriteHalf<'_>) {
        let txn = self.txn.inner.txn.unwrap();
        let env = self.txn.inner.env.env_mut_ptr();
        (
            ReadHalf { txn, env, _marker: PhantomData },
            WriteHalf { txn, env, _marker: PhantomData },
        )
    }

    /// Commit all the operations of a transaction into the database.
    /// The transaction is reset.
    pub fn commit(mut self) -> Result<()> {
        // Asserts that the transaction hasn't been already
        // committed/aborter and ensure we cannot use it two times.
        let mut txn = self.txn.inner.txn.take().unwrap();
        let result = unsafe { mdb_result(ffi::mdb_txn_commit(txn.as_mut())) };
        result.map_err(Into::into)
    }

    /// Abandon all the operations of the transaction instead of saving them.
    /// The transaction is reset.
    pub fn abort(mut self) {
        // Asserts that the transaction hasn't been already
        // committed/aborter and ensure we cannot use it twice.
        let mut txn = self.txn.inner.txn.take().unwrap();
        unsafe { ffi::mdb_txn_abort(txn.as_mut()) }
    }
}

impl<'p> Deref for RwTxn<'p> {
    type Target = RoTxn<'p, WithoutTls>;

    fn deref(&self) -> &Self::Target {
        &self.txn
    }
}

// TODO can't we just always implement it?
#[cfg(master3)]
impl std::ops::DerefMut for RwTxn<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.txn
    }
}

/// The read half of a split [`RwTxn`].
///
/// Created by [`RwTxn::split`]. Borrows from the parent transaction,
/// preventing [`RwTxn::commit`] or [`RwTxn::abort`] while this value is
/// alive.  Implements [`ReadTxn`], so it can be passed to any database
/// method that needs a read transaction reference.
///
/// `ReadHalf` is `!Send` because LMDB transactions must stay on the
/// thread that created them.
///
/// ```compile_fail
/// fn assert_send<T: Send>() {}
/// assert_send::<heed::ReadHalf<'static>>();
/// ```
pub struct ReadHalf<'a> {
    txn: NonNull<ffi::MDB_txn>,
    env: NonNull<ffi::MDB_env>,
    _marker: PhantomData<&'a ()>,
}

/// The write half of a split [`RwTxn`].
///
/// Created by [`RwTxn::split`]. Borrows from the parent transaction,
/// preventing [`RwTxn::commit`] or [`RwTxn::abort`] while this value is
/// alive.  Implements both [`ReadTxn`] and [`WriteTxn`], so it can be
/// used for both reads and writes.
///
/// `WriteHalf` is `!Send` because LMDB transactions must stay on the
/// thread that created them.
///
/// ```compile_fail
/// fn assert_send<T: Send>() {}
/// assert_send::<heed::WriteHalf<'static>>();
/// ```
pub struct WriteHalf<'a> {
    txn: NonNull<ffi::MDB_txn>,
    env: NonNull<ffi::MDB_env>,
    _marker: PhantomData<&'a mut ()>,
}

// SAFETY: ReadHalf holds a valid MDB_txn pointer obtained from a live RwTxn.
// The lifetime `'a` ties it to the &mut RwTxn borrow, guaranteeing the
// transaction is not committed/aborted while this exists.
unsafe impl ReadTxn for ReadHalf<'_> {
    fn txn_ptr(&self) -> NonNull<ffi::MDB_txn> {
        self.txn
    }

    fn env_mut_ptr(&self) -> NonNull<ffi::MDB_env> {
        self.env
    }
}

// SAFETY: WriteHalf holds the same valid MDB_txn pointer and the underlying
// transaction was opened for read-write (without MDB_RDONLY).
unsafe impl ReadTxn for WriteHalf<'_> {
    fn txn_ptr(&self) -> NonNull<ffi::MDB_txn> {
        self.txn
    }

    fn env_mut_ptr(&self) -> NonNull<ffi::MDB_env> {
        self.env
    }
}

unsafe impl WriteTxn for WriteHalf<'_> {}

#[cfg(test)]
mod tests {
    #[test]
    fn ro_txns_are_send() {
        use crate::{RoTxn, WithoutTls};

        fn is_send<T: Send>() {}

        is_send::<RoTxn<WithoutTls>>();
    }

    #[test]
    fn rw_txns_are_send() {
        use crate::RwTxn;

        fn is_send<T: Send>() {}

        is_send::<RwTxn>();
    }
}
