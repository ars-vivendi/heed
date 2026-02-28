# Design: `RwTxn::split()` — Cross-Database Read+Write

## Problem

LMDB supports multiple named databases within a single environment, all sharing
one write transaction. A common pattern is reading from database A while writing
to database B within the same transaction. The current heed API prevents this
because write methods require `&mut RwTxn`, which exclusively borrows the
transaction and blocks simultaneous reads.

```rust
// Desired pattern — currently impossible:
let iter = db_a.iter(&wtxn)?;          // borrows &wtxn
for item in iter {
    let (k, v) = item?;
    db_b.put(&mut wtxn, &k, &new_v)?;  // ERROR: wtxn already borrowed
}
```

## LMDB Safety Background

LMDB's documentation states:

> Values returned from the database are valid only until a subsequent update
> operation, or the end of the transaction.

This is scoped at the **transaction level** — the docs make no per-database
guarantee. However, the implementation provides stronger properties:

- **Non-WRITEMAP mode** (default): `mdb_page_touch()` performs copy-on-write to
  **heap-allocated** pages via `malloc`. The original mmap pages (where read
  pointers point) are never modified in-place. Writing to DB B does not
  invalidate pointers into DB A's pages.

- **WRITEMAP mode**: Dirty pages are allocated directly in the mmap region.
  Freed page numbers can be reused. A write to DB B *can* invalidate pointers
  into DB A if a freed page from A is reused by B.

Since `WRITEMAP` is already behind `unsafe fn flags()` in heed, and most users
never enable it, we choose to **always allow zero-copy reads from ReadHalf** and
document the WRITEMAP risk.

## Design: Borrowing Split

### Overview

Add a `split()` method to `RwTxn` that borrows it mutably and returns two
halves:

```rust
impl RwTxn<'_> {
    pub fn split(&mut self) -> (ReadHalf<'_>, WriteHalf<'_>)
}
```

- `ReadHalf<'a>` — supports all read operations (get, iter, range, etc.)
- `WriteHalf<'a>` — supports all write operations (put, delete, clear, etc.)

Both halves borrow from the `RwTxn`, preventing `commit()` / `abort()` while
the split is active. They share the same underlying `MDB_txn` pointer.

### Traits: `ReadTxn` and `WriteTxn`

To accept both the original transaction types and the split halves, we introduce
two traits:

```rust
/// # Safety
/// Implementors must return a valid, live MDB_txn pointer for a read (or
/// read-write) LMDB transaction that is not concurrently accessed from
/// another thread.
pub unsafe trait ReadTxn {
    /// Returns the raw LMDB transaction pointer.
    fn txn_ptr(&self) -> NonNull<ffi::MDB_txn>;

    /// Returns the raw LMDB environment pointer.
    fn env_mut_ptr(&self) -> NonNull<ffi::MDB_env>;
}

/// Marker trait for transactions that support write operations.
///
/// # Safety
/// Implementors must ensure the underlying MDB_txn was opened without
/// MDB_RDONLY.
pub unsafe trait WriteTxn: ReadTxn {}
```

Implementations:

| Type               | `ReadTxn` | `WriteTxn` |
|--------------------|-----------|------------|
| `RoTxn`            | ✓         |            |
| `RwTxn`            | ✓         | ✓          |
| `ReadHalf<'_>`     | ✓         |            |
| `WriteHalf<'_>`    | ✓ *       | ✓          |

\* `WriteHalf` implements `ReadTxn` because LMDB write cursors can also read.

### Struct Definitions

```rust
/// A read-only view obtained from splitting a RwTxn.
///
/// Holds a shared reference to the underlying transaction pointer.
/// All read operations (get, iter, range, etc.) are available.
pub struct ReadHalf<'p> {
    txn: NonNull<ffi::MDB_txn>,
    env: NonNull<ffi::MDB_env>,
    _phantom: PhantomData<&'p RwTxn<'p>>,
}

/// A write-capable view obtained from splitting a RwTxn.
///
/// Holds an exclusive-like reference to the underlying transaction pointer.
/// All write operations (put, delete, clear, etc.) are available, plus reads.
pub struct WriteHalf<'p> {
    txn: NonNull<ffi::MDB_txn>,
    env: NonNull<ffi::MDB_env>,
    _phantom: PhantomData<&'p mut RwTxn<'p>>,
}
```

Both are `!Send + !Sync` (via `NonNull` which is `!Send + !Sync`).

### Method Signature Migration

**Read methods** change from `txn: &RoTxn` to `txn: &impl ReadTxn`:

```rust
// Before
pub fn get<'a, 'txn>(&self, txn: &'txn RoTxn, key: &'a KC::EItem) -> ...
pub fn iter<'txn>(&self, txn: &'txn RoTxn) -> ...

// After
pub fn get<'a, 'txn>(&self, txn: &'txn impl ReadTxn, key: &'a KC::EItem) -> ...
pub fn iter<'txn>(&self, txn: &'txn impl ReadTxn) -> ...
```

**Write methods** change from `txn: &mut RwTxn` to `txn: &mut impl WriteTxn`:

```rust
// Before
pub fn put<'a>(&self, txn: &mut RwTxn, key: &'a KC::EItem, data: &'a DC::EItem) -> ...
pub fn delete<'a>(&self, txn: &mut RwTxn, key: &'a KC::EItem) -> ...

// After
pub fn put<'a>(&self, txn: &mut impl WriteTxn, key: &'a KC::EItem, data: &'a DC::EItem) -> ...
pub fn delete<'a>(&self, txn: &mut impl WriteTxn, key: &'a KC::EItem) -> ...
```

### Removal of `Rw*` Iterator Types

The existing `RwIter`, `RwRange`, `RwPrefix` (and reverse variants) duplicate
the read iterators but add `del_current()` and `put_current()`. These methods
are **unsafe** and operate through the cursor, not the transaction.

We **remove** these types entirely. Users who need cursor-level mutation can use
the split pattern or collect-then-mutate. The `_mut()` iterator methods on
Database are also removed.

Rationale:
- The Rw iterators create a false sense of safety — their mutation methods are
  all `unsafe` anyway.
- They duplicate ~1500 lines of nearly identical code.
- The split pattern provides a safer, more ergonomic alternative for cross-DB
  mutation, which is the most common use case.
- Same-DB cursor mutation (del_current/put_current) remains available through
  `RwCursor` directly for advanced users.

### Cursor Changes

`RoCursor::new` currently accepts `&'txn RoTxn`. It will change to accept
`&'txn impl ReadTxn`:

```rust
impl<'txn> RoCursor<'txn> {
    pub(crate) fn new(txn: &'txn impl ReadTxn, dbi: MDB_dbi) -> Result<RoCursor<'txn>>
}
```

`RwCursor::new` changes to accept `&'txn impl WriteTxn`.

### Usage Example

```rust
let mut wtxn = env.write_txn()?;

// Phase 1: read from source, write to dest
{
    let (read, mut write) = wtxn.split();
    let iter = source_db.iter(&read)?;
    for result in iter {
        let (key, val) = result?;
        let new_val = transform(val);
        dest_db.put(&mut write, &key, &new_val)?;
    }
}
// split dropped — wtxn is usable again

// Phase 2: commit
wtxn.commit()?;
```

### What's NOT Changing

- `RoTxn` / `RwTxn` structs remain as-is.
- `commit()` / `abort()` remain on `RwTxn` (not on halves).
- `Env::write_txn()`, `Env::read_txn()` unchanged.
- `Env::create_database()` still takes `&mut RwTxn` (not trait-based, since it
  needs the full transaction).
- All existing read-only patterns (`db.get(&rtxn, ...)`) continue to work.
- `RoCursor` / `RwCursor` remain as internal types.

### WRITEMAP Safety Note

When `WRITEMAP` is enabled (via `unsafe fn flags()`), zero-copy references from
`ReadHalf` may be invalidated by writes through `WriteHalf` to **any** database
in the same environment. This is documented but not prevented at the type level,
because `WRITEMAP` is already an unsafe opt-in.

## Implementation Plan

### Phase 0: Remove Rw iterators and `_mut` methods
1. Delete `RwIter`, `RwRevIter` from iter.rs
2. Delete `RwRange`, `RwRevRange` from range.rs
3. Delete `RwPrefix`, `RwRevPrefix` from prefix.rs
4. Remove `iter_mut`, `rev_iter_mut`, `range_mut`, `rev_range_mut`,
   `prefix_iter_mut`, `rev_prefix_iter_mut` from Database
5. Same for EncryptedDatabase
6. Remove `Rw*` exports from lib.rs
7. Remove `del_current`, `put_current`, `put_current_reserved_with_flags`,
   `put_current_with_flags`, `append_dup` from RwCursor (or remove RwCursor
   entirely if no longer used internally)
8. Fix all compiler errors, update examples
9. Commit

### Phase 1: Add `ReadTxn` / `WriteTxn` traits
1. Define traits in txn.rs
2. Implement for `RoTxn` and `RwTxn`
3. Export from lib.rs
4. Commit

### Phase 2: Migrate method signatures to traits
1. Change all read methods: `&RoTxn` → `&impl ReadTxn`
2. Change all write methods: `&mut RwTxn` → `&mut impl WriteTxn`
3. Change cursor constructors
4. Fix compiler errors
5. Commit

### Phase 3: Add `ReadHalf` / `WriteHalf` / `split()`
1. Define structs in txn.rs
2. Implement `ReadTxn` for `ReadHalf`, `ReadTxn + WriteTxn` for `WriteHalf`
3. Add `RwTxn::split()` method
4. Export from lib.rs
5. Commit

### Phase 4: Examples, tests, documentation
1. Add cross-database read+write example
2. Add compile-fail tests (split prevents commit, etc.)
3. Update README
4. Commit

---

## Implementation Status — COMPLETE

All phases have been implemented and committed on branch `feat/rwtxn-split`:

| Commit | Phase | Summary |
|--------|-------|---------|
| `96c3540` | 0 | Remove Rw iterator types and `_mut` methods |
| `ed4a6e7` | 1 | Add `ReadTxn` and `WriteTxn` traits |
| `c900734` | 2 | Migrate all method signatures to trait-based |
| `258df6d` | 3 | Add `ReadHalf`, `WriteHalf`, `RwTxn::split()` |
| `6125c8c` | 4 | Example, doc tests, compile-fail tests |

**Test results:** 71 tests pass, 0 warnings.

---

## Safety Audit Findings

A post-implementation audit (`heed/src/txn_split_safety_tests.rs`) tested the
split feature under both the intended cross-database pattern and adversarial
same-database patterns against live LMDB internals.

### Confirmed Issues

#### 1. Same-database loose-page reuse (UB — data corruption)

**Root cause:** LMDB's `mdb_page_loose()` adds freed dirty pages to a
per-transaction "loose" list. `mdb_page_alloc()` immediately reuses those pages
for the next allocation — including while zero-copy references into those pages
are still live.

**Trigger sequence:**
1. Write N entries via `WriteHalf` (pages become dirty / heap-allocated).
2. Read a value via `ReadHalf` — pointer into a dirty heap page.
3. Delete entries via `WriteHalf` — B-tree merges free dirty pages as loose.
4. Insert new entries via `WriteHalf` — `mdb_page_alloc` reuses loose pages,
   overwriting the memory behind the held reference.

**Observed:** Three independent tests (`same_db_dirty_page_reuse_after_merge`,
`same_db_heavy_delete_reinsert_cycle`,
`same_db_get_dirty_page_then_overwrite_same_key`) all reproduce this, showing
values silently changing to unrelated content on every run.

#### 2. MAIN_DBI aliasing (UB — data corruption)

**Root cause:** The **unnamed database** (opened with `create_database(&mut
wtxn, None)`) is LMDB's `MAIN_DBI` (slot 1). All named-database metadata
records (name → DBI mapping) are also stored as key-value pairs in `MAIN_DBI`.
They share the same B-tree.

Writing to any **named** database triggers `mdb_cursor_touch(mc, MAIN_DBI)`
for the first write in that transaction, which copy-on-writes the B-tree path
from root to leaf in `MAIN_DBI`. Any cursor the `ReadHalf` has registered on
`MAIN_DBI` (e.g. from iterating the unnamed DB) is left pointing at stale or
reused pages.

**Consequence:** "read unnamed DB + write any named DB" is **not** a safe
cross-database pattern, even though it looks like one. Confirmed by:
- `main_dbi_aliasing_unnamed_db_plus_named_db` — stored value changed from
  `"VVV..."` to `"RRR..."`.
- `main_dbi_aliasing_iter_unnamed_while_writing_named` — iterator produced
  **invalid UTF-8** after named-DB writes, indicating the MAIN_DBI page data
  was fully replaced under the cursor.

#### 3. Behavioral skew on same-database iteration (not UB, but incorrect)

Concurrent `put` calls through `WriteHalf` on the same database can cause
`ReadHalf` iterators to visit entries that were not present when the iterator
was created. B-tree page splits relocate cursor positions into newly-allocated
pages and the iterator follows, reading interspersed new entries.

**Observed:** `same_db_iter_with_concurrent_inserts` returned 3624 entries
instead of the expected 1000 seed entries.

### Safe Patterns (confirmed)

| Pattern | Result |
|---------|--------|
| Read named `db_a` + write named `db_b` (`db_a ≠ db_b`) | ✓ Safe |
| Read dirty pages of `db_a` + write `db_b` | ✓ Safe |
| Cross-database range copy | ✓ Safe |

The cross-database isolation holds because in non-WRITEMAP mode, COW pages for
`db_b` are newly `malloc`'d heap allocations that do not overlap with `db_a`'s
pages. They also do not share B-tree nodes (each named DB has its own root DBI).

### Safe Usage Rule

> **`RwTxn::split()` is only safe when `ReadHalf` and `WriteHalf` operate
> on distinct, named databases — and neither half touches the unnamed
> (MAIN_DBI) database while the other half is active.**
