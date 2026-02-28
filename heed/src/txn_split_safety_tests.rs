//! Safety tests for `RwTxn::split()`.
//!
//! These tests exercise the split-transaction feature to verify safety
//! under various usage patterns. Some tests target the intended
//! cross-database pattern; others deliberately use the same database
//! from both halves to reveal potential unsoundness.
//!
//! Tests annotated with `#[should_panic]` are expected to detect
//! corruption or behavioral inconsistencies caused by the current API
//! allowing same-database (or MAIN_DBI-aliased) use of both halves.
//!
//! Run with ASan for even stronger detection:
//! ```sh
//! RUSTFLAGS="-Zsanitizer=address" cargo +nightly test -p heed --lib txn_split_safety_tests -- --nocapture
//! ```

use crate::Database;
use crate::EnvOpenOptions;
use crate::types::*;

/// Helper: open a temporary env with room for named databases.
fn tmp_env() -> (tempfile::TempDir, crate::Env<crate::WithTls>) {
    let dir = tempfile::tempdir().unwrap();
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(100 * 1024 * 1024) // 100 MB — room for large tests
            .max_dbs(10)
            .open(dir.path())
            .unwrap()
    };
    (dir, env)
}

/// Helper: open a temporary env with many named-DB slots.
fn tmp_env_many_dbs() -> (tempfile::TempDir, crate::Env<crate::WithTls>) {
    let dir = tempfile::tempdir().unwrap();
    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(100 * 1024 * 1024)
            .max_dbs(100)
            .open(dir.path())
            .unwrap()
    };
    (dir, env)
}

/// Helper: check a raw pointer against expected content. Panics on corruption.
fn assert_ref_intact(ptr: *const u8, len: usize, expected: &str, label: &str) {
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    match std::str::from_utf8(bytes) {
        Ok(s) if s == expected => { /* still intact */ }
        Ok(s) => {
            panic!(
                "[UNSOUND] {label}: data corrupted. Expected {:?}..., got {:?}...",
                &expected[..20.min(expected.len())],
                &s[..20.min(s.len())]
            );
        }
        Err(_) => {
            panic!("[UNSOUND] {label}: invalid UTF-8 — page was overwritten with non-UTF-8 data");
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// 1. Cross-database split — the intended, safe pattern
// ═══════════════════════════════════════════════════════════════════════

/// Verifies the core intended use case: iterating one database via
/// ReadHalf while inserting into a different database via WriteHalf.
#[test]
fn cross_db_split_iter_and_put() {
    let (_dir, env) = tmp_env();

    let mut wtxn = env.write_txn().unwrap();
    let src: Database<Str, Str> = env.create_database(&mut wtxn, Some("src")).unwrap();
    let dst: Database<Str, Str> = env.create_database(&mut wtxn, Some("dst")).unwrap();

    for i in 0u32..200 {
        src.put(&mut wtxn, &format!("key-{i:05}"), &format!("val-{i}")).unwrap();
    }
    wtxn.commit().unwrap();

    let mut wtxn = env.write_txn().unwrap();
    {
        let (read, mut write) = wtxn.split();
        let iter = src.iter(&read).unwrap();
        for item in iter {
            let (k, v) = item.unwrap();
            dst.put(&mut write, k, v).unwrap();
        }
    }
    wtxn.commit().unwrap();

    let rtxn = env.read_txn().unwrap();
    let src_count = src.len(&rtxn).unwrap();
    let dst_count = dst.len(&rtxn).unwrap();
    assert_eq!(src_count, dst_count);

    for item in src.iter(&rtxn).unwrap() {
        let (k, v) = item.unwrap();
        let dst_v = dst.get(&rtxn, k).unwrap().unwrap();
        assert_eq!(v, dst_v, "mismatch at key {k}");
    }
}

// ═══════════════════════════════════════════════════════════════════════
// 2. Same-database: get from fresh mmap page held across put
// ═══════════════════════════════════════════════════════════════════════

/// Holds a zero-copy reference from `get(&read, …)` while writing
/// through `&mut write` to the **same** database.
///
/// This happens to work in non-WRITEMAP mode because the old mmap
/// page is never modified (COW allocates a new heap page). But it
/// relies on LMDB implementation details, not documented guarantees.
#[test]
fn same_db_get_held_across_put_fresh_page() {
    let (_dir, env) = tmp_env();

    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Str, Str> = env.create_database(&mut wtxn, Some("db")).unwrap();
    db.put(&mut wtxn, "greeting", "hello").unwrap();
    wtxn.commit().unwrap();

    let mut wtxn = env.write_txn().unwrap();
    {
        let (read, mut write) = wtxn.split();
        let val: &str = db.get(&read, "greeting").unwrap().unwrap();
        assert_eq!(val, "hello");

        db.put(&mut write, "greeting", "world").unwrap();

        // Old mmap page is untouched by COW.
        assert_eq!(val, "hello");
        let new_val: &str = db.get(&write, "greeting").unwrap().unwrap();
        assert_eq!(new_val, "world");
    }
    wtxn.commit().unwrap();
}

// ═══════════════════════════════════════════════════════════════════════
// 3. Same-database: dirty page → loose page reuse (UB)
// ═══════════════════════════════════════════════════════════════════════

/// Triggers use-after-free through LMDB's loose-page reuse:
///   1. Write entries via WriteHalf (dirtying pages).
///   2. Read a value via ReadHalf → pointer into dirty heap page.
///   3. Delete entries → B-tree merges free dirty pages as "loose".
///   4. Insert entries → `mdb_page_alloc` reuses loose pages.
///   5. The held reference now points at overwritten memory.
#[test]
#[should_panic(expected = "UNSOUND")]
fn same_db_dirty_page_reuse_after_merge() {
    let (_dir, env) = tmp_env();

    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Str, Str> = env.create_database(&mut wtxn, Some("db")).unwrap();
    wtxn.commit().unwrap();

    let mut wtxn = env.write_txn().unwrap();
    {
        let (read, mut write) = wtxn.split();

        let big_val = "x".repeat(512);
        for i in 0u32..5000 {
            db.put(&mut write, &format!("k-{i:05}"), &big_val).unwrap();
        }

        let held_ref: &str = db.get(&read, "k-02500").unwrap().unwrap();
        let expected = held_ref.to_string();
        let held_ptr = held_ref.as_ptr();

        for i in 2000u32..4000 {
            db.delete(&mut write, &format!("k-{i:05}")).unwrap();
        }

        let new_val = "Y".repeat(512);
        for i in 10000u32..15000 {
            db.put(&mut write, &format!("j-{i:05}"), &new_val).unwrap();
        }

        assert_ref_intact(held_ptr, expected.len(), &expected, "held_ref (k-02500)");
    }
}

// ═══════════════════════════════════════════════════════════════════════
// 4. Same-database: iterator + delete (behavioral)
// ═══════════════════════════════════════════════════════════════════════

/// Iterates a database via ReadHalf while deleting entries from the
/// same database via WriteHalf. Checks whether the cursor remains
/// consistent. Because LMDB's cursor fixup adjusts internal page
/// pointers for registered cursors on the same DBI, the iterator
/// count may still be correct — this tests behavioral consistency.
#[test]
fn same_db_iter_with_concurrent_deletes() {
    let (_dir, env) = tmp_env();

    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Str, Str> = env.create_database(&mut wtxn, Some("db")).unwrap();
    let val = "v".repeat(256);
    for i in 0u32..3000 {
        db.put(&mut wtxn, &format!("entry-{i:05}"), &val).unwrap();
    }
    wtxn.commit().unwrap();

    let mut wtxn = env.write_txn().unwrap();
    {
        let (read, mut write) = wtxn.split();

        let mut iter = db.iter(&read).unwrap();
        let mut read_count = 0u32;
        let mut delete_count = 0u32;

        while let Some(result) = iter.next() {
            match result {
                Ok((key, _value)) => {
                    read_count += 1;
                    if read_count % 2 == 0 {
                        let owned_key = key.to_string();
                        db.delete(&mut write, &owned_key).unwrap();
                        delete_count += 1;
                    }
                }
                Err(e) => {
                    panic!(
                        "[ERROR] Iterator error after \
                         {read_count} reads / {delete_count} deletes: {e}"
                    );
                }
            }
        }

        // The cursor is registered in txn->mt_cursors[dbi] so LMDB
        // fixes up page pointers. The iterator count may or may not
        // be affected depending on which pages are restructured.
        eprintln!(
            "[same_db_iter_with_concurrent_deletes] \
             read_count={read_count}, delete_count={delete_count}"
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════
// 5. Same-database: iterator + massive inserts (behavioral)
// ═══════════════════════════════════════════════════════════════════════

/// Iterates a database via ReadHalf while flooding the same database
/// with inserts via WriteHalf, triggering page splits.
///
/// The iterator should see exactly 1000 seed entries, but B-tree
/// restructuring causes the cursor to visit newly inserted entries
/// that "appear" in the traversal path.
#[test]
#[should_panic(expected = "BEHAVIORAL")]
fn same_db_iter_with_concurrent_inserts() {
    let (_dir, env) = tmp_env();

    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Str, Str> = env.create_database(&mut wtxn, Some("db")).unwrap();
    let val = "seed-value-padding-to-fill-page".to_string();
    for i in (0u32..2000).step_by(2) {
        db.put(&mut wtxn, &format!("m-{i:06}"), &val).unwrap();
    }
    wtxn.commit().unwrap();

    let mut wtxn = env.write_txn().unwrap();
    {
        let (read, mut write) = wtxn.split();

        let mut iter = db.iter(&read).unwrap();
        let mut read_count = 0u32;
        let mut insert_count = 0u32;
        let insert_val = "Z".repeat(400);

        while let Some(result) = iter.next() {
            match result {
                Ok((_key, _value)) => {
                    read_count += 1;
                    if read_count % 5 == 0 {
                        for j in 0..20 {
                            let new_key = format!("m-{:06}", read_count * 2 + 1 + j * 1000);
                            db.put(&mut write, &new_key, &insert_val).unwrap();
                            insert_count += 1;
                        }
                    }
                }
                Err(e) => {
                    panic!("[ERROR] Iterator error after {read_count} reads: {e}");
                }
            }
        }

        if read_count != 1000 {
            panic!(
                "[BEHAVIORAL] Expected 1000 entries but got {read_count}. \
                 WriteHalf inserts affected ReadHalf cursor."
            );
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════
// 6. Cross-database with prior dirty read-database
// ═══════════════════════════════════════════════════════════════════════

/// Writes to db_a first (dirtying its pages), then splits and reads
/// db_a while writing to db_b. Cross-database isolation means db_b's
/// writes cannot touch db_a's pages — should be safe.
#[test]
fn cross_db_with_prior_dirty_read_db() {
    let (_dir, env) = tmp_env();

    let mut wtxn = env.write_txn().unwrap();
    let db_a: Database<Str, Str> = env.create_database(&mut wtxn, Some("a")).unwrap();
    let db_b: Database<Str, Str> = env.create_database(&mut wtxn, Some("b")).unwrap();

    for i in 0u32..500 {
        db_a.put(&mut wtxn, &format!("a-{i:05}"), &format!("val-{i}")).unwrap();
    }
    wtxn.commit().unwrap();

    let mut wtxn = env.write_txn().unwrap();
    for i in 0u32..500 {
        db_a.put(&mut wtxn, &format!("a-{i:05}"), &format!("modified-{i}")).unwrap();
    }

    {
        let (read, mut write) = wtxn.split();

        let mut count = 0u32;
        let iter = db_a.iter(&read).unwrap();
        for item in iter {
            let (key, val) = item.unwrap();
            db_b.put(&mut write, key, val).unwrap();
            count += 1;
        }
        assert_eq!(count, 500);

        let v = db_a.get(&read, "a-00042").unwrap().unwrap();
        assert_eq!(v, "modified-42");
    }

    wtxn.commit().unwrap();

    let rtxn = env.read_txn().unwrap();
    assert_eq!(db_b.len(&rtxn).unwrap(), 500);
    assert_eq!(db_b.get(&rtxn, "a-00042").unwrap(), Some("modified-42"));
}

// ═══════════════════════════════════════════════════════════════════════
// 7. Same-db: heavy delete + re-insert cycle (UB)
// ═══════════════════════════════════════════════════════════════════════

/// Aggressive loose-page reuse: multiple delete+insert cycles while
/// holding zero-copy references into dirty pages.
#[test]
#[should_panic(expected = "UNSOUND")]
fn same_db_heavy_delete_reinsert_cycle() {
    let (_dir, env) = tmp_env();

    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Str, Str> = env.create_database(&mut wtxn, Some("db")).unwrap();
    wtxn.commit().unwrap();

    let mut wtxn = env.write_txn().unwrap();
    {
        let (read, mut write) = wtxn.split();

        let big_val = "A".repeat(1024);
        for i in 0u32..3000 {
            db.put(&mut write, &format!("init-{i:05}"), &big_val).unwrap();
        }

        let ref1: &str = db.get(&read, "init-01000").unwrap().unwrap();
        let ref2: &str = db.get(&read, "init-02000").unwrap().unwrap();
        let ptr1 = ref1.as_ptr();
        let ptr2 = ref2.as_ptr();
        let expected1 = ref1.to_string();
        let expected2 = ref2.to_string();

        for cycle in 0..5 {
            let base = cycle * 600;
            for i in base..(base + 500) {
                let key = format!("init-{i:05}");
                let _ = db.delete(&mut write, &key);
            }
            let cycle_val = "B".repeat(1024);
            for i in 0..500u32 {
                let key = format!("cycle{cycle}-{i:05}");
                db.put(&mut write, &key, &cycle_val).unwrap();
            }
        }

        assert_ref_intact(ptr1, expected1.len(), &expected1, "ref1 (init-01000)");
        assert_ref_intact(ptr2, expected2.len(), &expected2, "ref2 (init-02000)");
    }
}

// ═══════════════════════════════════════════════════════════════════════
// 8. Cross-database range read + batch write
// ═══════════════════════════════════════════════════════════════════════

/// Cross-database range iteration — should be safe.
#[test]
fn cross_db_range_read_with_batch_write() {
    let (_dir, env) = tmp_env();

    let mut wtxn = env.write_txn().unwrap();
    let src: Database<Str, Str> = env.create_database(&mut wtxn, Some("src")).unwrap();
    let dst: Database<Str, Str> = env.create_database(&mut wtxn, Some("dst")).unwrap();

    for i in 0u32..5000 {
        src.put(&mut wtxn, &format!("a-{i:05}"), &format!("data-{i}")).unwrap();
    }
    wtxn.commit().unwrap();

    let mut wtxn = env.write_txn().unwrap();
    {
        let (read, mut write) = wtxn.split();

        use std::ops::Bound;
        let range = (Bound::Included("a-01000"), Bound::Included("a-01999"));
        let range = src.range(&read, &range).unwrap();
        let mut count = 0u32;
        for item in range {
            let (k, v) = item.unwrap();
            dst.put(&mut write, k, v).unwrap();
            count += 1;
        }
        assert_eq!(count, 1000);
    }
    wtxn.commit().unwrap();

    let rtxn = env.read_txn().unwrap();
    assert_eq!(dst.len(&rtxn).unwrap(), 1000);
    assert_eq!(dst.get(&rtxn, "a-01000").unwrap(), Some("data-1000"));
    assert_eq!(dst.get(&rtxn, "a-01999").unwrap(), Some("data-1999"));
    assert!(dst.get(&rtxn, "a-00999").unwrap().is_none());
    assert!(dst.get(&rtxn, "a-02000").unwrap().is_none());
}

// ═══════════════════════════════════════════════════════════════════════
// 9. Same-database: get dirty page, overwrite same key (UB)
// ═══════════════════════════════════════════════════════════════════════

/// Reads a value from a dirty page, then overwrites the same key with
/// a much longer value + inserts many fillers. The held reference into
/// the dirty page is corrupted.
#[test]
#[should_panic(expected = "UNSOUND")]
fn same_db_get_dirty_page_then_overwrite_same_key() {
    let (_dir, env) = tmp_env();

    let mut wtxn = env.write_txn().unwrap();
    let db: Database<Str, Str> = env.create_database(&mut wtxn, Some("db")).unwrap();
    wtxn.commit().unwrap();

    let mut wtxn = env.write_txn().unwrap();
    {
        let (read, mut write) = wtxn.split();

        db.put(&mut write, "target", "short").unwrap();

        let val: &str = db.get(&read, "target").unwrap().unwrap();
        let ptr = val.as_ptr();
        let original = val.to_string();
        assert_eq!(val, "short");

        let long_val = "X".repeat(4000);
        db.put(&mut write, "target", &long_val).unwrap();

        for i in 0..500u32 {
            db.put(&mut write, &format!("filler-{i:05}"), &"Z".repeat(200)).unwrap();
        }

        assert_ref_intact(ptr, original.len(), &original, "target ref");
    }
}

// ═══════════════════════════════════════════════════════════════════════
// 10. MAIN_DBI aliasing: unnamed DB + named DB share B-tree (UB)
// ═══════════════════════════════════════════════════════════════════════

/// The **unnamed** database (opened with `None`) IS `MAIN_DBI` (DBI 1)
/// in LMDB. All **named** database metadata records are also stored in
/// `MAIN_DBI`. They share the same B-tree.
///
/// This test dirties unnamed-DB (MAIN_DBI) pages, holds a zero-copy
/// ref, then deletes + re-inserts via WriteHalf to trigger loose-page
/// reuse in MAIN_DBI. It also writes to a named DB to exercise the
/// `mdb_cursor_touch` path that COW's MAIN_DBI pages.
#[test]
#[should_panic(expected = "UNSOUND")]
fn main_dbi_aliasing_unnamed_db_plus_named_db() {
    let (_dir, env) = tmp_env_many_dbs();

    let mut wtxn = env.write_txn().unwrap();
    let unnamed: Database<Str, Str> = env.create_database(&mut wtxn, None).unwrap();
    let named: Database<Str, Str> = env.create_database(&mut wtxn, Some("named")).unwrap();
    wtxn.commit().unwrap();

    // Seed unnamed DB.
    let mut wtxn = env.write_txn().unwrap();
    let big_val = "U".repeat(512);
    for i in 0u32..3000 {
        unnamed.put(&mut wtxn, &format!("u-{i:05}"), &big_val).unwrap();
    }
    wtxn.commit().unwrap();

    // New txn: dirty unnamed-DB pages, then split.
    let mut wtxn = env.write_txn().unwrap();

    let new_val = "V".repeat(512);
    for i in 0u32..3000 {
        unnamed.put(&mut wtxn, &format!("u-{i:05}"), &new_val).unwrap();
    }

    {
        let (read, mut write) = wtxn.split();

        // Zero-copy ref into a dirty MAIN_DBI page.
        let val: &str = unnamed.get(&read, "u-01500").unwrap().unwrap();
        let ptr = val.as_ptr();
        let expected = val.to_string();

        // Write to named DB (triggers mdb_cursor_touch on MAIN_DBI).
        let named_val = "N".repeat(512);
        for i in 0u32..5000 {
            named.put(&mut write, &format!("n-{i:05}"), &named_val).unwrap();
        }

        // Delete unnamed-DB entries → MAIN_DBI page merges → loose pages.
        for i in 0u32..2500 {
            unnamed.delete(&mut write, &format!("u-{i:05}")).unwrap();
        }

        // Re-insert to trigger loose-page reuse.
        let reuse_val = "R".repeat(512);
        for i in 5000u32..8000 {
            unnamed.put(&mut write, &format!("u-{i:05}"), &reuse_val).unwrap();
        }

        assert_ref_intact(ptr, expected.len(), &expected, "unnamed-db ref (u-01500)");
    }
}

// ═══════════════════════════════════════════════════════════════════════
// 11. MAIN_DBI aliasing: iterate unnamed DB while writing named DBs
//     (behavioral)
// ═══════════════════════════════════════════════════════════════════════

/// Iterates the unnamed DB while writing to many named databases.
/// Named-DB writes trigger `mdb_cursor_touch` on MAIN_DBI for the
/// first write per named DB, COW'ing shared B-tree pages. The
/// unnamed-DB iterator cursor is registered on MAIN_DBI and may be
/// affected.
///
/// This is a cross-database scenario the design intends to be safe,
/// but the MAIN_DBI B-tree is shared. In practice, named-DB writes
/// corrupt the page data visible to the unnamed-DB iterator, causing
/// decode errors (invalid UTF-8) or wrong entry counts.
#[test]
#[should_panic(expected = "UNSOUND")]
fn main_dbi_aliasing_iter_unnamed_while_writing_named() {
    let (_dir, env) = tmp_env_many_dbs();

    let mut wtxn = env.write_txn().unwrap();
    let unnamed: Database<Str, Str> = env.create_database(&mut wtxn, None).unwrap();
    let mut named_dbs = Vec::new();
    for i in 0..20 {
        let db: Database<Str, Str> =
            env.create_database(&mut wtxn, Some(&format!("ndb-{i:02}"))).unwrap();
        named_dbs.push(db);
    }

    let val = "data".to_string();
    for i in 0u32..1000 {
        unnamed.put(&mut wtxn, &format!("key-{i:05}"), &val).unwrap();
    }
    wtxn.commit().unwrap();

    // Iterate unnamed DB while writing to named DBs.
    let mut wtxn = env.write_txn().unwrap();
    {
        let (read, mut write) = wtxn.split();

        let mut iter = unnamed.iter(&read).unwrap();
        let mut read_count = 0u32;
        let mut write_count = 0u32;

        while let Some(result) = iter.next() {
            match result {
                Ok((_key, _value)) => {
                    read_count += 1;
                    if read_count % 50 == 0 {
                        for (j, ndb) in named_dbs.iter().enumerate() {
                            let k = format!("wr-{read_count}-{j}");
                            ndb.put(&mut write, &k, "nval").unwrap();
                            write_count += 1;
                        }
                    }
                }
                Err(e) => {
                    panic!(
                        "[UNSOUND] Iterator decode error after {read_count} reads / \
                         {write_count} writes: {e}. Named-DB writes corrupted \
                         MAIN_DBI pages under the unnamed-DB cursor."
                    );
                }
            }
        }

        eprintln!(
            "[main_dbi_aliasing_iter] read_count={read_count}, write_count={write_count}"
        );

        // If MAIN_DBI sharing causes issues, read_count != 1000.
        if read_count != 1000 {
            panic!(
                "[UNSOUND] Expected 1000 entries from unnamed DB \
                 but iterator returned {read_count}. Named-DB writes \
                 affected the MAIN_DBI B-tree."
            );
        }
    }
}
