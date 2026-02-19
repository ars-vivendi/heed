//! Demonstrates `RwTxn::split()` — reading from one database while
//! writing to another within a single transaction.

use std::error::Error;

use heed::types::*;
use heed::{Database, EnvOpenOptions};

fn main() -> Result<(), Box<dyn Error>> {
    let path = tempfile::tempdir()?;

    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3000)
            .open(path)?
    };

    // Create two databases: "source" and "dest".
    let mut wtxn = env.write_txn()?;
    let source: Database<Str, Str> = env.create_database(&mut wtxn, Some("source"))?;
    let dest: Database<Str, Str> = env.create_database(&mut wtxn, Some("dest"))?;

    // Seed the source database.
    source.put(&mut wtxn, "alice", "42")?;
    source.put(&mut wtxn, "bob", "100")?;
    source.put(&mut wtxn, "carol", "7")?;
    wtxn.commit()?;

    // --- Split example ---
    // Read every entry from `source` and copy it into `dest`
    // within a single read-write transaction.
    let mut wtxn = env.write_txn()?;
    {
        let (read, mut write) = wtxn.split();

        // `read` implements ReadTxn — we can iterate over `source`.
        // `write` implements WriteTxn — we can insert into `dest`.
        let iter = source.iter(&read)?;
        for item in iter {
            let (key, value) = item?;
            dest.put(&mut write, key, value)?;
        }
    }
    // After dropping both halves, we can commit normally.
    wtxn.commit()?;

    // Verify the copy.
    let rtxn = env.read_txn()?;
    let mut count = 0;
    for item in dest.iter(&rtxn)? {
        let (key, value) = item?;
        // The value in dest should match the source.
        let original = source.get(&rtxn, key)?.unwrap();
        assert_eq!(value, original);
        count += 1;
    }
    assert_eq!(count, 3);
    println!("Successfully copied {count} entries from source to dest using split().");

    Ok(())
}
