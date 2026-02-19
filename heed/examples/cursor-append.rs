use std::error::Error;

use heed::types::*;
use heed::{Database, EnvOpenOptions, PutFlags};

// In this test we are checking that we can append ordered entries in one
// database even if there is multiple databases which already contain entries.
fn main() -> Result<(), Box<dyn Error>> {
    let env_path = tempfile::tempdir()?;

    let env = unsafe {
        EnvOpenOptions::new()
            .map_size(10 * 1024 * 1024) // 10MB
            .max_dbs(3)
            .open(env_path)?
    };

    let mut wtxn = env.write_txn()?;
    let first: Database<Str, Str> = env.create_database(&mut wtxn, Some("first"))?;
    let second: Database<Str, Str> = env.create_database(&mut wtxn, Some("second"))?;

    // We fill the first database with entries.
    first.put(&mut wtxn, "I am here", "to test things")?;
    first.put(&mut wtxn, "I am here too", "for the same purpose")?;

    // We append ordered entries in the second database using the APPEND flag.
    second.put_with_flags(&mut wtxn, PutFlags::APPEND, "aaaa", "lol")?;
    second.put_with_flags(&mut wtxn, PutFlags::APPEND, "abcd", "lol")?;
    second.put_with_flags(&mut wtxn, PutFlags::APPEND, "bcde", "lol")?;

    wtxn.commit()?;

    Ok(())
}
