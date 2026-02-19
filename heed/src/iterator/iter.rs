use std::marker;

use types::LazyDecode;

use crate::iteration_method::{IterationMethod, MoveBetweenKeys, MoveThroughDuplicateValues};
use crate::*;

/// A read-only iterator structure.
pub struct RoIter<'txn, KC, DC, IM = MoveThroughDuplicateValues> {
    cursor: RoCursor<'txn>,
    move_on_first: bool,
    _phantom: marker::PhantomData<(KC, DC, IM)>,
}

impl<'txn, KC, DC, IM> RoIter<'txn, KC, DC, IM> {
    pub(crate) fn new(cursor: RoCursor<'txn>) -> RoIter<'txn, KC, DC, IM> {
        RoIter { cursor, move_on_first: true, _phantom: marker::PhantomData }
    }

    /// Move on the first value of keys, ignoring duplicate values.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::{DatabaseFlags, EnvOpenOptions};
    /// use heed::types::*;
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = unsafe { EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?
    /// # };
    /// type BEI64 = I64<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db = env.database_options()
    ///     .types::<BEI64, BEI64>()
    ///     .flags(DatabaseFlags::DUP_SORT)
    ///     .name("dup-sort")
    ///     .create(&mut wtxn)?;
    ///
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &68, &120)?;
    /// db.put(&mut wtxn, &68, &121)?;
    /// db.put(&mut wtxn, &68, &122)?;
    /// db.put(&mut wtxn, &68, &123)?;
    /// db.put(&mut wtxn, &35, &120)?;
    /// db.put(&mut wtxn, &0, &120)?;
    /// db.put(&mut wtxn, &42, &120)?;
    ///
    /// let mut iter = db.iter(&wtxn)?.move_between_keys();
    /// assert_eq!(iter.next().transpose()?, Some((0, 120)));
    /// assert_eq!(iter.next().transpose()?, Some((35, 120)));
    /// assert_eq!(iter.next().transpose()?, Some((42, 120)));
    /// assert_eq!(iter.next().transpose()?, Some((68, 120)));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn move_between_keys(self) -> RoIter<'txn, KC, DC, MoveBetweenKeys> {
        RoIter {
            cursor: self.cursor,
            move_on_first: self.move_on_first,
            _phantom: marker::PhantomData,
        }
    }

    /// Move through key/values entries and output duplicate values.
    ///
    /// ```
    /// # use std::fs;
    /// # use std::path::Path;
    /// # use heed::{DatabaseFlags, EnvOpenOptions};
    /// use heed::types::*;
    /// use heed::byteorder::BigEndian;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let dir = tempfile::tempdir()?;
    /// # let env = unsafe { EnvOpenOptions::new()
    /// #     .map_size(10 * 1024 * 1024) // 10MB
    /// #     .max_dbs(3000)
    /// #     .open(dir.path())?
    /// # };
    /// type BEI64 = I64<BigEndian>;
    ///
    /// let mut wtxn = env.write_txn()?;
    /// let db = env.database_options()
    ///     .types::<BEI64, BEI64>()
    ///     .flags(DatabaseFlags::DUP_SORT)
    ///     .name("dup-sort")
    ///     .create(&mut wtxn)?;
    ///
    /// # db.clear(&mut wtxn)?;
    /// db.put(&mut wtxn, &68, &120)?;
    /// db.put(&mut wtxn, &68, &121)?;
    /// db.put(&mut wtxn, &68, &122)?;
    /// db.put(&mut wtxn, &68, &123)?;
    /// db.put(&mut wtxn, &35, &120)?;
    /// db.put(&mut wtxn, &0, &120)?;
    /// db.put(&mut wtxn, &42, &120)?;
    ///
    /// let mut iter = db.iter(&wtxn)?.move_through_duplicate_values();
    /// assert_eq!(iter.next().transpose()?, Some((0, 120)));
    /// assert_eq!(iter.next().transpose()?, Some((35, 120)));
    /// assert_eq!(iter.next().transpose()?, Some((42, 120)));
    /// assert_eq!(iter.next().transpose()?, Some((68, 120)));
    /// assert_eq!(iter.next().transpose()?, Some((68, 121)));
    /// assert_eq!(iter.next().transpose()?, Some((68, 122)));
    /// assert_eq!(iter.next().transpose()?, Some((68, 123)));
    /// assert_eq!(iter.next().transpose()?, None);
    ///
    /// drop(iter);
    /// wtxn.commit()?;
    /// # Ok(()) }
    /// ```
    pub fn move_through_duplicate_values(self) -> RoIter<'txn, KC, DC, MoveThroughDuplicateValues> {
        RoIter {
            cursor: self.cursor,
            move_on_first: self.move_on_first,
            _phantom: marker::PhantomData,
        }
    }

    /// Change the codec types of this iterator, specifying the codecs.
    pub fn remap_types<KC2, DC2>(self) -> RoIter<'txn, KC2, DC2, IM> {
        RoIter {
            cursor: self.cursor,
            move_on_first: self.move_on_first,
            _phantom: marker::PhantomData,
        }
    }

    /// Change the key codec type of this iterator, specifying the new codec.
    pub fn remap_key_type<KC2>(self) -> RoIter<'txn, KC2, DC, IM> {
        self.remap_types::<KC2, DC>()
    }

    /// Change the data codec type of this iterator, specifying the new codec.
    pub fn remap_data_type<DC2>(self) -> RoIter<'txn, KC, DC2, IM> {
        self.remap_types::<KC, DC2>()
    }

    /// Wrap the data bytes into a lazy decoder.
    pub fn lazily_decode_data(self) -> RoIter<'txn, KC, LazyDecode<DC>, IM> {
        self.remap_types::<KC, LazyDecode<DC>>()
    }
}

impl<'txn, KC, DC, IM> Iterator for RoIter<'txn, KC, DC, IM>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
    IM: IterationMethod,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_first {
            self.move_on_first = false;
            self.cursor.move_on_first(IM::MOVE_OPERATION)
        } else {
            self.cursor.move_on_next(IM::MOVE_OPERATION)
        };

        match result {
            Ok(Some((key, data))) => match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                (Ok(key), Ok(data)) => Some(Ok((key, data))),
                (Err(e), _) | (_, Err(e)) => Some(Err(Error::Decoding(e))),
            },
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }

    fn last(mut self) -> Option<Self::Item> {
        let result = if self.move_on_first {
            self.cursor.move_on_last(IM::MOVE_OPERATION)
        } else {
            match (self.cursor.current(), self.cursor.move_on_last(IM::MOVE_OPERATION)) {
                (Ok(Some((ckey, _))), Ok(Some((key, data)))) if ckey != key => {
                    Ok(Some((key, data)))
                }
                (Ok(_), Ok(_)) => Ok(None),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        };

        match result {
            Ok(Some((key, data))) => match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                (Ok(key), Ok(data)) => Some(Ok((key, data))),
                (Err(e), _) | (_, Err(e)) => Some(Err(Error::Decoding(e))),
            },
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

impl<KC, DC, IM> fmt::Debug for RoIter<'_, KC, DC, IM> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RoIter").finish()
    }
}
/// A reverse read-only iterator structure.
pub struct RoRevIter<'txn, KC, DC, IM = MoveThroughDuplicateValues> {
    cursor: RoCursor<'txn>,
    move_on_last: bool,
    _phantom: marker::PhantomData<(KC, DC, IM)>,
}

impl<'txn, KC, DC, IM> RoRevIter<'txn, KC, DC, IM> {
    pub(crate) fn new(cursor: RoCursor<'txn>) -> RoRevIter<'txn, KC, DC, IM> {
        RoRevIter { cursor, move_on_last: true, _phantom: marker::PhantomData }
    }

    /// Move on the first value of keys, ignoring duplicate values.
    ///
    /// For more info, see [`RoIter::move_between_keys`].
    pub fn move_between_keys(self) -> RoRevIter<'txn, KC, DC, MoveBetweenKeys> {
        RoRevIter {
            cursor: self.cursor,
            move_on_last: self.move_on_last,
            _phantom: marker::PhantomData,
        }
    }

    /// Move through key/values entries and output duplicate values.
    ///
    /// For more info, see [`RoIter::move_through_duplicate_values`].
    pub fn move_through_duplicate_values(
        self,
    ) -> RoRevIter<'txn, KC, DC, MoveThroughDuplicateValues> {
        RoRevIter {
            cursor: self.cursor,
            move_on_last: self.move_on_last,
            _phantom: marker::PhantomData,
        }
    }

    /// Change the codec types of this iterator, specifying the codecs.
    pub fn remap_types<KC2, DC2>(self) -> RoRevIter<'txn, KC2, DC2, IM> {
        RoRevIter {
            cursor: self.cursor,
            move_on_last: self.move_on_last,
            _phantom: marker::PhantomData,
        }
    }

    /// Change the key codec type of this iterator, specifying the new codec.
    pub fn remap_key_type<KC2>(self) -> RoRevIter<'txn, KC2, DC, IM> {
        self.remap_types::<KC2, DC>()
    }

    /// Change the data codec type of this iterator, specifying the new codec.
    pub fn remap_data_type<DC2>(self) -> RoRevIter<'txn, KC, DC2, IM> {
        self.remap_types::<KC, DC2>()
    }

    /// Wrap the data bytes into a lazy decoder.
    pub fn lazily_decode_data(self) -> RoRevIter<'txn, KC, LazyDecode<DC>, IM> {
        self.remap_types::<KC, LazyDecode<DC>>()
    }
}

impl<'txn, KC, DC, IM> Iterator for RoRevIter<'txn, KC, DC, IM>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
    IM: IterationMethod,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_last {
            self.move_on_last = false;
            self.cursor.move_on_last(IM::MOVE_OPERATION)
        } else {
            self.cursor.move_on_prev(IM::MOVE_OPERATION)
        };

        match result {
            Ok(Some((key, data))) => match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                (Ok(key), Ok(data)) => Some(Ok((key, data))),
                (Err(e), _) | (_, Err(e)) => Some(Err(Error::Decoding(e))),
            },
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }

    fn last(mut self) -> Option<Self::Item> {
        let result = if self.move_on_last {
            self.cursor.move_on_first(IM::MOVE_OPERATION)
        } else {
            match (self.cursor.current(), self.cursor.move_on_first(IM::MOVE_OPERATION)) {
                (Ok(Some((ckey, _))), Ok(Some((key, data)))) if ckey != key => {
                    Ok(Some((key, data)))
                }
                (Ok(_), Ok(_)) => Ok(None),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        };

        match result {
            Ok(Some((key, data))) => match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                (Ok(key), Ok(data)) => Some(Ok((key, data))),
                (Err(e), _) | (_, Err(e)) => Some(Err(Error::Decoding(e))),
            },
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

impl<KC, DC, IM> fmt::Debug for RoRevIter<'_, KC, DC, IM> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RoRevIter").finish()
    }
}
