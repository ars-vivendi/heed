use std::marker;
use std::ops::Bound;

use types::LazyDecode;

use crate::cursor::MoveOperation;
use crate::iteration_method::{IterationMethod, MoveBetweenKeys, MoveThroughDuplicateValues};
use crate::*;

fn move_on_range_end<'txn>(
    cursor: &mut RoCursor<'txn>,
    end_bound: &Bound<Vec<u8>>,
) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
    match end_bound {
        Bound::Included(end) => match cursor.move_on_key_greater_than_or_equal_to(end) {
            Ok(Some((key, data))) if key == &end[..] => Ok(Some((key, data))),
            Ok(_) => cursor.move_on_prev(MoveOperation::NoDup),
            Err(e) => Err(e),
        },
        Bound::Excluded(end) => cursor
            .move_on_key_greater_than_or_equal_to(end)
            .and_then(|_| cursor.move_on_prev(MoveOperation::NoDup)),
        Bound::Unbounded => cursor.move_on_last(MoveOperation::NoDup),
    }
}

fn move_on_range_start<'txn>(
    cursor: &mut RoCursor<'txn>,
    start_bound: &mut Bound<Vec<u8>>,
) -> Result<Option<(&'txn [u8], &'txn [u8])>> {
    match start_bound {
        Bound::Included(start) => cursor.move_on_key_greater_than_or_equal_to(start),
        Bound::Excluded(start) => match cursor.move_on_key_greater_than_or_equal_to(start)? {
            Some((key, _)) if key == start => cursor.move_on_next(MoveOperation::NoDup),
            result => Ok(result),
        },
        Bound::Unbounded => cursor.move_on_first(MoveOperation::NoDup),
    }
}

/// A read-only range iterator structure.
pub struct RoRange<'txn, KC, DC, C = DefaultComparator, IM = MoveThroughDuplicateValues> {
    cursor: RoCursor<'txn>,
    move_on_start: bool,
    start_bound: Bound<Vec<u8>>,
    end_bound: Bound<Vec<u8>>,
    _phantom: marker::PhantomData<(KC, DC, C, IM)>,
}

impl<'txn, KC, DC, C, IM> RoRange<'txn, KC, DC, C, IM> {
    pub(crate) fn new(
        cursor: RoCursor<'txn>,
        start_bound: Bound<Vec<u8>>,
        end_bound: Bound<Vec<u8>>,
    ) -> RoRange<'txn, KC, DC, C, IM> {
        RoRange {
            cursor,
            move_on_start: true,
            start_bound,
            end_bound,
            _phantom: marker::PhantomData,
        }
    }

    /// Move on the first value of keys, ignoring duplicate values.
    ///
    /// For more info, see [`RoIter::move_between_keys`].
    pub fn move_between_keys(self) -> RoRange<'txn, KC, DC, C, MoveBetweenKeys> {
        RoRange {
            cursor: self.cursor,
            move_on_start: self.move_on_start,
            start_bound: self.start_bound,
            end_bound: self.end_bound,
            _phantom: marker::PhantomData,
        }
    }

    /// Move through key/values entries and output duplicate values.
    ///
    /// For more info, see [`RoIter::move_through_duplicate_values`].
    pub fn move_through_duplicate_values(
        self,
    ) -> RoRange<'txn, KC, DC, C, MoveThroughDuplicateValues> {
        RoRange {
            cursor: self.cursor,
            move_on_start: self.move_on_start,
            start_bound: self.start_bound,
            end_bound: self.end_bound,
            _phantom: marker::PhantomData,
        }
    }

    /// Change the codec types of this iterator, specifying the codecs.
    pub fn remap_types<KC2, DC2>(self) -> RoRange<'txn, KC2, DC2, C, IM> {
        RoRange {
            cursor: self.cursor,
            move_on_start: self.move_on_start,
            start_bound: self.start_bound,
            end_bound: self.end_bound,
            _phantom: marker::PhantomData,
        }
    }

    /// Change the key codec type of this iterator, specifying the new codec.
    pub fn remap_key_type<KC2>(self) -> RoRange<'txn, KC2, DC, C, IM> {
        self.remap_types::<KC2, DC>()
    }

    /// Change the data codec type of this iterator, specifying the new codec.
    pub fn remap_data_type<DC2>(self) -> RoRange<'txn, KC, DC2, C, IM> {
        self.remap_types::<KC, DC2>()
    }

    /// Wrap the data bytes into a lazy decoder.
    pub fn lazily_decode_data(self) -> RoRange<'txn, KC, LazyDecode<DC>, C, IM> {
        self.remap_types::<KC, LazyDecode<DC>>()
    }
}

impl<'txn, KC, DC, C, IM> Iterator for RoRange<'txn, KC, DC, C, IM>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
    IM: IterationMethod,
    C: Comparator,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_start {
            self.move_on_start = false;
            move_on_range_start(&mut self.cursor, &mut self.start_bound)
        } else {
            self.cursor.move_on_next(IM::MOVE_OPERATION)
        };

        match result {
            Ok(Some((key, data))) => {
                let must_be_returned = match &self.end_bound {
                    Bound::Included(end) => C::compare(key, end).is_le(),
                    Bound::Excluded(end) => C::compare(key, end).is_lt(),
                    Bound::Unbounded => true,
                };

                if must_be_returned {
                    match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                        (Ok(key), Ok(data)) => Some(Ok((key, data))),
                        (Err(e), _) | (_, Err(e)) => Some(Err(Error::Decoding(e))),
                    }
                } else {
                    None
                }
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }

    fn last(mut self) -> Option<Self::Item> {
        let result = if self.move_on_start {
            move_on_range_end(&mut self.cursor, &self.end_bound)
        } else {
            match (self.cursor.current(), move_on_range_end(&mut self.cursor, &self.end_bound)) {
                (Ok(Some((ckey, _))), Ok(Some((key, data)))) if C::compare(ckey, key).is_ne() => {
                    Ok(Some((key, data)))
                }
                (Ok(_), Ok(_)) => Ok(None),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        };

        match result {
            Ok(Some((key, data))) => {
                let must_be_returned = match &self.start_bound {
                    Bound::Included(start) => C::compare(key, start).is_ge(),
                    Bound::Excluded(start) => C::compare(key, start).is_gt(),
                    Bound::Unbounded => true,
                };

                if must_be_returned {
                    match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                        (Ok(key), Ok(data)) => Some(Ok((key, data))),
                        (Err(e), _) | (_, Err(e)) => Some(Err(Error::Decoding(e))),
                    }
                } else {
                    None
                }
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

impl<KC, DC, C, IM> fmt::Debug for RoRange<'_, KC, DC, C, IM> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RoRange").finish()
    }
}

/// A reverse read-only range iterator structure.
pub struct RoRevRange<'txn, KC, DC, C = DefaultComparator, IM = MoveThroughDuplicateValues> {
    cursor: RoCursor<'txn>,
    move_on_end: bool,
    start_bound: Bound<Vec<u8>>,
    end_bound: Bound<Vec<u8>>,
    _phantom: marker::PhantomData<(KC, DC, C, IM)>,
}

impl<'txn, KC, DC, C, IM> RoRevRange<'txn, KC, DC, C, IM> {
    pub(crate) fn new(
        cursor: RoCursor<'txn>,
        start_bound: Bound<Vec<u8>>,
        end_bound: Bound<Vec<u8>>,
    ) -> RoRevRange<'txn, KC, DC, C, IM> {
        RoRevRange {
            cursor,
            move_on_end: true,
            start_bound,
            end_bound,
            _phantom: marker::PhantomData,
        }
    }

    /// Move on the first value of keys, ignoring duplicate values.
    ///
    /// For more info, see [`RoIter::move_between_keys`].
    pub fn move_between_keys(self) -> RoRevRange<'txn, KC, DC, C, MoveBetweenKeys> {
        RoRevRange {
            cursor: self.cursor,
            move_on_end: self.move_on_end,
            start_bound: self.start_bound,
            end_bound: self.end_bound,
            _phantom: marker::PhantomData,
        }
    }

    /// Move through key/values entries and output duplicate values.
    ///
    /// For more info, see [`RoIter::move_through_duplicate_values`].
    pub fn move_through_duplicate_values(
        self,
    ) -> RoRevRange<'txn, KC, DC, C, MoveThroughDuplicateValues> {
        RoRevRange {
            cursor: self.cursor,
            move_on_end: self.move_on_end,
            start_bound: self.start_bound,
            end_bound: self.end_bound,
            _phantom: marker::PhantomData,
        }
    }

    /// Change the codec types of this iterator, specifying the codecs.
    pub fn remap_types<KC2, DC2>(self) -> RoRevRange<'txn, KC2, DC2, C, IM> {
        RoRevRange {
            cursor: self.cursor,
            move_on_end: self.move_on_end,
            start_bound: self.start_bound,
            end_bound: self.end_bound,
            _phantom: marker::PhantomData,
        }
    }

    /// Change the key codec type of this iterator, specifying the new codec.
    pub fn remap_key_type<KC2>(self) -> RoRevRange<'txn, KC2, DC, C, IM> {
        self.remap_types::<KC2, DC>()
    }

    /// Change the data codec type of this iterator, specifying the new codec.
    pub fn remap_data_type<DC2>(self) -> RoRevRange<'txn, KC, DC2, C, IM> {
        self.remap_types::<KC, DC2>()
    }

    /// Wrap the data bytes into a lazy decoder.
    pub fn lazily_decode_data(self) -> RoRevRange<'txn, KC, LazyDecode<DC>, C, IM> {
        self.remap_types::<KC, LazyDecode<DC>>()
    }
}

impl<'txn, KC, DC, C, IM> Iterator for RoRevRange<'txn, KC, DC, C, IM>
where
    KC: BytesDecode<'txn>,
    DC: BytesDecode<'txn>,
    IM: IterationMethod,
    C: Comparator,
{
    type Item = Result<(KC::DItem, DC::DItem)>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = if self.move_on_end {
            self.move_on_end = false;
            move_on_range_end(&mut self.cursor, &self.end_bound)
        } else {
            self.cursor.move_on_prev(IM::MOVE_OPERATION)
        };

        match result {
            Ok(Some((key, data))) => {
                let must_be_returned = match &self.start_bound {
                    Bound::Included(start) => C::compare(key, start).is_ge(),
                    Bound::Excluded(start) => C::compare(key, start).is_gt(),
                    Bound::Unbounded => true,
                };

                if must_be_returned {
                    match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                        (Ok(key), Ok(data)) => Some(Ok((key, data))),
                        (Err(e), _) | (_, Err(e)) => Some(Err(Error::Decoding(e))),
                    }
                } else {
                    None
                }
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }

    fn last(mut self) -> Option<Self::Item> {
        let result = if self.move_on_end {
            move_on_range_start(&mut self.cursor, &mut self.start_bound)
        } else {
            let current = self.cursor.current();
            let start = move_on_range_start(&mut self.cursor, &mut self.start_bound);
            match (current, start) {
                (Ok(Some((ckey, _))), Ok(Some((key, data)))) if C::compare(ckey, key).is_ne() => {
                    Ok(Some((key, data)))
                }
                (Ok(_), Ok(_)) => Ok(None),
                (Err(e), _) | (_, Err(e)) => Err(e),
            }
        };

        match result {
            Ok(Some((key, data))) => {
                let must_be_returned = match &self.end_bound {
                    Bound::Included(end) => C::compare(key, end).is_le(),
                    Bound::Excluded(end) => C::compare(key, end).is_lt(),
                    Bound::Unbounded => true,
                };

                if must_be_returned {
                    match (KC::bytes_decode(key), DC::bytes_decode(data)) {
                        (Ok(key), Ok(data)) => Some(Ok((key, data))),
                        (Err(e), _) | (_, Err(e)) => Some(Err(Error::Decoding(e))),
                    }
                } else {
                    None
                }
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

impl<KC, DC, C, IM> fmt::Debug for RoRevRange<'_, KC, DC, C, IM> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RoRevRange").finish()
    }
}
