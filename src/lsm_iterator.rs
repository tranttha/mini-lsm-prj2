// Copyright (c) 2022-2026 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::ops::Bound;

use anyhow::{Result, bail};
use bytes::Bytes;

use crate::{
    iterators::{
        StorageIterator, concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the course for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    is_valid: bool,
    /// The user key of the last version we've already resolved (returned to the caller, or
    /// determined to be deleted). Descending timestamp order guarantees the first *visible*
    /// version of a user key we see is its newest, so every later entry with this same user
    /// key is an older version we must skip, not a new key to reconsider.
    prev_key: Vec<u8>,
    /// Snapshot ceiling: only versions with `ts <= read_ts` are visible to this iterator.
    read_ts: u64,
}

impl LsmIterator {
    pub(crate) fn new(
        iter: LsmIteratorInner,
        end_bound: Bound<Bytes>,
        read_ts: u64,
    ) -> Result<Self> {
        let mut iter = Self {
            inner: iter,
            end_bound,
            is_valid: true,
            prev_key: Vec::new(),
            read_ts,
        };
        iter.move_to_non_delete()?;
        Ok(iter)
    }

    /// Whether `key` is past `end_bound` — i.e. the scan should already be finished.
    fn past_end(&self, key: &[u8]) -> bool {
        match &self.end_bound {
            Bound::Included(b) => key > b.as_ref(),
            Bound::Excluded(b) => key >= b.as_ref(),
            Bound::Unbounded => false,
        }
    }

    /// Collapse the multi-version internal stream down to one entry per user key — the
    /// newest version visible at `read_ts` — skipping that key's older versions and, if the
    /// newest visible version is a tombstone, the key itself. Checks `end_bound` before
    /// consuming each entry, so a long run of old versions/tombstones past the bound is
    /// never read just to discover we were already done.
    fn move_to_non_delete(&mut self) -> Result<()> {
        loop {
            if !self.inner.is_valid() || self.past_end(self.inner.key().into_inner()) {
                self.is_valid = false;
                return Ok(());
            }
            if self.inner.key().ts() > self.read_ts {
                // Invisible to this snapshot: must not touch `prev_key`, or a later, truly
                // visible version of this same key would be wrongly treated as an old
                // version of an already-resolved key and skipped too.
                self.inner.next()?;
                continue;
            }
            let key = self.inner.key().into_inner();
            if key == self.prev_key.as_slice() {
                // An older version of a user key we've already resolved — its newest
                // visible version was already seen (and returned or skipped) earlier.
                self.inner.next()?;
                continue;
            }
            self.prev_key = key.to_vec();
            if self.inner.value().is_empty() {
                // Newest visible version of this (new) key is a tombstone: skip it.
                // `prev_key` is already updated, so the check above will skip its older
                // versions too.
                self.inner.next()?;
                continue;
            }
            return Ok(());
        }
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.inner.key().into_inner()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        self.move_to_non_delete()
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        assert!(self.is_valid(), "invalid access to the iterator");
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        assert!(self.is_valid(), "invalid access to the iterator");
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("the iterator is tainted by a previous error");
        }
        if self.iter.is_valid()
            && let Err(e) = self.iter.next()
        {
            self.has_errored = true;
            return Err(e);
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
