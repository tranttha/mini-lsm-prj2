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

use std::sync::Arc;

use bytes::Buf;

use crate::key::{KeySlice, KeyVec};

use super::{Block, SIZEOF_U16, SIZEOF_U64};

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        let mut iter = Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        };
        // Unconditionally establish `first_key`'s raw bytes now, before any caller can
        // run `seek_to_key`'s binary search, which almost never visits idx 0 first and
        // would otherwise decode a compressed entry against a still-empty `first_key`.
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice<'_> {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        debug_assert!(!self.key.is_empty(), "invalid iterator");
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_idx(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to_idx(self.idx);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let (mut low, mut high) = (0, self.block.offsets.len());
        while low < high {
            let mid = low + (high - low) / 2;
            self.seek_to_idx(mid);
            assert!(self.is_valid());
            match self.key().cmp(&key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Equal => return,
                std::cmp::Ordering::Greater => high = mid,
            }
        }
        self.seek_to_idx(low);
    }

    /// Positions the cursor at entry `idx`, or invalidates the iterator if `idx` is out of range.
    fn seek_to_idx(&mut self, idx: usize) {
        self.idx = idx;
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }
        let offset = self.block.offsets[idx] as usize;
        let mut entry = &self.block.data[offset..];

        let overlap_len = entry.get_u16() as usize;
        let rest_len = entry.get_u16() as usize;
        let rest_key = &entry[..rest_len];
        self.key.clear();
        self.key.append(&self.first_key.key_ref()[..overlap_len]);
        self.key.append(rest_key);
        entry.advance(rest_len);

        let ts = entry.get_u64();
        self.key.set_ts(ts);
        if idx == 0 {
            self.first_key = self.key.clone();
        }

        let value_len = entry.get_u16() as usize;
        let value_start = offset + SIZEOF_U16 * 3 + SIZEOF_U64 + rest_len;
        self.value_range = (value_start, value_start + value_len);
    }
}
