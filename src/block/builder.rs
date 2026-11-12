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

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::{Block, SIZEOF_U16, SIZEOF_U64};

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Current encoded size: data section + one u16 offset per entry + the trailing element count.
    fn estimated_size(&self) -> usize {
        SIZEOF_U16 + self.offsets.len() * SIZEOF_U16 + self.data.len()
    }

    /// Length of the shared prefix between `self.first_key` and `key`. `0` when
    /// `self.first_key` is still empty (the very first entry), with no special case needed.
    fn compute_overlap(&self, key: KeySlice) -> usize {
        self.first_key
            .key_ref()
            .iter()
            .zip(key.key_ref())
            .take_while(|(a, b)| a == b)
            .count()
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key must not be empty");

        let overlap_len = self.compute_overlap(key);
        let rest_key = &key.key_ref()[overlap_len..];

        // A block always accepts its first entry, even if that entry alone exceeds
        // `block_size`, so a single oversized key-value pair is never unrepresentable.
        // Measured against the actual (overlap-compressed) size being written, not the
        // full key length, or compression would never let a block pack more entries.
        if !self.is_empty()
            && self.estimated_size() + rest_key.len() + value.len() + SIZEOF_U16 * 4 + SIZEOF_U64
                > self.block_size
        {
            return false;
        }

        self.offsets.push(self.data.len() as u16);

        self.data.put_u16(overlap_len as u16);
        self.data.put_u16(rest_key.len() as u16);
        self.data.put(rest_key);
        self.data.put_u64(key.ts());
        self.data.put_u16(value.len() as u16);
        self.data.put(value);

        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        if self.is_empty() {
            panic!("block should not be empty");
        }
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
