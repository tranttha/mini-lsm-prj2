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

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::BufMut;

use super::bloom::Bloom;
use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeySlice, KeyVec},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: KeyVec,
    last_key: KeyVec,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
    max_ts: u64,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
            key_hashes: Vec::new(),
            max_ts: 0,
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        // Hashed on user-key bytes only: every timestamped version of one user key must
        // set the same bits, since a point lookup only knows the user key, not which
        // version's timestamp happens to be stored.
        self.key_hashes.push(farmhash::fingerprint32(key.key_ref()));
        self.max_ts = self.max_ts.max(key.ts());
        if self.builder.add(key, value) {
            if self.first_key.is_empty() {
                self.first_key.set_from_slice(key);
            }
            self.last_key.set_from_slice(key);
            return;
        }

        // The current block is full: seal it (using the first/last key it actually
        // holds) before `first_key`/`last_key` get reassigned to describe the new block.
        self.finish_block();

        let added = self.builder.add(key, value);
        debug_assert!(added, "a freshly created block must accept its first entry");
        self.first_key.set_from_slice(key);
        self.last_key.set_from_slice(key);
    }

    /// Finalizes the current (non-empty) block builder into `self.data`/`self.meta`,
    /// and replaces it with a fresh, empty `BlockBuilder`.
    fn finish_block(&mut self) {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let encoded = builder.build().encode();
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: self.first_key.clone().into_key_bytes(),
            last_key: self.last_key.clone().into_key_bytes(),
        });
        self.data.extend_from_slice(&encoded);
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        if !self.builder.is_empty() {
            self.finish_block();
        }
        let block_meta_offset = self.data.len();
        BlockMeta::encode_block_meta(&self.meta, &mut self.data);
        self.data.put_u32(block_meta_offset as u32);

        let bits_per_key = Bloom::bloom_bits_per_key(self.key_hashes.len(), 0.01);
        let bloom = Bloom::build_from_key_hashes(&self.key_hashes, bits_per_key);
        let bloom_offset = self.data.len();
        bloom.encode(&mut self.data);
        self.data.put_u32(bloom_offset as u32);
        self.data.put_u64(self.max_ts);

        let first_key = self.meta.first().unwrap().first_key.clone();
        let last_key = self.meta.last().unwrap().last_key.clone();
        let file = FileObject::create(path.as_ref(), self.data)?;
        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
            max_ts: self.max_ts,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
