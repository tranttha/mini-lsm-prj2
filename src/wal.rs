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

use anyhow::{Result, bail};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::{KeyBytes, KeySlice};

const SIZEOF_U16: usize = std::mem::size_of::<u16>();
const SIZEOF_U32: usize = std::mem::size_of::<u32>();
const SIZEOF_U64: usize = std::mem::size_of::<u64>();

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new().create_new(true).write(true).open(path)?;
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let mut file = OpenOptions::new().read(true).append(true).open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        // Tracks the byte offset right after the last *fully validated* frame. Truncation
        // branches below return without advancing it, so it never includes an incomplete
        // trailing frame's bytes; a corruption branch (`bail!`) skips the final `set_len`
        // entirely, leaving the file untouched rather than attempting to repair it.
        let mut consumed = 0usize;
        loop {
            let rest = &buf[consumed..];
            if rest.is_empty() {
                break;
            }
            if rest.len() < SIZEOF_U32 {
                eprintln!("WAL: discarding truncated frame header at tail");
                break;
            }
            let batch_size = (&rest[..SIZEOF_U32]).get_u32() as usize;
            let frame_len = SIZEOF_U32 + batch_size + SIZEOF_U32;
            if rest.len() < frame_len {
                // The frame's body and/or checksum never finished landing on disk (a crash
                // mid-write of this transaction's batch): every earlier, already-validated
                // frame is still durable and already applied below, so stop here rather than
                // failing recovery over debris from a write that never completed.
                eprintln!("WAL: discarding truncated trailing batch frame");
                break;
            }
            let body = &rest[SIZEOF_U32..SIZEOF_U32 + batch_size];
            let checksum = (&rest[SIZEOF_U32 + batch_size..frame_len]).get_u32();
            if checksum != farmhash::fingerprint32(body) {
                // The frame is fully present on disk but its contents don't check out: this
                // is corruption, not truncation, so it's a hard error rather than a silent
                // stop — distinct from the "not enough bytes present" cases above.
                bail!("WAL batch checksum mismatch: corrupted record");
            }
            // Parse the whole frame into a buffer first; only insert into `skiplist` once
            // every record in it has decoded successfully, so a structurally-broken frame
            // (despite a checksum match) can never apply even its first record.
            let mut body_rest = body;
            let mut records = Vec::new();
            while !body_rest.is_empty() {
                if body_rest.len() < SIZEOF_U16 {
                    bail!("WAL batch corrupt: truncated key length");
                }
                let key_len = body_rest.get_u16() as usize;
                if body_rest.len() < key_len + SIZEOF_U64 + SIZEOF_U16 {
                    bail!("WAL batch corrupt: key/timestamp/value-length overruns batch body");
                }
                let key = Bytes::copy_from_slice(&body_rest[..key_len]);
                body_rest.advance(key_len);
                let ts = body_rest.get_u64();
                let value_len = body_rest.get_u16() as usize;
                if body_rest.len() < value_len {
                    bail!("WAL batch corrupt: value overruns batch body");
                }
                let value = Bytes::copy_from_slice(&body_rest[..value_len]);
                body_rest.advance(value_len);
                records.push((KeyBytes::from_bytes_with_ts(key, ts), value));
            }
            for (key, value) in records {
                skiplist.insert(key, value);
            }
            consumed += frame_len;
        }
        // Drop any incomplete trailing frame from disk too, not just from the in-memory
        // replay: left in place, it would sit between this point and whatever gets appended
        // next, and a *future* recovery pass would see real data after it instead of a clean
        // EOF — making stale garbage from this crash look like corruption of a later, good
        // write (see the two-recovery-passes trace this was derived from).
        file.set_len(consumed as u64)?;
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Encodes every record into one `batch_size(u32) | body | checksum(u32)` frame, where
    /// `body` is the concatenation of `key_len(u16) | key | ts(u64) | value_len(u16) | value`
    /// for each record and the checksum covers the complete body. One checksum for the whole
    /// frame (rather than one per record, as the pre-Day-5 single-record format had) is what
    /// makes the frame atomic on recovery: a crash can never leave a validated prefix of one
    /// transaction's records sitting next to an unvalidated suffix of that same transaction.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut body = Vec::new();
        for (key, value) in data {
            if key.key_len() > u16::MAX as usize {
                bail!("key too large to encode in the WAL");
            }
            if value.len() > u16::MAX as usize {
                bail!("value too large to encode in the WAL");
            }
            body.put_u16(key.key_len() as u16);
            body.put_slice(key.key_ref());
            body.put_u64(key.ts());
            body.put_u16(value.len() as u16);
            body.put_slice(value);
        }
        if body.len() > u32::MAX as usize {
            bail!("batch too large to encode in the WAL");
        }
        let checksum = farmhash::fingerprint32(&body);
        let mut frame = Vec::with_capacity(SIZEOF_U32 + body.len() + SIZEOF_U32);
        frame.put_u32(body.len() as u32);
        frame.put_slice(&body);
        frame.put_u32(checksum);
        self.file.lock().write_all(&frame)?;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
