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

use std::{
    collections::HashSet,
    ops::Bound,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Result, bail};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{StorageIterator, two_merge_iterator::TwoMergeIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
    mvcc::CommittedTxnData,
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        if self.committed.load(Ordering::SeqCst) {
            bail!("cannot operate on a committed transaction");
        }
        if let Some(key_hashes) = &self.key_hashes {
            // Record the read even on a miss: the key's *absence* still influenced this
            // transaction's behavior, and a later transaction inserting it is exactly the
            // kind of dependency serializable validation needs to catch.
            key_hashes.lock().1.insert(farmhash::hash32(key));
        }
        // A transaction's own (uncommitted) writes must shadow whatever the engine has
        // committed for the same key.
        if let Some(entry) = self.local_storage.get(key) {
            return Ok(if entry.value().is_empty() {
                None
            } else {
                Some(entry.value().clone())
            });
        }
        self.inner.get_with_ts(key, self.read_ts)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        if self.committed.load(Ordering::SeqCst) {
            bail!("cannot operate on a committed transaction");
        }
        let local_iter = create_txn_local_iterator(self.local_storage.clone(), lower, upper);
        let lsm_iter = self.inner.scan_with_ts(lower, upper, self.read_ts)?;
        let iter = TwoMergeIterator::create(local_iter, lsm_iter)?;
        TxnIterator::create(self.clone(), iter)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) {
        assert!(
            !self.committed.load(Ordering::SeqCst),
            "cannot operate on a committed transaction"
        );
        if let Some(key_hashes) = &self.key_hashes {
            key_hashes.lock().0.insert(farmhash::hash32(key));
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    pub fn delete(&self, key: &[u8]) {
        assert!(
            !self.committed.load(Ordering::SeqCst),
            "cannot operate on a committed transaction"
        );
        if let Some(key_hashes) = &self.key_hashes {
            key_hashes.lock().0.insert(farmhash::hash32(key));
        }
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::new());
    }

    pub fn commit(&self) -> Result<()> {
        // Mark unusable *before* publishing: once `write_batch_inner` is attempted, this
        // handle is spent even on failure — retrying would re-gather the same `local_storage`
        // and submit it again under a fresh commit ts, risking two on-disk versions for one
        // logical write.
        if self.committed.swap(true, Ordering::SeqCst) {
            bail!("transaction has already committed");
        }
        // Empty write batch: return without allocating a commit ts or touching
        // `commit_lock`/`committed_txns` at all — there is nothing to validate or publish.
        if self.local_storage.is_empty() {
            return Ok(());
        }

        // `commit_lock` has to span validation, the real ts allocation inside
        // `write_batch_inner`, and the `committed_txns` insert as one critical section:
        // releasing it in between would let a concurrent committer validate against the same
        // stale `committed_txns`/`latest_commit_ts` this transaction just checked, each
        // oblivious to the other's still-unpublished write — reintroducing write skew even
        // though both transactions individually "pass" validation.
        let _commit_lock = self
            .key_hashes
            .as_ref()
            .map(|_| self.inner.mvcc().commit_lock.lock());

        if let Some(key_hashes) = &self.key_hashes {
            let guard = key_hashes.lock();
            let (write_set, read_set) = &*guard;
            // Invariant 5: a transaction that never wrote anything through `put`/`delete`
            // has no write set to register, so it can never conflict with anyone else's
            // reads — skip validation entirely rather than compare against an empty set.
            if !write_set.is_empty() {
                let expected_commit_ts = self.inner.mvcc().latest_commit_ts() + 1;
                let committed_txns = self.inner.mvcc().committed_txns.lock();
                for (_, txn_data) in committed_txns.range((self.read_ts + 1)..expected_commit_ts) {
                    if read_set.iter().any(|h| txn_data.key_hashes.contains(h)) {
                        bail!(
                            "serializable validation failed: read set overlaps a concurrently committed write set"
                        );
                    }
                }
            }
        }

        let batch: Vec<WriteBatchRecord<Bytes>> = self
            .local_storage
            .iter()
            .map(|entry| {
                if entry.value().is_empty() {
                    WriteBatchRecord::Del(entry.key().clone())
                } else {
                    WriteBatchRecord::Put(entry.key().clone(), entry.value().clone())
                }
            })
            .collect();
        let commit_ts = self.inner.write_batch_inner(&batch)?;

        if let Some(key_hashes) = &self.key_hashes {
            let write_set = &key_hashes.lock().0;
            if !write_set.is_empty() {
                let mut committed_txns = self.inner.mvcc().committed_txns.lock();
                committed_txns.insert(
                    commit_ts,
                    CommittedTxnData {
                        key_hashes: write_set.clone(),
                        read_ts: self.read_ts,
                        commit_ts,
                    },
                );
                // No future transaction can start with a `read_ts` older than the current
                // watermark, so a record with `commit_ts` strictly below it can never again
                // fall inside any validation's `(read_ts, expected_commit_ts)` window.
                let watermark = self.inner.mvcc().watermark();
                committed_txns.retain(|&ts, _| ts >= watermark);
            }
        }

        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        // Unregister this transaction's read_ts: it no longer needs any version protected.
        self.inner.mvcc().ts.lock().1.remove_reader(self.read_ts);
    }
}

/// Create a bound of `Bytes` from a bound of `&[u8]`, mirroring `mem_table::map_bound` but
/// without a timestamp — `local_storage` holds only a transaction's own uncommitted writes,
/// latest-wins per key, with no version history to preserve.
fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn create_txn_local_iterator(
    map: Arc<SkipMap<Bytes, Bytes>>,
    lower: Bound<&[u8]>,
    upper: Bound<&[u8]>,
) -> TxnLocalIterator {
    let (lower, upper) = (map_bound(lower), map_bound(upper));
    let mut iter = TxnLocalIteratorBuilder {
        map,
        iter_builder: |map| map.range((lower, upper)),
        item: (Bytes::new(), Bytes::new()),
    }
    .build();
    let entry = iter.with_iter_mut(|iter| TxnLocalIterator::entry_to_item(iter.next()));
    iter.with_mut(|x| *x.item = entry);
    iter
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl TxnLocalIterator {
    fn entry_to_item(
        entry: Option<crossbeam_skiplist::map::Entry<'_, Bytes, Bytes>>,
    ) -> (Bytes, Bytes) {
        entry
            .map(|x| (x.key().clone(), x.value().clone()))
            .unwrap_or_else(|| (Bytes::new(), Bytes::new()))
    }
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn value(&self) -> &[u8] {
        &self.borrow_item().1[..]
    }

    fn key(&self) -> &[u8] {
        &self.borrow_item().0[..]
    }

    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let entry = self.with_iter_mut(|iter| TxnLocalIterator::entry_to_item(iter.next()));
        self.with_mut(|x| *x.item = entry);
        Ok(())
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        let mut txn_iter = Self { txn, iter };
        // `TxnLocalIterator` has no tombstone concept of its own (an empty value just means
        // "deleted within this transaction"); skip any such entry up front, same as
        // `LsmIterator` does internally for the committed side.
        while txn_iter.iter.is_valid() && txn_iter.iter.value().is_empty() {
            txn_iter.iter.next()?;
        }
        txn_iter.record_read();
        Ok(txn_iter)
    }

    /// Records the key this iterator is currently sitting on into the transaction's read
    /// set — only the keys actually surfaced by this scan, not the gaps between them (that
    /// gap-blindness is exactly the documented scan-phantom limitation this chapter leaves
    /// unresolved).
    fn record_read(&self) {
        if !self.iter.is_valid() {
            return;
        }
        if let Some(key_hashes) = &self.txn.key_hashes {
            key_hashes
                .lock()
                .1
                .insert(farmhash::hash32(self.iter.key()));
        }
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        while self.iter.is_valid() && self.iter.value().is_empty() {
            self.iter.next()?;
        }
        self.record_read();
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
