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

// #![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::collections::HashMap;
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::{KeySlice, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::MemTable;
use crate::mvcc::LsmMvccInner;
use crate::mvcc::txn::{Transaction, TxnIterator};
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

/// Whether an SST's `[table_first, table_last]` range can contain anything in
/// `[user_lower, user_upper]`. An `Excluded` bound is one comparison "tighter" than the
/// matching `Included` bound at the same boundary value; `Unbounded` never excludes.
fn range_overlap(
    user_lower: Bound<&[u8]>,
    user_upper: Bound<&[u8]>,
    table_first: &[u8],
    table_last: &[u8],
) -> bool {
    let excluded_by_upper = match user_upper {
        Bound::Included(hi) => hi < table_first,
        Bound::Excluded(hi) => hi <= table_first,
        Bound::Unbounded => false,
    };
    let excluded_by_lower = match user_lower {
        Bound::Included(lo) => lo > table_last,
        Bound::Excluded(lo) => lo >= table_last,
        Bound::Unbounded => false,
    };
    !excluded_by_upper && !excluded_by_lower
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        // Signal both background threads to stop *before* joining either handle — the
        // threads' own loops only ever exit by receiving on this channel, so joining first
        // would wait forever on a thread that has no other way to finish.
        self.flush_notifier.send(()).ok();
        self.compaction_notifier.send(()).ok();

        if let Some(flush_thread) = self.flush_thread.lock().take() {
            flush_thread
                .join()
                .map_err(|e| anyhow::anyhow!("flush thread panicked: {:?}", e))?;
        }
        if let Some(compaction_thread) = self.compaction_thread.lock().take() {
            compaction_thread
                .join()
                .map_err(|e| anyhow::anyhow!("compaction thread panicked: {:?}", e))?;
        }

        if self.inner.options.enable_wal {
            // Every immutable memtable was already synced when it was frozen; only the
            // current one can still have unsynced writes.
            self.inner.sync()?;
        } else {
            if !self.inner.state.read().memtable.is_empty() {
                self.inner
                    .force_freeze_memtable(&self.inner.state_lock.lock())?;
            }
            while !self.inner.state.read().imm_memtables.is_empty() {
                self.inner.force_flush_next_imm_memtable()?;
            }
        }

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<Arc<Transaction>> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }
        let block_cache = Arc::new(BlockCache::new(1024));

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let manifest_path = path.join("MANIFEST");
        let (manifest, mut state, next_sst_id, max_commit_ts) = if !manifest_path.exists() {
            (
                Manifest::create(&manifest_path)?,
                LsmStorageState::create(&options),
                1,
                0,
            )
        } else {
            let (manifest, records) = Manifest::recover(&manifest_path)?;
            let mut state = LsmStorageState::create(&options);
            let mut max_id = 0;
            let mut live_memtable_ids: std::collections::BTreeSet<usize> =
                std::collections::BTreeSet::new();
            for record in records {
                match record {
                    ManifestRecord::Flush(id) => {
                        max_id = max_id.max(id);
                        // A flush retires the memtable of the same id logically: its WAL no
                        // longer needs to be replayed, even if the file hasn't been deleted yet.
                        live_memtable_ids.remove(&id);
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, id);
                        } else {
                            state.levels.insert(0, (id, vec![id]));
                        }
                    }
                    ManifestRecord::Compaction(task, output) => {
                        max_id = max_id.max(output.iter().copied().max().unwrap_or(0));
                        let (new_state, _removed) = compaction_controller
                            .apply_compaction_result(&state, &task, &output, true);
                        state = new_state;
                    }
                    ManifestRecord::NewMemtable(id) => {
                        max_id = max_id.max(id);
                        live_memtable_ids.insert(id);
                    }
                }
            }

            let mut live_ids: Vec<usize> = state.l0_sstables.clone();
            for (_, ids) in &state.levels {
                live_ids.extend(ids.iter().copied());
            }
            // Highest commit timestamp seen across every surviving SST/memtable, so a fresh
            // write after recovery never allocates a timestamp that collides with (or sorts
            // as older than) one already durably stored.
            let mut max_commit_ts = 0u64;
            for id in live_ids {
                let sst_path = Self::path_of_sst_static(path, id);
                let sst =
                    SsTable::open(id, Some(block_cache.clone()), FileObject::open(&sst_path)?)?;
                max_commit_ts = max_commit_ts.max(sst.max_ts());
                state.sstables.insert(id, Arc::new(sst));
            }

            if let CompactionController::Leveled(_) = &compaction_controller {
                let sstables = &state.sstables;
                for (_, ids) in state.levels.iter_mut() {
                    ids.sort_by(|a, b| sstables[a].first_key().cmp(sstables[b].first_key()));
                }
            }

            // Every surviving live memtable becomes immutable, newest id first — matching
            // `force_freeze_memtable`'s own `imm_memtables.insert(0, ...)` convention. The
            // new *current* memtable is always freshly minted below, never one of these.
            for id in live_memtable_ids.iter().rev() {
                let wal_path = Self::path_of_wal_static(path, *id);
                let memtable = MemTable::recover_from_wal(*id, wal_path)?;
                max_commit_ts = max_commit_ts.max(memtable.max_ts());
                state.imm_memtables.push(Arc::new(memtable));
            }

            (manifest, state, max_id + 1, max_commit_ts)
        };

        let next_sst_id = AtomicUsize::new(next_sst_id);
        let new_memtable_id = next_sst_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        state.memtable = if options.enable_wal {
            let memtable = MemTable::create_with_wal(
                new_memtable_id,
                Self::path_of_wal_static(path, new_memtable_id),
            )?;
            File::open(path)?.sync_all()?;
            manifest.add_record_when_init(ManifestRecord::NewMemtable(new_memtable_id))?;
            Arc::new(memtable)
        } else {
            Arc::new(MemTable::create(new_memtable_id))
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id,
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: Some(LsmMvccInner::new(max_commit_ts)),
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        // Every previously-current memtable was already synced the moment it was retired
        // (`force_freeze_memtable`), so the only one that can still have unsynced writes is
        // the current one.
        self.state.read().memtable.sync_wal()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage, reading at a snapshot's `read_ts`.
    ///
    /// A version's timestamp, not only its source, now determines visibility (a single
    /// memtable or SST can hold several timestamped versions of the same user key), so this
    /// can no longer stop at the first source with *any* entry the way the pre-MVCC engine
    /// did. The simplest correct implementation reuses the same merged, version-collapsing
    /// stream as `scan`, narrowed to this one key's version group, with Bloom-filter pruning
    /// kept for the L0 probe.
    pub(crate) fn get_with_ts(&self, key: &[u8], read_ts: u64) -> Result<Option<Bytes>> {
        let iter = self.scan_with_bloom(
            Bound::Included(key),
            Bound::Included(key),
            Some(key),
            read_ts,
        )?;
        if iter.is_valid() {
            Ok(Some(Bytes::copy_from_slice(iter.value())))
        } else {
            Ok(None)
        }
    }

    /// Get a key from the storage at the latest committed snapshot.
    pub fn get(self: &Arc<Self>, key: &[u8]) -> Result<Option<Bytes>> {
        let txn = self.mvcc().new_txn(self.clone(), self.options.serializable);
        txn.get(key)
    }

    /// Shared construction for `get_with_ts`/`scan_with_ts`: builds the merged,
    /// timestamp-aware, version-collapsing iterator over memtables, immutable memtables, L0
    /// SSTs, and every level below L0, bounded by `lower`/`upper` and `read_ts`.
    /// `bloom_probe_key`, when set, additionally skips any L0 SST whose Bloom filter rules
    /// out that exact key — valid only for a single-key point lookup (`get_with_ts`), never
    /// for an arbitrary range (`scan_with_ts`), since a range has no single key to probe.
    fn scan_with_bloom(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        bloom_probe_key: Option<&[u8]>,
        read_ts: u64,
    ) -> Result<FusedIterator<LsmIterator>> {
        // Creating and seeking SST iterators may perform I/O, so snapshot the state and
        // release the read lock before touching any SST.
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        // `SkipMap::range` compares full internal keys directly, so (unlike the
        // one-directional SST seek below) both ends need a real ts sentinel: included ⇒
        // the sentinel that sorts before every version of that key; excluded ⇒ the one that
        // sorts after every version of that key, so the whole group is skipped in one step.
        let mem_lower = match lower {
            Bound::Included(key) => {
                Bound::Included(KeySlice::from_slice_with_ts(key, TS_RANGE_BEGIN))
            }
            Bound::Excluded(key) => {
                Bound::Excluded(KeySlice::from_slice_with_ts(key, TS_RANGE_END))
            }
            Bound::Unbounded => Bound::Unbounded,
        };
        let mem_upper = match upper {
            Bound::Included(key) => {
                Bound::Included(KeySlice::from_slice_with_ts(key, TS_RANGE_END))
            }
            Bound::Excluded(key) => {
                Bound::Excluded(KeySlice::from_slice_with_ts(key, TS_RANGE_BEGIN))
            }
            Bound::Unbounded => Bound::Unbounded,
        };

        let mut memtable_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        memtable_iters.push(Box::new(snapshot.memtable.scan(mem_lower, mem_upper)));
        for memtable in snapshot.imm_memtables.iter() {
            memtable_iters.push(Box::new(memtable.scan(mem_lower, mem_upper)));
        }
        let memtable_iter = MergeIterator::create(memtable_iters);

        let mut sst_iters = Vec::with_capacity(snapshot.l0_sstables.len());
        for sst_id in snapshot.l0_sstables.iter() {
            let sst = snapshot.sstables[sst_id].clone();
            // Skip SSTs whose key range can't overlap the requested range at all.
            if !range_overlap(
                lower,
                upper,
                sst.first_key().key_ref(),
                sst.last_key().key_ref(),
            ) {
                continue;
            }
            // Skip SSTs whose Bloom filter definitively rules out the probed key — no seek,
            // no block read. Only applies to a point lookup, never a range (`bloom_probe_key`
            // is `None` for `scan`).
            if let Some(key) = bloom_probe_key
                && let Some(bloom) = &sst.bloom
                && !bloom.may_contain(farmhash::fingerprint32(key))
            {
                continue;
            }
            let iter = match lower {
                Bound::Included(key) => SsTableIterator::create_and_seek_to_key(
                    sst,
                    KeySlice::from_slice_with_ts(key, TS_RANGE_BEGIN),
                )?,
                Bound::Excluded(key) => {
                    // Seeking the sentinel that sorts *after* every version of `key` skips
                    // the whole group in one lower-bound seek; the extra `.next()` only
                    // fires in the fringe case of a real entry at `ts == TS_RANGE_END`.
                    let mut iter = SsTableIterator::create_and_seek_to_key(
                        sst,
                        KeySlice::from_slice_with_ts(key, TS_RANGE_END),
                    )?;
                    if iter.is_valid() && iter.key().key_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(sst)?,
            };
            sst_iters.push(Box::new(iter));
        }
        let sst_iter = MergeIterator::create(sst_iters);

        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for (_, level_sst_ids) in snapshot.levels.iter() {
            let level_ssts: Vec<Arc<SsTable>> = level_sst_ids
                .iter()
                .filter_map(|id| {
                    let sst = snapshot.sstables[id].clone();
                    if range_overlap(
                        lower,
                        upper,
                        sst.first_key().key_ref(),
                        sst.last_key().key_ref(),
                    ) {
                        Some(sst)
                    } else {
                        None
                    }
                })
                .collect();
            let iter = match lower {
                Bound::Included(key) => SstConcatIterator::create_and_seek_to_key(
                    level_ssts,
                    KeySlice::from_slice_with_ts(key, TS_RANGE_BEGIN),
                )?,
                Bound::Excluded(key) => {
                    let mut iter = SstConcatIterator::create_and_seek_to_key(
                        level_ssts,
                        KeySlice::from_slice_with_ts(key, TS_RANGE_END),
                    )?;
                    if iter.is_valid() && iter.key().key_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(level_ssts)?,
            };
            level_iters.push(Box::new(iter));
        }
        let levels_iter = MergeIterator::create(level_iters);

        let inner = TwoMergeIterator::create(
            TwoMergeIterator::create(memtable_iter, sst_iter)?,
            levels_iter,
        )?;
        let end_bound = match upper {
            Bound::Included(key) => Bound::Included(Bytes::copy_from_slice(key)),
            Bound::Excluded(key) => Bound::Excluded(Bytes::copy_from_slice(key)),
            Bound::Unbounded => Bound::Unbounded,
        };
        Ok(FusedIterator::new(LsmIterator::new(
            inner, end_bound, read_ts,
        )?))
    }

    /// Writes a batch of records as one commit and returns the commit timestamp it was
    /// published under: every record receives the same, freshly allocated ts, and the write
    /// lock covers allocation, the memtable/WAL update, and publishing the new latest commit
    /// timestamp, so concurrent batches can never reuse or interleave timestamps. This is the
    /// one place that actually touches the memtable/WAL — both the non-serializable path and
    /// `Transaction::commit` (which needs the real ts back, to key its `committed_txns`
    /// record) call into this directly, never into the public `write_batch` below.
    pub(crate) fn write_batch_inner<T: AsRef<[u8]>>(
        &self,
        batch: &[WriteBatchRecord<T>],
    ) -> Result<u64> {
        let (ts, size_after) = {
            let _write_lock = self.mvcc().write_lock.lock();
            let ts = self.mvcc().latest_commit_ts() + 1;
            let state = self.state.read().clone();
            let data: Vec<(KeySlice, &[u8])> = batch
                .iter()
                .map(|record| match record {
                    WriteBatchRecord::Put(key, value) => (
                        KeySlice::from_slice_with_ts(key.as_ref(), ts),
                        value.as_ref(),
                    ),
                    WriteBatchRecord::Del(key) => {
                        (KeySlice::from_slice_with_ts(key.as_ref(), ts), &[][..])
                    }
                })
                .collect();
            // One `put_batch` call for the whole batch, not one `put` per record: every
            // record lands in the same memtable and the same WAL frame, even though they
            // all share this one commit ts regardless.
            state.memtable.put_batch(&data)?;
            self.mvcc().update_commit_ts(ts);
            (ts, state.memtable.approximate_size())
        };

        if size_after >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            if self.state.read().memtable.approximate_size() >= self.options.target_sst_size {
                self.force_freeze_memtable(&state_lock)?;
            }
        }

        Ok(ts)
    }

    /// Write a batch of records as one commit. When `options.serializable`, every write
    /// (even this "simple" batch form) is routed through a real `Transaction::commit` — so
    /// there is no write path left that can allocate a commit ts without also going through
    /// serializable validation under `commit_lock`; a direct call to `write_batch_inner`
    /// here would be exactly the gap that lets a concurrent writer sneak in unvalidated.
    pub fn write_batch<T: AsRef<[u8]>>(
        self: &Arc<Self>,
        batch: &[WriteBatchRecord<T>],
    ) -> Result<()> {
        if self.options.serializable {
            let txn = self.new_txn()?;
            for record in batch {
                match record {
                    WriteBatchRecord::Put(key, value) => txn.put(key.as_ref(), value.as_ref()),
                    WriteBatchRecord::Del(key) => txn.delete(key.as_ref()),
                }
            }
            txn.commit()
        } else {
            self.write_batch_inner(batch)?;
            Ok(())
        }
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(self: &Arc<Self>, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(key, value)])
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(self: &Arc<Self>, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(key)])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let new_memtable_id = self.next_sst_id();
        let new_memtable = if self.options.enable_wal {
            let memtable =
                MemTable::create_with_wal(new_memtable_id, self.path_of_wal(new_memtable_id))?;
            self.sync_dir()?;
            Arc::new(memtable)
        } else {
            Arc::new(MemTable::create(new_memtable_id))
        };

        let old_memtable = {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            let old_memtable = std::mem::replace(&mut snapshot.memtable, new_memtable);
            snapshot.imm_memtables.insert(0, old_memtable.clone());
            *guard = Arc::new(snapshot);
            old_memtable
        };
        // The old memtable is being retired from "current": sync its WAL now, so that a
        // future top-level `sync()` (which only syncs the *new* current memtable's WAL) is
        // still a complete durability guarantee for everything written before this freeze.
        old_memtable.sync_wal()?;

        if self.options.enable_wal {
            self.manifest.as_ref().unwrap().add_record(
                _state_lock_observer,
                ManifestRecord::NewMemtable(new_memtable_id),
            )?;
        }

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let _state_lock = self.state_lock.lock();

        // The oldest immutable memtable sits at the back (`force_freeze_memtable` always
        // inserts new ones at the front). Another flush may have already drained the list
        // between the caller's own check and this one, so recheck under `state_lock`.
        let memtable_to_flush = {
            let guard = self.state.read();
            let Some(memtable) = guard.imm_memtables.last() else {
                return Ok(());
            };
            memtable.clone()
        };

        // Building the SST is I/O-heavy, so it happens outside the `state` lock entirely.
        let sst_id = memtable_to_flush.id();
        let mut builder = SsTableBuilder::new(self.options.block_size);
        memtable_to_flush.flush(&mut builder)?;
        let sst = Arc::new(builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?);
        // The SST's own bytes are already synced by `FileObject::create`; sync the
        // directory too so the filename itself is durable before the manifest can
        // reference it.
        self.sync_dir()?;

        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            let removed = snapshot.imm_memtables.pop().unwrap();
            // Catches a flush racing against something that changed the back of
            // `imm_memtables` between selecting this memtable and installing the result.
            assert_eq!(removed.id(), sst_id, "flushed the wrong memtable");
            if self.compaction_controller.flush_to_l0() {
                snapshot.l0_sstables.insert(0, sst_id);
            } else {
                // Tiered compaction has no L0: each flush becomes a new one-SST tier.
                snapshot.levels.insert(0, (sst_id, vec![sst_id]));
            }
            snapshot.sstables.insert(sst_id, sst);
            *guard = Arc::new(snapshot);
            self.manifest
                .as_ref()
                .unwrap()
                .add_record(&_state_lock, ManifestRecord::Flush(sst_id))?;
        }

        Ok(())
    }

    pub fn new_txn(self: &Arc<Self>) -> Result<Arc<Transaction>> {
        Ok(self.mvcc().new_txn(self.clone(), self.options.serializable))
    }

    /// Get an iterator over a range of keys at a snapshot's `read_ts`.
    pub(crate) fn scan_with_ts(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        read_ts: u64,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.scan_with_bloom(lower, upper, None, read_ts)
    }

    /// Create an iterator over a range of keys at the latest committed snapshot.
    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        let txn = self.mvcc().new_txn(self.clone(), self.options.serializable);
        txn.scan(lower, upper)
    }
}
