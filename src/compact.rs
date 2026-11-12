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

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    /// Drains `iter` into one or more new SSTs, splitting on `target_sst_size`. Versions above
    /// `watermark` are always kept (some live snapshot may still need them); for a given key's
    /// run of versions at or below `watermark`, only the newest is kept — relying on `Key::Ord`
    /// sorting `(key asc, ts desc)`, so the first `ts <= watermark` entry seen for a key is
    /// necessarily that key's newest surviving version, and the stream only ever decreases from
    /// there until the key changes. That lone survivor is dropped entirely (not written out) if
    /// it's a tombstone and `compact_to_bottom_level` — only the bottom level has no level below
    /// it where an older value could resurface once the tombstone stops shadowing it.
    ///
    /// Splitting only ever happens right after crossing into a new user key, never in the
    /// middle of one key's version history — so every timestamped version of a key is
    /// guaranteed to land in the same output SST, even if that makes the SST exceed
    /// `target_sst_size`. This is what lets later code assume "a key found in one SST in a
    /// level is not in any other SST in that level."
    fn compact_generate_sst_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom_level: bool,
        watermark: u64,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut new_ssts = Vec::new();
        let mut builder = SsTableBuilder::new(self.options.block_size);
        let mut builder_has_entries = false;
        let mut seen_any_key = false;
        let mut last_key: Vec<u8> = Vec::new();
        let mut kept_watermark_version_for_key = false;

        while iter.is_valid() {
            let key = iter.key();
            let value = iter.value();
            let is_new_key = !seen_any_key || key.key_ref() != last_key.as_slice();

            if is_new_key {
                if builder_has_entries && builder.estimated_size() >= self.options.target_sst_size {
                    let id = self.next_sst_id();
                    let finished = std::mem::replace(
                        &mut builder,
                        SsTableBuilder::new(self.options.block_size),
                    );
                    new_ssts.push(Arc::new(finished.build(
                        id,
                        Some(self.block_cache.clone()),
                        self.path_of_sst(id),
                    )?));
                    builder_has_entries = false;
                }
                last_key = key.key_ref().to_vec();
                kept_watermark_version_for_key = false;
            }
            seen_any_key = true;

            let keep = if key.ts() > watermark {
                true
            } else if !kept_watermark_version_for_key {
                kept_watermark_version_for_key = true;
                true
            } else {
                false
            };

            if keep && !(key.ts() <= watermark && compact_to_bottom_level && value.is_empty()) {
                builder.add(key, value);
                builder_has_entries = true;
            }
            iter.next()?;
        }
        if builder_has_entries {
            let id = self.next_sst_id();
            new_ssts.push(Arc::new(builder.build(
                id,
                Some(self.block_cache.clone()),
                self.path_of_sst(id),
            )?));
        }
        Ok(new_ssts)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let compact_to_bottom_level = task.compact_to_bottom_level();
        let watermark = self.mvcc().watermark();
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let snapshot = self.state.read().clone();
                let mut iters: Vec<Box<SsTableIterator>> =
                    Vec::with_capacity(l0_sstables.len() + l1_sstables.len());
                for id in l0_sstables.iter().chain(l1_sstables.iter()) {
                    let sst = snapshot.sstables[id].clone();
                    iters.push(Box::new(SsTableIterator::create_and_seek_to_first(sst)?));
                }
                // Full compaction captures every SST that exists at this stage (all of L0
                // and all of L1), so no older, un-merged version of any key survives
                // elsewhere; tombstones are safe to drop instead of being carried forward.
                let merge_iter = MergeIterator::create(iters);
                self.compact_generate_sst_from_iter(merge_iter, compact_to_bottom_level, watermark)
            }
            CompactionTask::Simple(simple_task) => {
                let snapshot = self.state.read().clone();
                let lower_ssts: Vec<Arc<SsTable>> = simple_task
                    .lower_level_sst_ids
                    .iter()
                    .map(|id| snapshot.sstables[id].clone())
                    .collect();
                let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                match simple_task.upper_level {
                    // L0 may overlap, so each file needs its own iterator, merged by priority.
                    None => {
                        let mut l0_iters: Vec<Box<SsTableIterator>> =
                            Vec::with_capacity(simple_task.upper_level_sst_ids.len());
                        for id in simple_task.upper_level_sst_ids.iter() {
                            let sst = snapshot.sstables[id].clone();
                            l0_iters
                                .push(Box::new(SsTableIterator::create_and_seek_to_first(sst)?));
                        }
                        let upper_iter = MergeIterator::create(l0_iters);
                        let merged = TwoMergeIterator::create(upper_iter, lower_iter)?;
                        self.compact_generate_sst_from_iter(
                            merged,
                            compact_to_bottom_level,
                            watermark,
                        )
                    }
                    // A non-L0 level is already a sorted, non-overlapping run.
                    Some(_) => {
                        let upper_ssts: Vec<Arc<SsTable>> = simple_task
                            .upper_level_sst_ids
                            .iter()
                            .map(|id| snapshot.sstables[id].clone())
                            .collect();
                        let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_ssts)?;
                        let merged = TwoMergeIterator::create(upper_iter, lower_iter)?;
                        self.compact_generate_sst_from_iter(
                            merged,
                            compact_to_bottom_level,
                            watermark,
                        )
                    }
                }
            }
            CompactionTask::Tiered(tiered_task) => {
                let snapshot = self.state.read().clone();
                // Tiers can overlap with each other (unlike levels within one leveled
                // level), so they need priority-based merging, not concatenation.
                let mut tier_iters: Vec<Box<SstConcatIterator>> =
                    Vec::with_capacity(tiered_task.tiers.len());
                for (_, sst_ids) in tiered_task.tiers.iter() {
                    let ssts: Vec<Arc<SsTable>> = sst_ids
                        .iter()
                        .map(|id| snapshot.sstables[id].clone())
                        .collect();
                    tier_iters.push(Box::new(SstConcatIterator::create_and_seek_to_first(ssts)?));
                }
                let merged = MergeIterator::create(tier_iters);
                self.compact_generate_sst_from_iter(merged, compact_to_bottom_level, watermark)
            }
            CompactionTask::Leveled(leveled_task) => {
                let snapshot = self.state.read().clone();
                let lower_ssts: Vec<Arc<SsTable>> = leveled_task
                    .lower_level_sst_ids
                    .iter()
                    .map(|id| snapshot.sstables[id].clone())
                    .collect();
                let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                match leveled_task.upper_level {
                    // L0 may overlap, so each file needs its own iterator, merged by priority.
                    None => {
                        let mut l0_iters: Vec<Box<SsTableIterator>> =
                            Vec::with_capacity(leveled_task.upper_level_sst_ids.len());
                        for id in leveled_task.upper_level_sst_ids.iter() {
                            let sst = snapshot.sstables[id].clone();
                            l0_iters
                                .push(Box::new(SsTableIterator::create_and_seek_to_first(sst)?));
                        }
                        let upper_iter = MergeIterator::create(l0_iters);
                        let merged = TwoMergeIterator::create(upper_iter, lower_iter)?;
                        self.compact_generate_sst_from_iter(
                            merged,
                            compact_to_bottom_level,
                            watermark,
                        )
                    }
                    // Task 1.4 always selects exactly one upper SST, so no merge needed.
                    Some(_) => {
                        let upper_id = leveled_task.upper_level_sst_ids[0];
                        let upper_sst = snapshot.sstables[&upper_id].clone();
                        let upper_iter = SsTableIterator::create_and_seek_to_first(upper_sst)?;
                        let merged = TwoMergeIterator::create(upper_iter, lower_iter)?;
                        self.compact_generate_sst_from_iter(
                            merged,
                            compact_to_bottom_level,
                            watermark,
                        )
                    }
                }
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0_sstables, l1_sstables) = {
            let state = self.state.read();
            (state.l0_sstables.clone(), state.levels[0].1.clone())
        };
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };
        // Merging and writing output happens with no lock held at all, so concurrent
        // flushes can keep inserting new L0 files while this runs.
        let new_ssts = self.compact(&task)?;
        let new_sst_ids: Vec<usize> = new_ssts.iter().map(|sst| sst.sst_id()).collect();
        // Each new SST's bytes are already synced individually; sync the directory too
        // so the filenames are durable before the manifest can reference them.
        self.sync_dir()?;

        {
            let _state_lock = self.state_lock.lock();
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            // Remove only the ids this task captured; anything inserted concurrently
            // (e.g. a flush finishing mid-compaction) is left untouched.
            snapshot.l0_sstables.retain(|id| !l0_sstables.contains(id));
            for id in l0_sstables.iter().chain(l1_sstables.iter()) {
                snapshot.sstables.remove(id);
            }
            for sst in &new_ssts {
                snapshot.sstables.insert(sst.sst_id(), sst.clone());
            }
            snapshot.levels[0].1 = new_sst_ids.clone();
            *guard = Arc::new(snapshot);
            self.manifest
                .as_ref()
                .unwrap()
                .add_record(&_state_lock, ManifestRecord::Compaction(task, new_sst_ids))?;
        }

        for id in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(*id))?;
        }
        self.sync_dir()?;

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = self.state.read().clone();
        let Some(task) = self
            .compaction_controller
            .generate_compaction_task(&snapshot)
        else {
            return Ok(());
        };
        // Merging and writing output happens with no lock held at all, so concurrent
        // flushes can keep inserting new L0 files while this runs.
        let new_ssts = self.compact(&task)?;
        let output_ids: Vec<usize> = new_ssts.iter().map(|sst| sst.sst_id()).collect();
        // Each new SST's bytes are already synced individually; sync the directory too
        // so the filenames are durable before the manifest can reference them.
        self.sync_dir()?;

        let removed_ids =
            {
                let _state_lock = self.state_lock.lock();
                // Re-read fresh: a flush may have landed on L0 while `compact` was running,
                // and `apply_compaction_result` must see it so it survives into the new state.
                // Insert the newly built SSTs *before* calling `apply_compaction_result`
                // (not after): the Leveled controller looks up `first_key()` through this
                // map to keep the lower level sorted, and it needs to see these ids too.
                let mut current = self.state.read().as_ref().clone();
                for sst in &new_ssts {
                    current.sstables.insert(sst.sst_id(), sst.clone());
                }
                let (mut new_state, removed_ids) = self
                    .compaction_controller
                    .apply_compaction_result(&current, &task, &output_ids, false);
                for id in &removed_ids {
                    new_state.sstables.remove(id);
                }
                let mut guard = self.state.write();
                *guard = Arc::new(new_state);
                self.manifest
                    .as_ref()
                    .unwrap()
                    .add_record(&_state_lock, ManifestRecord::Compaction(task, output_ids))?;
                removed_ids
            };

        for id in removed_ids {
            std::fs::remove_file(self.path_of_sst(id))?;
        }
        self.sync_dir()?;

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let should_flush = {
            let guard = self.state.read();
            guard.imm_memtables.len() >= self.options.num_memtable_limit
        };
        if should_flush {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
