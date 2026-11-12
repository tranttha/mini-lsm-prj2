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

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        _snapshot: &LsmStorageState,
        _sst_ids: &[usize],
        _in_level: usize,
    ) -> Vec<usize> {
        let begin = _sst_ids
            .iter()
            .map(|id| _snapshot.sstables[id].first_key())
            .min()
            .unwrap();
        let end = _sst_ids
            .iter()
            .map(|id| _snapshot.sstables[id].last_key())
            .max()
            .unwrap();
        _snapshot.levels[_in_level - 1]
            .1
            .iter()
            .filter(|id| {
                let sst = &_snapshot.sstables[id];
                sst.first_key() <= end && sst.last_key() >= begin
            })
            .copied()
            .collect()
    }

    /// Target size of each level, in bytes, indexed by `level - 1`.
    fn compute_target_sizes(&self, _snapshot: &LsmStorageState) -> Vec<u64> {
        let base_level_size = self.options.base_level_size_mb as u64 * 1024 * 1024;
        let mut target_sizes = vec![0u64; self.options.max_levels];

        let bottom_idx = self.options.max_levels - 1;
        let real_bottom_size: u64 = _snapshot.levels[bottom_idx]
            .1
            .iter()
            .map(|id| _snapshot.sstables[id].table_size())
            .sum();
        target_sizes[bottom_idx] = real_bottom_size.max(base_level_size);

        for i in (0..bottom_idx).rev() {
            let below = target_sizes[i + 1];
            target_sizes[i] = if below > base_level_size {
                below / self.options.level_size_multiplier as u64
            } else {
                0
            };
        }
        target_sizes
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let target_sizes = self.compute_target_sizes(_snapshot);
        // First level (from L1) with a positive target; L0 always compacts directly here.
        let base_level = (0..self.options.max_levels)
            .find(|&i| target_sizes[i] > 0)
            .map(|i| i + 1)
            .unwrap();

        if _snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            let lower_level_sst_ids =
                self.find_overlapping_ssts(_snapshot, &_snapshot.l0_sstables, base_level);
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids,
                is_lower_level_bottom_level: base_level == self.options.max_levels,
            });
        }

        let mut best: Option<(usize, f64)> = None;
        for level in base_level..=self.options.max_levels {
            let target = target_sizes[level - 1];
            let real_size: u64 = _snapshot.levels[level - 1]
                .1
                .iter()
                .map(|id| _snapshot.sstables[id].table_size())
                .sum();
            if real_size <= target {
                continue;
            }
            let priority = real_size as f64 / target as f64;
            if best.is_none_or(|(_, best_priority)| priority >= best_priority) {
                best = Some((level, priority));
            }
        }

        let (level, _) = best?;
        let upper_level_sst_ids = vec![*_snapshot.levels[level - 1].1.iter().min().unwrap()];
        let lower_level = level + 1;
        let lower_level_sst_ids =
            self.find_overlapping_ssts(_snapshot, &upper_level_sst_ids, lower_level);
        Some(LeveledCompactionTask {
            upper_level: Some(level),
            upper_level_sst_ids,
            lower_level,
            lower_level_sst_ids,
            is_lower_level_bottom_level: lower_level == self.options.max_levels,
        })
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &LeveledCompactionTask,
        _output: &[usize],
        _in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();

        match _task.upper_level {
            // L0 can receive a concurrent flush between task generation and
            // installation, so remove only the ids this task actually captured.
            None => {
                snapshot
                    .l0_sstables
                    .retain(|id| !_task.upper_level_sst_ids.contains(id));
            }
            Some(upper_level) => {
                snapshot.levels[upper_level - 1]
                    .1
                    .retain(|id| !_task.upper_level_sst_ids.contains(id));
            }
        }

        let lower_idx = _task.lower_level - 1;
        let mut new_lower: Vec<usize> = snapshot.levels[lower_idx]
            .1
            .iter()
            .filter(|id| !_task.lower_level_sst_ids.contains(id))
            .copied()
            .collect();
        new_lower.extend(_output.iter().copied());
        // During manifest replay, `snapshot.sstables` hasn't been populated yet (SST
        // files aren't opened until a later recovery phase), so `first_key()` isn't
        // available; leave the order as-is and let that later phase sort it.
        if !_in_recovery {
            new_lower.sort_by(|a, b| {
                snapshot.sstables[a]
                    .first_key()
                    .cmp(snapshot.sstables[b].first_key())
            });
        }
        snapshot.levels[lower_idx].1 = new_lower;

        let mut removed_ids = _task.upper_level_sst_ids.clone();
        removed_ids.extend(_task.lower_level_sst_ids.iter().copied());
        (snapshot, removed_ids)
    }
}
