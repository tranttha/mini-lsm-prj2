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

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        assert!(
            _snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );
        if _snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        // Trigger 1.1: space amplification ratio (every tier except the last, over the last).
        let last_size = _snapshot.levels.last().unwrap().1.len();
        let upper_size: usize = _snapshot.levels[.._snapshot.levels.len() - 1]
            .iter()
            .map(|(_, ids)| ids.len())
            .sum();
        if upper_size * 100 >= self.options.max_size_amplification_percent * last_size {
            return Some(TieredCompactionTask {
                tiers: _snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        // Trigger 1.2: size ratio. Cross-multiplied to avoid integer-division truncation:
        // `this_size / running_total > (100 + size_ratio) / 100`
        //   <=> `this_size * 100 > (100 + size_ratio) * running_total`.
        let mut running_total = _snapshot.levels[0].1.len();
        for i in 1.._snapshot.levels.len() {
            let this_size = _snapshot.levels[i].1.len();
            if this_size * 100 > (100 + self.options.size_ratio) * running_total
                && i >= self.options.min_merge_width
            {
                return Some(TieredCompactionTask {
                    tiers: _snapshot.levels[..i].to_vec(),
                    bottom_tier_included: false,
                });
            }
            running_total += this_size;
        }

        // Trigger 1.3: reduce sorted runs (fallback — always produces a task once the
        // precondition is met and neither earlier trigger fired).
        let take = self
            .options
            .max_merge_width
            .unwrap_or(_snapshot.levels.len())
            .min(_snapshot.levels.len());
        Some(TieredCompactionTask {
            tiers: _snapshot.levels[..take].to_vec(),
            bottom_tier_included: take == _snapshot.levels.len(),
        })
    }

    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &TieredCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = _snapshot.clone();
        let captured_tier_ids: HashSet<usize> = _task.tiers.iter().map(|(id, _)| *id).collect();

        let mut new_levels = Vec::with_capacity(snapshot.levels.len());
        let mut removed_ids = Vec::new();
        let mut inserted = false;
        for (tier_id, sst_ids) in snapshot.levels.iter() {
            if captured_tier_ids.contains(tier_id) {
                removed_ids.extend(sst_ids.iter().copied());
                // Insert the new merged tier exactly where the first captured tier
                // sat — preserving anything newer concurrently prepended ahead of it,
                // and anything older left untouched after it. No output at all (an
                // all-tombstone bottom compaction) means no replacement tier, not an
                // empty placeholder.
                if !inserted && !_output.is_empty() {
                    new_levels.push((_output[0], _output.to_vec()));
                    inserted = true;
                }
            } else {
                new_levels.push((*tier_id, sst_ids.clone()));
            }
        }
        snapshot.levels = new_levels;
        (snapshot, removed_ids)
    }
}
