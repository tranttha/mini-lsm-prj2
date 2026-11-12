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

use std::collections::BTreeMap;

pub struct Watermark {
    readers: BTreeMap<u64, usize>,
}

impl Default for Watermark {
    fn default() -> Self {
        Self::new()
    }
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: BTreeMap::new(),
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        *self.readers.entry(ts).or_insert(0) += 1;
    }

    pub fn remove_reader(&mut self, ts: u64) {
        let count = self
            .readers
            .get_mut(&ts)
            .expect("removing a reader that was never added");
        *count -= 1;
        if *count == 0 {
            self.readers.remove(&ts);
        }
    }

    pub fn num_retained_snapshots(&self) -> usize {
        self.readers.len()
    }

    /// The oldest timestamp any active reader still needs, or `None` if there are no active
    /// readers at all. `BTreeMap` iterates keys in ascending order, so the first key is the
    /// minimum.
    pub fn watermark(&self) -> Option<u64> {
        self.readers.keys().next().copied()
    }
}
