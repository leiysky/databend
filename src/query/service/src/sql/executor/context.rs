// Copyright 2022 Datafuse Labs.
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

use common_planner::IndexType;

pub(super) type ChunkOffset = usize;

/// Context of compiling `RelOperator` into `PhysicalPlan`
#[derive(Clone, Debug, Default)]
pub(super) struct ChunkContext {
    /// Mapping from chunk offset to column index
    pub(crate) column_indices: Vec<IndexType>,
}

impl ChunkContext {
    pub fn new(column_indices: Vec<IndexType>) -> Self {
        Self { column_indices }
    }

    /// Get column index with chunk offset
    pub fn get_column_index(&self, chunk_offset: ChunkOffset) -> Option<IndexType> {
        self.column_indices.get(chunk_offset)
    }

    /// Get chunk offset with column index
    pub fn get_chunk_offset(&self, column_index: IndexType) -> Option<ChunkOffset> {
        self.column_indices
            .iter()
            .position(|index| *index == column_index)
    }

    pub fn append(&mut self, column_index: IndexType) {
        self.column_indices.push(column_index);
    }
}
