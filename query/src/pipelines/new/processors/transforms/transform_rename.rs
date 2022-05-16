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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::DataField;
use common_exception::Result;

use super::transform::Transform;
use super::transform::Transformer;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;

/// Reorganize DataBlock with given permutation, may potentially remove columns
pub struct TransformRename {
    // (Original name, Output name)
    pub permutation: Vec<(String, String)>,
}

impl TransformRename {
    pub fn new(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        permutation: Vec<(String, String)>,
    ) -> ProcessorPtr {
        Transformer::<Self>::create(input_port, output_port, Self { permutation })
    }
}

impl Transform for TransformRename {
    const NAME: &'static str = "Output";

    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let mut output_block = DataBlock::empty();
        let input_schema = data.schema();
        for (original, output) in self.permutation.iter() {
            let index = input_schema.index_of(original.as_str())?;
            let original_field = input_schema.field(index);
            let column = data.column(index);
            let output_field = DataField::new(output.as_str(), original_field.data_type().clone());

            output_block = output_block.add_column(column.clone(), output_field)?;
        }

        Ok(output_block)
    }
}
