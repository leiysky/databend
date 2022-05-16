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
use common_planners::Expression;

use super::transform::Transform;
use super::transform::Transformer;
use crate::common::ExpressionEvaluator;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::sessions::QueryContext;

/// Evaluate scalar expressions and add the results into result set with given alias.
pub struct TransformScalar {
    ctx: Arc<QueryContext>,
    /// (Expression, Alias)
    pub expressions: Vec<(Expression, DataField)>,
}

impl TransformScalar {
    pub fn new(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        ctx: Arc<QueryContext>,
        expressions: Vec<(Expression, DataField)>,
    ) -> ProcessorPtr {
        Transformer::<Self>::create(input_port, output_port, Self { ctx, expressions })
    }
}

impl Transform for TransformScalar {
    const NAME: &'static str = "ScalarTransform";

    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let func_ctx = self.ctx.try_get_function_context()?;
        let mut result_block = data;
        for (expr, alias) in self.expressions.iter() {
            let result = ExpressionEvaluator::eval(func_ctx.clone(), expr, &result_block)?;
            result_block = result_block.add_column(result, alias.clone())?;
        }

        Ok(result_block)
    }
}
