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
use common_exception::Result;
use common_planners::Expression;

use super::transform::Transform;
use super::transform::Transformer;
use crate::common::ExpressionEvaluator;
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::sessions::QueryContext;

pub struct TransformFilterV2 {
    ctx: Arc<QueryContext>,
    pub predicates: Vec<Expression>,
}

impl TransformFilterV2 {
    pub fn new(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        ctx: Arc<QueryContext>,
        predicates: Vec<Expression>,
    ) -> ProcessorPtr {
        Transformer::<Self>::create(input_port, output_port, Self { ctx, predicates })
    }
}

impl Transform for TransformFilterV2 {
    const NAME: &'static str = "TransformFilter(v2)";

    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let func_ctx = self.ctx.try_get_function_context()?;

        let pred = self
            .predicates
            .iter()
            .cloned()
            .reduce(|prev, next| Expression::BinaryExpression {
                left: Box::new(prev),
                op: "and".to_string(),
                right: Box::new(next),
            })
            .unwrap_or_else(|| self.predicates[0].clone());

        let filter = ExpressionEvaluator::eval(func_ctx, &pred, &data)?;

        DataBlock::filter_block(&data, &filter)
    }
}
