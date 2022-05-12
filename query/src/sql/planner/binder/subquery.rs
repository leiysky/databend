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

use crate::sql::optimizer::SExpr;
use common_exception::{ErrorCode, Result};
use crate::sql::BindContext;
use crate::sql::plans::FilterPlan;

pub struct SubqueryDecorrelator {}

impl SubqueryDecorrelator {
    pub fn decorrelate_where(&self, mut bind_context: BindContext) -> Result<BindContext> {
        let s_expr = bind_context.expression.ok_or(
            ErrorCode::LogicalError("Invalid BindContext")
        )?;
        let filter: FilterPlan = s_expr.plan().try_into()?;
        for predicate in filter.predicates.iter() {}

        todo!()
    }
}