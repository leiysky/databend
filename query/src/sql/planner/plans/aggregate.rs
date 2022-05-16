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

use std::any::Any;

use super::AggregateFunction;
use super::Scalar;
use crate::sql::IndexType;
use crate::sql::optimizer::PhysicalProperty;
use crate::sql::optimizer::RelationalProperty;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::BasePlan;
use crate::sql::plans::LogicalPlan;
use crate::sql::plans::PhysicalPlan;
use crate::sql::plans::PlanType;

#[derive(Clone, Debug)]
pub struct AggregatePlan {
    // group by scalar expressions, such as: group by a+1, a;
    pub group_items: Vec<GroupItem>,
    // aggregate scalar expressions, such as: sum(col1), count(*);
    pub aggregate_functions: Vec<AggregateItem>,
}

#[derive(Clone, Debug)]
pub struct GroupItem {
    pub index: IndexType,
    pub expr: Scalar,
}

#[derive(Clone, Debug)]
pub struct AggregateItem {
    pub index: IndexType,
    pub aggregate: AggregateFunction,
}

impl BasePlan for AggregatePlan {
    fn plan_type(&self) -> PlanType {
        PlanType::Aggregate
    }

    fn is_physical(&self) -> bool {
        true
    }

    fn is_logical(&self) -> bool {
        true
    }

    fn as_physical(&self) -> Option<&dyn PhysicalPlan> {
        todo!()
    }

    fn as_logical(&self) -> Option<&dyn LogicalPlan> {
        todo!()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl PhysicalPlan for AggregatePlan {
    fn compute_physical_prop(&self, _expression: &SExpr) -> PhysicalProperty {
        todo!()
    }
}

impl LogicalPlan for AggregatePlan {
    fn compute_relational_prop(&self, _expression: &SExpr) -> RelationalProperty {
        todo!()
    }
}
