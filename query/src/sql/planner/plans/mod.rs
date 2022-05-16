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

mod aggregate;
mod filter;
mod hash_join;
mod limit;
mod logical_get;
mod logical_join;
mod pattern;
mod physical_scan;
mod project;
mod scalar;
mod sort;

use std::any::Any;

pub use aggregate::AggregateItem;
pub use aggregate::AggregatePlan;
pub use aggregate::GroupItem;
use enum_dispatch::enum_dispatch;
pub use filter::FilterPlan;
pub use hash_join::PhysicalHashJoin;
pub use limit::LimitPlan;
pub use logical_get::LogicalGet;
pub use logical_join::LogicalInnerJoin;
pub use pattern::PatternPlan;
pub use physical_scan::PhysicalScan;
pub use project::ProjectItem;
pub use project::ProjectPlan;
pub use scalar::*;
pub use sort::SortItem;
pub use sort::SortPlan;

use crate::sql::optimizer::PhysicalProperty;
use crate::sql::optimizer::RelationalProperty;
use crate::sql::optimizer::SExpr;

#[enum_dispatch]
pub trait BasePlan: Any {
    fn plan_type(&self) -> PlanType;

    fn is_physical(&self) -> bool;

    fn is_logical(&self) -> bool;

    fn is_pattern(&self) -> bool {
        false
    }

    fn as_physical(&self) -> Option<&dyn PhysicalPlan>;

    fn as_logical(&self) -> Option<&dyn LogicalPlan>;

    fn as_any(&self) -> &dyn Any;
}

pub trait LogicalPlan {
    fn compute_relational_prop(&self, expression: &SExpr) -> RelationalProperty;
}

pub trait PhysicalPlan {
    fn compute_physical_prop(&self, expression: &SExpr) -> PhysicalProperty;
}

/// Relational operator
#[derive(Clone, PartialEq, Debug)]
pub enum PlanType {
    // Logical operators
    LogicalGet,
    LogicalInnerJoin,

    // Physical operators
    PhysicalScan,
    PhysicalHashJoin,

    // Operators that are both logical and physical
    Project,
    Filter,
    Aggregate,
    Sort,
    Limit,

    // Pattern
    Pattern,
}

#[enum_dispatch(BasePlan)]
#[derive(Clone, Debug)]
pub enum BasePlanImpl {
    LogicalGet(LogicalGet),
    LogicalInnerJoin(LogicalInnerJoin),

    PhysicalScan(PhysicalScan),
    PhysicalHashJoin(PhysicalHashJoin),

    Project(ProjectPlan),
    Filter(FilterPlan),
    Aggregate(AggregatePlan),
    Sort(SortPlan),
    Limit(LimitPlan),

    Pattern(PatternPlan),
}
