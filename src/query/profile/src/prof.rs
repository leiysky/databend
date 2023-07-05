// Copyright 2021 Datafuse Labs
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

use std::fmt::Display;
use std::fmt::Formatter;
use std::time::Duration;

use common_exception::ErrorCode;
use common_exception::Result;

use crate::ProcessorProfile;

#[derive(Debug, Clone)]
pub struct QueryProfile {
    /// Query ID of the query profile
    pub query_id: String,

    /// Flattened plan node profiles
    pub operator_profiles: Vec<OperatorProfile>,
}

impl QueryProfile {
    pub fn new(query_id: String, operator_profiles: Vec<OperatorProfile>) -> Self {
        QueryProfile {
            query_id,
            operator_profiles,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OperatorProfile {
    /// ID of the plan node
    pub id: u32,

    /// Type of the plan operator, e.g. `HashJoin`
    pub operator_type: OperatorType,

    /// IDs of the children plan nodes
    pub children: Vec<u32>,

    /// The execution information of the plan operator
    pub execution_info: OperatorExecutionInfo,

    /// Attribute of the plan operator
    pub attribute: OperatorAttribute,
}

#[derive(Debug, Clone)]
pub enum OperatorType {
    Join,
    Aggregate,
    AggregateExpand,
    Filter,
    ProjectSet,
    EvalScalar,
    Limit,
    TableScan,
    Sort,
    UnionAll,
    Project,
    Window,
    RowFetch,
    Exchange,
    RuntimeFilter,
    Insert,
}

impl OperatorType {
    pub fn from_str(s: &str) -> Result<Self> {
        match s {
            "Join" => Ok(OperatorType::Join),
            "Aggregate" => Ok(OperatorType::Aggregate),
            "AggregateExpand" => Ok(OperatorType::AggregateExpand),
            "Filter" => Ok(OperatorType::Filter),
            "ProjectSet" => Ok(OperatorType::ProjectSet),
            "EvalScalar" => Ok(OperatorType::EvalScalar),
            "Limit" => Ok(OperatorType::Limit),
            "TableScan" => Ok(OperatorType::TableScan),
            "Sort" => Ok(OperatorType::Sort),
            "UnionAll" => Ok(OperatorType::UnionAll),
            "Project" => Ok(OperatorType::Project),
            "Window" => Ok(OperatorType::Window),
            "RowFetch" => Ok(OperatorType::RowFetch),
            "Exchange" => Ok(OperatorType::Exchange),
            "RuntimeFilter" => Ok(OperatorType::RuntimeFilter),
            "Insert" => Ok(OperatorType::Insert),
            _ => Err(ErrorCode::Internal(format!("Unknown operator type: {}", s))),
        }
    }
}

impl Display for OperatorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OperatorType::Join => write!(f, "Join"),
            OperatorType::Aggregate => write!(f, "Aggregate"),
            OperatorType::AggregateExpand => write!(f, "AggregateExpand"),
            OperatorType::Filter => write!(f, "Filter"),
            OperatorType::ProjectSet => write!(f, "ProjectSet"),
            OperatorType::EvalScalar => write!(f, "EvalScalar"),
            OperatorType::Limit => write!(f, "Limit"),
            OperatorType::TableScan => write!(f, "TableScan"),
            OperatorType::Sort => write!(f, "Sort"),
            OperatorType::UnionAll => write!(f, "UnionAll"),
            OperatorType::Project => write!(f, "Project"),
            OperatorType::Window => write!(f, "Window"),
            OperatorType::RowFetch => write!(f, "RowFetch"),
            OperatorType::Exchange => write!(f, "Exchange"),
            OperatorType::RuntimeFilter => write!(f, "RuntimeFilter"),
            OperatorType::Insert => write!(f, "Insert"),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct OperatorExecutionInfo {
    pub process_time: Duration,
    pub input_rows: usize,
    pub input_bytes: usize,
    pub output_rows: usize,
    pub output_bytes: usize,
}

impl From<ProcessorProfile> for OperatorExecutionInfo {
    fn from(value: ProcessorProfile) -> Self {
        (&value).into()
    }
}

impl From<&ProcessorProfile> for OperatorExecutionInfo {
    fn from(value: &ProcessorProfile) -> Self {
        OperatorExecutionInfo {
            process_time: value.cpu_time,
            input_rows: value.input_rows,
            input_bytes: value.input_bytes,
            output_rows: value.output_rows,
            output_bytes: value.output_bytes,
        }
    }
}

/// Attribute of the plan operator.
///
/// Each variant of [`OperatorType`] has a corresponding variant in this enum.
///
/// We can not derive `serde::Deserialize` for this enum because
/// the tag is outside of the JSON object, which is actually held
/// by the [`OperatorProfile`] struct.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(untagged)]
pub enum OperatorAttribute {
    Join(JoinAttribute),
    Aggregate(AggregateAttribute),
    AggregateExpand(AggregateExpandAttribute),
    Filter(FilterAttribute),
    EvalScalar(EvalScalarAttribute),
    ProjectSet(ProjectSetAttribute),
    Limit(LimitAttribute),
    TableScan(TableScanAttribute),
    Sort(SortAttribute),
    Window(WindowAttribute),
    Exchange(ExchangeAttribute),
    Empty,
}

impl Default for OperatorAttribute {
    fn default() -> Self {
        OperatorAttribute::Empty
    }
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct JoinAttribute {
    pub join_type: String,
    pub equi_conditions: String,
    pub non_equi_conditions: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AggregateAttribute {
    pub group_keys: Vec<String>,
    pub functions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AggregateExpandAttribute {
    pub group_keys_set: Vec<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct EvalScalarAttribute {
    pub scalars: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ProjectSetAttribute {
    pub functions: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FilterAttribute {
    pub predicate: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LimitAttribute {
    pub limit: usize,
    pub offset: usize,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SortAttribute {
    pub sort_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TableScanAttribute {
    pub qualified_name: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WindowAttribute {
    pub function: String,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ExchangeAttribute {
    pub exchange_mode: String,
}
