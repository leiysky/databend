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

use std::time::Duration;

use common_exception::ErrorCode;
use common_exception::Result;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;

use crate::OperatorAttribute;
use crate::OperatorExecutionInfo;
use crate::OperatorProfile;
use crate::OperatorType;

/// [`QueryProfileEntry`] is a single entry of query profile log, which
/// can be constructed from [`OperatorProfile`].
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq)]
pub struct QueryProfileEntry {
    pub query_id: String,
    pub operator_id: u32,
    pub operator_type: String,
    pub children: Vec<u32>,
    pub execution_info: SerializableExecutionInfo,
    pub attribute: SerializableOperatorAttribute,
}

impl QueryProfileEntry {
    /// Construct a [`QueryProfileEntry`] from [`OperatorProfile`]
    pub fn from_operator_profile(
        query_id: String,
        operator_profile: &crate::OperatorProfile,
    ) -> Self {
        Self {
            query_id,
            operator_id: operator_profile.id,
            operator_type: operator_profile.operator_type.to_string(),
            children: operator_profile.children.clone(),
            execution_info: (&operator_profile.execution_info).into(),
            attribute: (&operator_profile.attribute).into(),
        }
    }

    /// Convert [`QueryProfileEntry`] to [`OperatorProfile`]
    pub fn into_operator_profile(self) -> Result<OperatorProfile> {
        let operator_type = OperatorType::from_str(&self.operator_type)?;
        let attribute = self.attribute.into_inner(&operator_type)?;
        Ok(OperatorProfile {
            id: self.operator_id,
            operator_type,
            children: self.children,
            execution_info: (&self.execution_info).into(),
            attribute,
        })
    }
}

/// Serializable representation of the [`OperatorExecutionInfo`]
/// Notice that the conversion may cause precision loss.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SerializableExecutionInfo {
    /// Process time in milliseconds
    pub process_time: f64,
    pub input_rows: usize,
    pub input_bytes: usize,
    pub output_rows: usize,
    pub output_bytes: usize,
}

impl From<&OperatorExecutionInfo> for SerializableExecutionInfo {
    fn from(value: &OperatorExecutionInfo) -> Self {
        Self {
            process_time: value.process_time.as_secs_f64() * 1000.0,
            input_rows: value.input_rows,
            input_bytes: value.input_bytes,
            output_rows: value.output_rows,
            output_bytes: value.output_bytes,
        }
    }
}

impl From<&SerializableExecutionInfo> for OperatorExecutionInfo {
    fn from(value: &SerializableExecutionInfo) -> Self {
        Self {
            process_time: Duration::from_secs_f64(value.process_time / 1000.0),
            input_rows: value.input_rows,
            input_bytes: value.input_bytes,
            output_rows: value.output_rows,
            output_bytes: value.output_bytes,
        }
    }
}

/// A serializable representation of [`OperatorAttribute`].
/// We can only convert it back to [`OperatorAttribute`] when we know the
/// operator type.
#[derive(Debug, Clone, PartialEq)]
pub struct SerializableOperatorAttribute(Value);

impl SerializableOperatorAttribute {
    /// Convert a [`SerializableOperatorAttribute`] to [`OperatorAttribute`] with
    /// given operator type.
    pub fn into_inner(self, op_type: &OperatorType) -> Result<OperatorAttribute> {
        match op_type {
            OperatorType::Join => {
                serde_json::from_value(self.0).map(|v| OperatorAttribute::Join(v))
            }
            OperatorType::Aggregate => {
                serde_json::from_value(self.0).map(|v| OperatorAttribute::Aggregate(v))
            }
            OperatorType::AggregateExpand => {
                serde_json::from_value(self.0).map(|v| OperatorAttribute::AggregateExpand(v))
            }
            OperatorType::Filter => {
                serde_json::from_value(self.0).map(|v| OperatorAttribute::Filter(v))
            }
            OperatorType::ProjectSet => {
                serde_json::from_value(self.0).map(|v| OperatorAttribute::ProjectSet(v))
            }
            OperatorType::EvalScalar => {
                serde_json::from_value(self.0).map(|v| OperatorAttribute::EvalScalar(v))
            }
            OperatorType::Limit => {
                serde_json::from_value(self.0).map(|v| OperatorAttribute::Limit(v))
            }
            OperatorType::TableScan => {
                serde_json::from_value(self.0).map(|v| OperatorAttribute::TableScan(v))
            }
            OperatorType::Sort => {
                serde_json::from_value(self.0).map(|v| OperatorAttribute::Sort(v))
            }
            OperatorType::Window => {
                serde_json::from_value(self.0).map(|v| OperatorAttribute::Window(v))
            }
            OperatorType::Exchange => {
                serde_json::from_value(self.0).map(|v| OperatorAttribute::Exchange(v))
            }
            OperatorType::Insert
            | OperatorType::RuntimeFilter
            | OperatorType::RowFetch
            | OperatorType::UnionAll
            | OperatorType::Project => Ok(OperatorAttribute::Empty),
        }
        .or_else(|e| {
            Err(ErrorCode::Internal(format!(
                "Failed to deserialize OperatorAttribute: {:?}",
                e
            )))
        })
    }
}

impl From<&OperatorAttribute> for SerializableOperatorAttribute {
    fn from(value: &OperatorAttribute) -> Self {
        Self(serde_json::value::to_value(value).unwrap())
    }
}

impl Serialize for SerializableOperatorAttribute {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SerializableOperatorAttribute {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        Ok(Self(value))
    }
}
