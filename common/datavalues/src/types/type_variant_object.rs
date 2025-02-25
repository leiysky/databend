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

use std::collections::BTreeMap;
use std::sync::Arc;

use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_exception::Result;

use super::data_type::DataType;
use super::data_type::ARROW_EXTENSION_NAME;
use super::type_id::TypeID;
use crate::prelude::*;

#[derive(Default, Clone, serde::Deserialize, serde::Serialize)]
pub struct VariantObjectType {}

impl VariantObjectType {
    pub fn new_impl() -> DataTypeImpl {
        DataTypeImpl::VariantObject(Self {})
    }
}

impl DataType for VariantObjectType {
    fn data_type_id(&self) -> TypeID {
        TypeID::VariantObject
    }

    #[inline]
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        "Object".to_string()
    }

    fn aliases(&self) -> &[&str] {
        &["Map"]
    }

    fn default_value(&self) -> DataValue {
        DataValue::Variant(VariantValue::from(serde_json::Value::Object(
            serde_json::Map::new(),
        )))
    }

    fn create_constant_column(&self, data: &DataValue, size: usize) -> Result<ColumnRef> {
        let value: VariantValue = DFTryFrom::try_from(data)?;
        let column = Series::from_data(vec![value]);
        Ok(Arc::new(ConstColumn::new(column, size)))
    }

    fn create_column(&self, data: &[DataValue]) -> Result<ColumnRef> {
        let values: Vec<VariantValue> = data
            .iter()
            .map(DFTryFrom::try_from)
            .collect::<Result<Vec<_>>>()?;

        Ok(Series::from_data(values))
    }

    fn arrow_type(&self) -> ArrowType {
        ArrowType::Extension(
            "VariantObject".to_owned(),
            Box::new(ArrowType::LargeBinary),
            None,
        )
    }

    fn custom_arrow_meta(&self) -> Option<BTreeMap<String, String>> {
        let mut mp = BTreeMap::new();
        mp.insert(
            ARROW_EXTENSION_NAME.to_string(),
            "VariantObject".to_string(),
        );
        Some(mp)
    }

    fn create_serializer(&self) -> TypeSerializerImpl {
        VariantSerializer {}.into()
    }

    fn create_deserializer(&self, capacity: usize) -> TypeDeserializerImpl {
        VariantDeserializer::with_capacity(capacity).into()
    }

    fn create_mutable(&self, capacity: usize) -> Box<dyn MutableColumn> {
        Box::new(MutableObjectColumn::<VariantValue>::with_capacity(capacity))
    }
}

impl std::fmt::Debug for VariantObjectType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
