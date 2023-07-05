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

use std::sync::Arc;

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_expression::types::ArgType;
use common_expression::types::ArrayType;
use common_expression::types::NumberDataType;
use common_expression::types::StringType;
use common_expression::types::UInt32Type;
use common_expression::types::ValueType;
use common_expression::types::VariantType;
use common_expression::DataBlock;
use common_expression::FromData;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRefExt;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_profile::OperatorAttribute;
use common_profile::OperatorExecutionInfo;
use common_profile::QueryProfileManager;
use common_profile::SerializableExecutionInfo;
use common_profile::SerializableOperatorAttribute;

use crate::SyncOneBlockSystemTable;
use crate::SyncSystemTable;

pub struct QueryProfileTable {
    table_info: TableInfo,
}

impl QueryProfileTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("query_id", TableDataType::String),
            TableField::new("operator_id", TableDataType::Number(NumberDataType::UInt32)),
            TableField::new("operator_type", TableDataType::String),
            TableField::new(
                "operator_children",
                TableDataType::Array(Box::new(TableDataType::Number(NumberDataType::UInt32))),
            ),
            TableField::new("execution_info", TableDataType::Variant),
            TableField::new("operator_attribute", TableDataType::Variant),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'query_profile'".to_string(),
            ident: TableIdent::new(table_id, 0),
            name: "query_profile".to_string(),
            meta: TableMeta {
                schema,
                engine: "QueryProfile".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        SyncOneBlockSystemTable::create(Self { table_info })
    }
}

impl SyncSystemTable for QueryProfileTable {
    const NAME: &'static str = "system.query_profile";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, _ctx: Arc<dyn TableContext>) -> common_exception::Result<DataBlock> {
        let profile_mgr = QueryProfileManager::instance();
        let query_profs = profile_mgr.list_all();

        let mut query_ids: Vec<Vec<u8>> = Vec::with_capacity(query_profs.len());
        let mut operator_ids: Vec<u32> = Vec::with_capacity(query_profs.len());
        let mut operator_types: Vec<Vec<u8>> = Vec::with_capacity(query_profs.len());
        let mut operator_childrens: Vec<Vec<u32>> = Vec::with_capacity(query_profs.len());
        let mut execution_infos: Vec<Vec<u8>> = Vec::with_capacity(query_profs.len());
        let mut operator_attributes: Vec<Vec<u8>> = Vec::with_capacity(query_profs.len());

        for prof in query_profs.iter() {
            for operator_prof in prof.operator_profiles.iter() {
                query_ids.push(prof.query_id.clone().into_bytes());
                operator_ids.push(operator_prof.id);
                operator_types.push(operator_prof.operator_type.to_string().into_bytes());
                operator_childrens.push(operator_prof.children.clone());

                let execution_info = SerializableExecutionInfo::from(&operator_prof.execution_info);
                let jsonb_value: jsonb::Value = (&serde_json::to_value(execution_info)?).into();
                execution_infos.push(jsonb_value.to_vec());

                let attribute_value = SerializableOperatorAttribute::from(&operator_prof.attribute);
                let jsonb_value: jsonb::Value = (&serde_json::to_value(attribute_value)?).into();
                operator_attributes.push(jsonb_value.to_vec());
            }
        }

        let block = DataBlock::new_from_columns(vec![
            // query_id
            StringType::from_data(query_ids),
            // operator_id
            UInt32Type::from_data(operator_ids),
            // operator_type
            StringType::from_data(operator_types),
            // operator_children
            ArrayType::upcast_column(ArrayType::<UInt32Type>::column_from_iter(
                operator_childrens
                    .into_iter()
                    .map(|children| UInt32Type::column_from_iter(children.into_iter(), &[])),
                &[],
            )),
            // execution_info
            VariantType::from_data(execution_infos),
            // operator_attribute
            VariantType::from_data(operator_attributes),
        ]);

        Ok(block)
    }
}
