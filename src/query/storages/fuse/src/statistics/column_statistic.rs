//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashMap;

use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberType;
use common_expression::types::ValueType;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::Scalar;
use common_functions::aggregates::eval_aggr;
use storages_common_index::MinMaxIndex;
use storages_common_index::SupportedType;
use storages_common_table_meta::meta::ColumnStatistics;
use storages_common_table_meta::meta::StatisticsOfColumns;

pub fn calc_column_distinct_of_values(
    column: &Column,
    data_type: &DataType,
    rows: usize,
) -> Result<u64> {
    let distinct_values = eval_aggr(
        "approx_count_distinct",
        vec![],
        &[column.clone()],
        &[data_type.clone()],
        rows,
    )?;
    let col = NumberType::<u64>::try_downcast_column(&distinct_values.0).unwrap();
    Ok(col[0])
}

pub fn get_traverse_columns_dfs(data_block: &DataBlock) -> traverse::TraverseResult {
    traverse::traverse_columns_dfs(data_block.columns())
}

pub fn gen_columns_statistics(
    data_block: &DataBlock,
    column_distinct_count: Option<HashMap<usize, usize>>,
) -> Result<StatisticsOfColumns> {
    let mut statistics = StatisticsOfColumns::new();
    let data_block = data_block.convert_to_full();
    let rows = data_block.num_rows();

    let leaves = get_traverse_columns_dfs(&data_block)?;
    for (idx, (col_idx, col, data_type)) in leaves.iter().enumerate() {
        if !MinMaxIndex::is_supported_type(data_type) {
            continue;
        }

        // later, during the evaluation of expressions, name of field does not matter
        let mut min = Scalar::Null;
        let mut max = Scalar::Null;
        let col = col.as_column().unwrap();

        let (mins, _) = eval_aggr("min", vec![], &[col.clone()], &[data_type.clone()], rows)?;
        let (maxs, _) = eval_aggr("max", vec![], &[col.clone()], &[data_type.clone()], rows)?;

        if mins.len() > 0 {
            min = if let Some(v) = mins.index(0) {
                if let Some(v) = v.to_owned().trim_min() {
                    v
                } else {
                    continue;
                }
            } else {
                continue;
            }
        }

        if maxs.len() > 0 {
            max = if let Some(v) = maxs.index(0) {
                if let Some(v) = v.to_owned().trim_max() {
                    v
                } else {
                    continue;
                }
            } else {
                continue;
            }
        }

        let (is_all_null, bitmap) = col.validity();
        let unset_bits = match (is_all_null, bitmap) {
            (true, _) => rows,
            (false, Some(bitmap)) => bitmap.unset_bits(),
            (false, None) => 0,
        };

        // use distinct count calculated by the xor hash function to avoid repetitive operation.
        let distinct_of_values = match (col_idx, &column_distinct_count) {
            (Some(col_idx), Some(ref column_distinct_count)) => {
                if let Some(value) = column_distinct_count.get(col_idx) {
                    // value calculated by xor hash function include NULL, need to subtract one.
                    if unset_bits > 0 {
                        *value as u64 - 1
                    } else {
                        *value as u64
                    }
                } else {
                    calc_column_distinct_of_values(col, data_type, rows)?
                }
            }
            (_, _) => calc_column_distinct_of_values(col, data_type, rows)?,
        };

        let in_memory_size = col.memory_size() as u64;
        let col_stats = ColumnStatistics {
            min,
            max,
            null_count: unset_bits as u64,
            in_memory_size,
            distinct_of_values: Some(distinct_of_values),
        };

        statistics.insert(idx as u32, col_stats);
    }
    Ok(statistics)
}

pub mod traverse {
    use common_expression::types::AnyType;
    use common_expression::types::DataType;
    use common_expression::BlockEntry;
    use common_expression::Column;
    use common_expression::Value;

    use super::*;

    pub type TraverseResult = Result<Vec<(Option<usize>, Value<AnyType>, DataType)>>;

    // traverses columns and collects the leaves in depth first manner
    pub fn traverse_columns_dfs(columns: &[BlockEntry]) -> TraverseResult {
        let mut leaves = vec![];
        for (idx, entry) in columns.iter().enumerate() {
            let data_type = &entry.data_type;
            let column = entry.value.as_column().unwrap();
            traverse_recursive(Some(idx), column, data_type, &mut leaves)?;
        }
        Ok(leaves)
    }

    fn traverse_recursive(
        idx: Option<usize>,
        column: &Column,
        data_type: &DataType,
        leaves: &mut Vec<(Option<usize>, Value<AnyType>, DataType)>,
    ) -> Result<()> {
        match data_type {
            DataType::Tuple(inner_data_types) => {
                let (inner_columns, _) = column.as_tuple().unwrap();
                for (inner_column, inner_data_type) in
                    inner_columns.iter().zip(inner_data_types.iter())
                {
                    traverse_recursive(None, inner_column, inner_data_type, leaves)?;
                }
            }
            _ => {
                leaves.push((idx, Value::Column(column.clone()), data_type.clone()));
            }
        }
        Ok(())
    }
}

// Impls of this trait should preserves the property of min/max statistics:
//
// the trimmed max should be larger than the non-trimmed one (if possible).
// and the trimmed min should be lesser than the non-trimmed one (if possible).
pub trait Trim: Sized {
    fn trim_min(self) -> Option<Self>;
    fn trim_max(self) -> Option<Self>;
}

pub const STATS_REPLACEMENT_CHAR: char = '\u{FFFD}';
pub const STATS_STRING_PREFIX_LEN: usize = 16;

impl Trim for Scalar {
    fn trim_min(self) -> Option<Self> {
        match self {
            Scalar::String(bytes) => match String::from_utf8(bytes) {
                Ok(mut v) => {
                    if v.len() <= STATS_STRING_PREFIX_LEN {
                        Some(Scalar::String(v.into_bytes()))
                    } else {
                        // find the character boundary to prevent String::truncate from panic
                        let vs = v.as_str();
                        let slice = match vs.char_indices().nth(STATS_STRING_PREFIX_LEN) {
                            None => vs,
                            Some((idx, _)) => &vs[..idx],
                        };

                        // do truncate
                        Some(Scalar::String({
                            v.truncate(slice.len());
                            v.into_bytes()
                        }))
                    }
                }
                Err(_) => {
                    // if failed to convert the bytes into (utf-8)string, just ignore it.
                    None
                }
            },
            v => Some(v),
        }
    }

    fn trim_max(self) -> Option<Self> {
        match self {
            Scalar::String(bytes) => match String::from_utf8(bytes) {
                Ok(v) => {
                    if v.len() <= STATS_STRING_PREFIX_LEN {
                        // if number of bytes is lesser, just return
                        Some(Scalar::String(v.into_bytes()))
                    } else {
                        // no need to trim, less than STRING_RREFIX_LEN chars
                        let number_of_chars = v.as_str().chars().count();
                        if number_of_chars <= STATS_STRING_PREFIX_LEN {
                            return Some(Scalar::String(v.into_bytes()));
                        }

                        // slice the input (at the boundary of chars), takes at most STRING_PREFIX_LEN chars
                        let vs = v.as_str();
                        let sliced = match vs.char_indices().nth(STATS_STRING_PREFIX_LEN) {
                            None => vs,
                            Some((idx, _)) => &vs[..idx],
                        };

                        // find the position to replace the char with REPLACEMENT_CHAR
                        // in reversed order, break at the first one we met
                        let mut idx = None;
                        for (i, c) in sliced.char_indices().rev() {
                            if c < STATS_REPLACEMENT_CHAR {
                                idx = Some(i);
                                break;
                            }
                        }

                        // grab the replacement_point
                        let replacement_point = idx?;

                        // rebuild the string (since the len of result string is rather small)
                        let mut r = String::with_capacity(STATS_STRING_PREFIX_LEN);
                        for (i, c) in sliced.char_indices() {
                            if i < replacement_point {
                                r.push(c)
                            } else {
                                r.push(STATS_REPLACEMENT_CHAR);
                            }
                        }

                        Some(Scalar::String(r.into_bytes()))
                    }
                }
                Err(_) => {
                    // if failed to convert the bytes into (utf-8)string, just ignore it.
                    None
                }
            },
            v => Some(v),
        }
    }
}