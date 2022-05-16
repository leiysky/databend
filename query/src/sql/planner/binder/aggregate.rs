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

use std::collections::HashMap;

use common_ast::ast::Expr;
use common_ast::parser::error::DisplayError;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::sql::binder::scalar::ScalarBinder;
use crate::sql::binder::scalar_common::find_aggregate_scalars;
use crate::sql::binder::scalar_common::find_aggregate_scalars_from_bind_context;
use crate::sql::binder::Binder;
use crate::sql::binder::ColumnBinding;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::AggregateFunction;
use crate::sql::plans::AggregateItem;
use crate::sql::plans::AggregatePlan;
use crate::sql::plans::AndExpr;
use crate::sql::plans::BoundColumnRef;
use crate::sql::plans::CastExpr;
use crate::sql::plans::ComparisonExpr;
use crate::sql::plans::FunctionCall;
use crate::sql::plans::GroupItem;
use crate::sql::plans::OrExpr;
use crate::sql::plans::Scalar;
use crate::sql::plans::ScalarExpr;
use crate::sql::BindContext;
use crate::sql::Metadata;

#[derive(Clone, PartialEq, Debug, Default)]
pub struct AggregateInfo {
    /// GROUP BY items, (display_name, scalar)
    pub group_items: Vec<(String, Scalar)>,

    /// Aggregation functions
    pub aggregate_functions: Vec<AggregateFunction>,

    /// Since scalar group by items is able to be referenced, we have to
    /// check if a scalar expression can be derived from a group by item.
    /// For example: `SELECT count(*) FROM t GROUP BY a+1 HAVING a+1`,
    /// the `a+1` in `HAVING` clause is valid. We will use display name to
    /// identify them.
    ///
    /// Mapping: group_item_display_name -> group_item
    /// Will be built in `bind_aggregate`.
    pub group_items_map: HashMap<String, ColumnBinding>,
}

/// Replace `Scalar::AggregateFunction` with `Scalar::BoundColumnRef(aggfunc.to_string())`
struct AggregateRewriter {
    aggregate_functions: HashMap<String, ColumnBinding>,
}

impl AggregateRewriter {
    pub fn replace(&self, scalar: &Scalar) -> Result<Scalar> {
        Ok(match scalar {
            v @ Scalar::BoundColumnRef(_) => v.clone(),
            v @ Scalar::ConstantExpr(_) => v.clone(),

            Scalar::AndExpr(and) => Scalar::AndExpr(AndExpr {
                left: Box::new(self.replace(&and.left)?),
                right: Box::new(self.replace(&and.right)?),
            }),

            Scalar::OrExpr(or) => Scalar::OrExpr(OrExpr {
                left: Box::new(self.replace(&or.left)?),
                right: Box::new(self.replace(&or.right)?),
            }),

            Scalar::ComparisonExpr(comp) => Scalar::ComparisonExpr(ComparisonExpr {
                op: comp.op.clone(),
                left: Box::new(self.replace(&comp.left)?),
                right: Box::new(self.replace(&comp.right)?),
            }),

            Scalar::FunctionCall(func_call) => {
                let arguments = func_call
                    .arguments
                    .iter()
                    .map(|arg| self.replace(scalar))
                    .collect::<Result<Vec<Scalar>>>()?;
                Scalar::FunctionCall(FunctionCall {
                    arguments,
                    func_name: func_call.func_name.clone(),
                    arg_types: func_call.arg_types.clone(),
                    return_type: func_call.return_type.clone(),
                })
            }

            Scalar::Cast(cast) => Scalar::Cast(CastExpr {
                argument: Box::new(self.replace(&cast.argument)?),
                from_type: cast.from_type.clone(),
                target_type: cast.target_type.clone(),
            }),

            // Notice that, we won't process subquery here
            v @ Scalar::SubqueryExpr(_) => v.clone(),

            // Replace with BoundColumnRef, add the aggregate function to Metadata
            Scalar::AggregateFunction(agg) => Scalar::BoundColumnRef(BoundColumnRef {
                column: self
                    .aggregate_functions
                    .get(&agg.display_name)
                    .cloned()
                    .ok_or(ErrorCode::LogicalError(format!(
                        "Invalid unbound aggregate function: {}",
                        &agg.display_name
                    )))?,
            }),
        })
    }
}

impl<'a> Binder {
    /// We have supported two kinds of `group by` items:
    ///
    ///   - Alias, the aliased expressions specified in `SELECT` clause, e.g. column `b` in
    ///     `SELECT a as b, COUNT(a) FROM t GROUP BY b`.
    ///   - Scalar expressions that can be evaluated in current scope(doesn't contain aliases), e.g.
    ///     column `a` and expression `a+1` in `SELECT a as b, COUNT(a) FROM t GROUP BY a, a+1`.
    pub(crate) async fn analyze_aggregate(
        &self,
        input_context: &BindContext,
        group_by: &[Expr<'a>],
    ) -> Result<AggregateInfo> {
        let mut available_aliases: Vec<ColumnBinding> = vec![];
        for column_binding in input_context.all_column_bindings() {
            if let Some(scalar) = &column_binding.scalar {
                // If a column is `BoundColumnRef`, we will treat it as an available alias anyway.
                // Otherwise we will check visibility and if the scalar contains aggregate functions.
                if matches!(scalar.as_ref(), Scalar::BoundColumnRef(_))
                    || (column_binding.visible
                        && find_aggregate_scalars(&[*scalar.clone()]).is_empty())
                {
                    available_aliases.push(column_binding.clone());
                }
            }
        }

        let scalar_binder = ScalarBinder::new(input_context, self.ctx.clone());
        let mut group_expr = Vec::with_capacity(group_by.len());
        for expr in group_by.iter() {
            let (scalar_expr, _) = scalar_binder.bind_expr(expr).await.or_else(|e| {
                let mut result = vec![];
                // If cannot resolve group item, then try to find an available alias
                for column_binding in available_aliases.iter() {
                    let col_name = column_binding.column_name.as_str();
                    if let Some(scalar) = &column_binding.scalar {
                        // TODO(leiysky): check if expr is a qualified name
                        if col_name == expr.to_string().to_lowercase().as_str() {
                            result.push(scalar);
                        }
                    }
                }

                if result.is_empty() {
                    Err(e)
                } else if result.len() > 1 {
                    Err(ErrorCode::SemanticError(expr.span().display_error(
                        format!("GROUP BY \"{}\" is ambiguous", expr),
                    )))
                } else {
                    Ok((*result[0].clone(), result[0].data_type()))
                }
            })?;

            group_expr.push((format!("{:#}", expr), scalar_expr));
        }

        // TODO(leiysky): deduplicate aggregate functions
        let mut aggregate_functions: Vec<AggregateFunction> = Vec::new();
        for agg_scalar in find_aggregate_scalars_from_bind_context(input_context)? {
            match agg_scalar {
                Scalar::AggregateFunction(agg) => aggregate_functions.push(agg),
                _ => {
                    return Err(ErrorCode::LogicalError(
                        "scalar expr must be Aggregation scalar expr",
                    ));
                }
            }
        }

        Ok(AggregateInfo {
            group_items: group_expr,
            aggregate_functions,
            ..Default::default()
        })
    }

    pub(super) async fn bind_aggregate(
        &mut self,
        input_context: &BindContext,
        mut output_context: BindContext,
        child: SExpr,
        mut agg_info: AggregateInfo,
    ) -> Result<(SExpr, BindContext)> {
        let mut group_items = Vec::with_capacity(agg_info.group_items.len());
        let mut group_items_map: HashMap<String, ColumnBinding> = HashMap::new();
        let mut agg_funcs = Vec::with_capacity(agg_info.aggregate_functions.len());
        let mut agg_output_columns =
            Vec::with_capacity(agg_info.aggregate_functions.len() + agg_info.group_items.len());

        // Add group items to context
        for (name, group_item) in agg_info.group_items.iter() {
            // TODO(leiysky): don't need to generate index for passthrough column, e.g. GROUP BY a
            let index = self
                .metadata
                .add_column(name.clone(), group_item.data_type(), None);
            let column_binding = ColumnBinding {
                index,
                table_name: None,
                column_name: name.clone(),
                data_type: group_item.data_type(),
                visible: true,
                scalar: Some(Box::new(group_item.clone())),
            };

            agg_output_columns.push(column_binding.clone());
            group_items_map.insert(name.clone(), column_binding);
            group_items.push(GroupItem {
                index,
                expr: group_item.clone(),
            });
        }

        // Fill the group_items_map
        agg_info.group_items_map = group_items_map;

        // Add aggregate functions to context
        for agg_func in agg_info.aggregate_functions.iter() {
            let index = self.metadata.add_column(
                agg_func.display_name.clone(),
                agg_func.return_type.clone(),
                None,
            );
            let column_binding = ColumnBinding {
                index,
                table_name: None,
                column_name: agg_func.display_name.clone(),
                data_type: agg_func.return_type.clone(),
                visible: true,
                scalar: Some(Box::new(Scalar::AggregateFunction(agg_func.clone()))),
            };

            agg_output_columns.push(column_binding);
            agg_funcs.push(AggregateItem {
                index,
                aggregate: agg_func.clone(),
            });
        }

        let agg_plan = AggregatePlan {
            group_items,
            aggregate_functions: agg_funcs,
        };
        let new_expr = SExpr::create_unary(agg_plan.into(), child);

        // Replace output context with aggregate output
        output_context.columns = agg_output_columns;
        output_context.agg_info = Some(agg_info);

        Ok((new_expr, output_context))
    }
}
