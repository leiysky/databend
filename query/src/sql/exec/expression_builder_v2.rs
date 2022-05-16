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

use common_datavalues::DataTypeImpl;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Expression;

use super::pipeline_builder::ColumnOrdinal;
use crate::sql::plans::AggregateFunction;
use crate::sql::plans::AndExpr;
use crate::sql::plans::BoundColumnRef;
use crate::sql::plans::CastExpr;
use crate::sql::plans::ComparisonExpr;
use crate::sql::plans::ConstantExpr;
use crate::sql::plans::FunctionCall;
use crate::sql::plans::OrExpr;
use crate::sql::plans::Scalar;
use crate::sql::IndexType;
use crate::sql::Metadata;

pub struct ExpressionBuilderV2<'a> {
    metadata: &'a Metadata,
}

impl<'a> ExpressionBuilderV2<'a> {
    pub fn create(metadata: &'a Metadata) -> Self {
        ExpressionBuilderV2 { metadata }
    }

    pub fn build(&self, scalar: &Scalar) -> Result<Expression> {
        match scalar {
            Scalar::BoundColumnRef(BoundColumnRef { column }) => {
                self.build_column_ref(column.index)
            }
            Scalar::ConstantExpr(ConstantExpr { value, data_type }) => {
                self.build_literal(value, data_type)
            }
            Scalar::ComparisonExpr(ComparisonExpr { op, left, right }) => {
                self.build_binary_operator(left, right, op.to_func_name())
            }
            Scalar::AggregateFunction(AggregateFunction {
                func_name,
                distinct,
                params,
                args,
                ..
            }) => self.build_aggr_function(func_name.clone(), *distinct, params.clone(), args),
            Scalar::AndExpr(AndExpr { left, right }) => {
                let left = self.build(&**left)?;
                let right = self.build(&**right)?;
                Ok(Expression::BinaryExpression {
                    left: Box::new(left),
                    op: "and".to_string(),
                    right: Box::new(right),
                })
            }
            Scalar::OrExpr(OrExpr { left, right }) => {
                let left = self.build(&**left)?;
                let right = self.build(&**right)?;
                Ok(Expression::BinaryExpression {
                    left: Box::new(left),
                    op: "or".to_string(),
                    right: Box::new(right),
                })
            }
            Scalar::FunctionCall(FunctionCall {
                arguments,
                func_name,
                ..
            }) => {
                let args = arguments
                    .iter()
                    .map(|arg| self.build(arg))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expression::ScalarFunction {
                    op: func_name.clone(),
                    args,
                })
            }
            Scalar::Cast(CastExpr {
                argument,
                target_type,
                ..
            }) => {
                let arg = self.build(argument)?;
                Ok(Expression::Cast {
                    expr: Box::new(arg),
                    data_type: target_type.clone(),
                    pg_style: false,
                })
            }
            Scalar::SubqueryExpr(_) => Err(ErrorCode::UnImplement("Unsupported subquery expr")),
        }
    }

    pub fn build_column_ref(&self, index: IndexType) -> Result<Expression> {
        let column = self.metadata.column(index);
        Ok(Expression::Column(ColumnOrdinal(index).to_string()))
    }

    pub fn build_literal(
        &self,
        data_value: &DataValue,
        data_type: &DataTypeImpl,
    ) -> Result<Expression> {
        Ok(Expression::Literal {
            value: data_value.clone(),
            column_name: None,
            data_type: data_type.clone(),
        })
    }

    pub fn build_binary_operator(
        &self,
        left: &Scalar,
        right: &Scalar,
        op: String,
    ) -> Result<Expression> {
        let left_child = self.build(left)?;
        let right_child = self.build(right)?;
        Ok(Expression::BinaryExpression {
            left: Box::new(left_child),
            op,
            right: Box::new(right_child),
        })
    }

    pub fn build_aggr_function(
        &self,
        op: String,
        distinct: bool,
        params: Vec<DataValue>,
        args: &Vec<Scalar>,
    ) -> Result<Expression> {
        let mut arg_exprs = Vec::with_capacity(args.len());
        for arg in args.iter() {
            arg_exprs.push(self.build(arg)?);
        }
        Ok(Expression::AggregateFunction {
            op,
            distinct,
            params,
            args: arg_exprs,
        })
    }
}
