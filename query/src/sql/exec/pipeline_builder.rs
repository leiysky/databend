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
use std::collections::BTreeSet;
use std::fmt::Display;
use std::sync::Arc;

use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::aggregates::get_layout_offsets;
use common_functions::aggregates::AggregateFunction;
use common_functions::aggregates::AggregateFunctionFactory;
use common_planners::Expression;

use super::expression_builder_v2::ExpressionBuilderV2;
use crate::pipelines::new::processors::AggregatorParams;
use crate::pipelines::new::processors::AggregatorTransformParams;
use crate::pipelines::new::processors::TransformAggregator;
use crate::pipelines::new::processors::TransformFilterV2;
use crate::pipelines::new::processors::TransformRename;
use crate::pipelines::new::processors::TransformScalar;
use crate::pipelines::new::NewPipeline;
use crate::sessions::QueryContext;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::AggregatePlan;
use crate::sql::plans::BasePlanImpl;
use crate::sql::plans::FilterPlan;
use crate::sql::plans::PhysicalScan;
use crate::sql::plans::ProjectPlan;
use crate::sql::plans::Scalar;
use crate::sql::plans::ScalarExpr;
use crate::sql::IndexType;
use crate::sql::Metadata;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct ColumnOrdinal(pub IndexType);

impl Display for ColumnOrdinal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<String> for ColumnOrdinal {
    type Error = ErrorCode;

    fn try_from(value: String) -> Result<Self> {
        Ok(Self(value.parse::<IndexType>()?))
    }
}

pub type OutputColumns = BTreeSet<ColumnOrdinal>;

pub struct PipelineBuilder {
    ctx: Arc<QueryContext>,
    metadata: Metadata,
    result: Vec<(IndexType, String)>,
    s_expr: SExpr,
}

impl PipelineBuilder {
    pub fn new(
        ctx: Arc<QueryContext>,
        metadata: Metadata,
        result: Vec<(IndexType, String)>,
        s_expr: SExpr,
    ) -> Self {
        Self {
            metadata,
            result,
            ctx,
            s_expr,
        }
    }

    /// Build DataSchema with OutputColumns.
    /// The column name would be stored in metadata as a mapping `Ordinal -> name`
    /// for debugging.
    pub fn build_data_schema(&self, output_columns: &OutputColumns) -> DataSchemaRef {
        let mut schema_meta = BTreeMap::new();
        let mut fields = Vec::with_capacity(output_columns.len());
        for ord in output_columns.iter() {
            let ordinal_name = ord.to_string();
            let column_entry = self.metadata.column(ord.0);
            let meta_name = column_entry.name.clone();
            schema_meta.insert(ordinal_name.clone(), meta_name);

            let field = DataField::new(ordinal_name.as_str(), column_entry.data_type.clone());
            fields.push(field);
        }

        DataSchemaRefExt::with_metadata(fields, schema_meta)
    }

    pub fn spawn(mut self) -> Result<(NewPipeline, Vec<NewPipeline>)> {
        let mut pipeline = NewPipeline::create();
        let s_expr = self.s_expr.clone();
        let result = self.build(self.ctx.clone(), &mut pipeline, &s_expr)?;
        self.build_rename(self.ctx.clone(), &mut pipeline)?;
        Ok((pipeline, vec![]))
    }

    pub fn build_rename(
        &mut self,
        ctx: Arc<QueryContext>,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        let expressions: Vec<(String, String)> = self
            .result
            .iter()
            .map(|(idx, output)| (ColumnOrdinal(*idx).to_string(), output.clone()))
            .collect();
        pipeline.add_transform(|input, output| {
            Ok(TransformRename::new(input, output, expressions.clone()))
        })?;
        Ok(())
    }

    pub fn build(
        &mut self,
        ctx: Arc<QueryContext>,
        pipeline: &mut NewPipeline,
        s_expr: &SExpr,
    ) -> Result<OutputColumns> {
        match s_expr.plan() {
            BasePlanImpl::PhysicalScan(scan) => self.build_scan(ctx, pipeline, &scan),
            BasePlanImpl::PhysicalHashJoin(_) => todo!(),
            BasePlanImpl::Project(project) => {
                self.build_project(ctx, pipeline, &project, s_expr.child(0)?)
            }
            BasePlanImpl::Filter(filter) => {
                self.build_filter(ctx, pipeline, &filter, s_expr.child(0)?)
            }
            BasePlanImpl::Aggregate(aggregate) => {
                self.build_aggregate(ctx, pipeline, &aggregate, s_expr.child(0)?)
            }
            BasePlanImpl::Sort(_) => todo!(),
            BasePlanImpl::Limit(_) => todo!(),
            _ => Err(ErrorCode::LogicalError(format!(
                "Invalid physical plan: {:?}",
                s_expr
            ))),
        }
    }

    pub fn build_project(
        &mut self,
        ctx: Arc<QueryContext>,
        pipeline: &mut NewPipeline,
        project: &ProjectPlan,
        child: &SExpr,
    ) -> Result<OutputColumns> {
        let input_columns = self.build(ctx.clone(), pipeline, child)?;
        let eb = ExpressionBuilderV2::create(&self.metadata);
        let expressions: Vec<(Expression, DataField)> = project
            .items
            .iter()
            .map(|item| {
                let alias = ColumnOrdinal(item.index).to_string();
                let data_field = DataField::new(alias.as_str(), item.expr.data_type());
                let scalar = eb.build(&item.expr)?;
                Ok((scalar, data_field))
            })
            .collect::<Result<_>>()?;
        pipeline.add_transform(|input, output| {
            Ok(TransformScalar::new(
                input,
                output,
                ctx.clone(),
                expressions.clone(),
            ))
        })?;

        let output = OutputColumns::from(project.items.iter().map(|item| item.index).collect());
        Ok(output)
    }

    pub fn build_filter(
        &mut self,
        ctx: Arc<QueryContext>,
        pipeline: &mut NewPipeline,
        filter: &FilterPlan,
        child: &SExpr,
    ) -> Result<OutputColumns> {
        let input_columns = self.build(ctx.clone(), pipeline, child)?;
        let eb = ExpressionBuilderV2::create(&self.metadata);
        let predicates = filter
            .predicates
            .iter()
            .map(|scalar| eb.build(scalar))
            .collect::<Result<Vec<Expression>>>()?;
        pipeline.add_transform(|input, output| {
            Ok(TransformFilterV2::new(
                input,
                output,
                ctx.clone(),
                predicates.clone(),
            ))
        })?;

        Ok(input_columns)
    }

    pub fn build_scan(
        &mut self,
        ctx: Arc<QueryContext>,
        pipeline: &mut NewPipeline,
        scan: &PhysicalScan,
    ) -> Result<OutputColumns> {
        let table_entry = self.metadata.table(scan.table_index);
        let table = table_entry.table.clone();
        let source = &table_entry.source;
        ctx.try_set_partitions(source.parts.clone())?;
        table.read2(ctx.clone(), source, pipeline);

        // TODO(leiysky): push the projection down to data source
        let columns: Vec<IndexType> = scan.columns.iter().cloned().collect();
        let renames: Vec<(String, String)> = columns
            .iter()
            .map(|index| {
                let column_entry = self.metadata.column(*index);
                (column_entry.name.clone(), ColumnOrdinal(*index).to_string())
            })
            .collect();
        pipeline.add_transform(|input, output| {
            Ok(TransformRename::new(input, output, renames.clone()))
        })?;

        let output_columns: OutputColumns = scan
            .columns
            .iter()
            .map(|index| ColumnOrdinal(*index))
            .collect();
        Ok(output_columns)
    }

    pub fn build_aggregate(
        &mut self,
        ctx: Arc<QueryContext>,
        pipeline: &mut NewPipeline,
        aggregate: &AggregatePlan,
        child: &SExpr,
    ) -> Result<OutputColumns> {
        let input_columns = self.build(ctx.clone(), pipeline, child)?;

        let mut output_columns = OutputColumns::new();
        for group_item in aggregate.group_items.iter() {
            let ord = ColumnOrdinal(group_item.index);
            output_columns.insert(ord);
        }

        for agg_item in aggregate.aggregate_functions.iter() {
            let ord = ColumnOrdinal(agg_item.index);
            output_columns.insert(ord);
        }

        let group_columns_name: Vec<String> = aggregate
            .group_items
            .iter()
            .map(|item| ColumnOrdinal(item.index).to_string())
            .collect();

        let group_data_fields: Vec<DataField> = aggregate
            .group_items
            .iter()
            .map(|item| DataField::new("", item.expr.data_type()))
            .collect();

        let agg_factory = AggregateFunctionFactory::instance();
        let aggregate_functions: Vec<Arc<dyn AggregateFunction>> = aggregate
            .aggregate_functions
            .iter()
            .map(|agg| {
                let fields = agg
                    .aggregate
                    .args
                    .iter()
                    .map(|arg| DataField::new("", arg.data_type()))
                    .collect();
                agg_factory.get(
                    agg.aggregate.func_name.as_str(),
                    agg.aggregate.params.clone(),
                    fields,
                )
            })
            .collect::<Result<_>>()?;

        let aggregate_functions_column_name: Vec<String> = aggregate
            .aggregate_functions
            .iter()
            .map(|item| ColumnOrdinal(item.index).to_string())
            .collect();

        let aggregate_functions_arguments_name: Vec<Vec<String>> = aggregate
            .aggregate_functions
            .iter()
            .map(|item| {
                item.aggregate
                    .args
                    .iter()
                    .map(|arg| {
                        if let Scalar::BoundColumnRef(col) = arg {
                            Ok(ColumnOrdinal(col.column.index).to_string())
                        } else {
                            Err(ErrorCode::LogicalError("Invalid aggregate argument"))
                        }
                    })
                    .collect::<Result<Vec<String>>>()
            })
            .collect::<Result<Vec<Vec<String>>>>()?;

        let mut states_offset = Vec::with_capacity(aggregate_functions.len());
        let layout = if !aggregate_functions.is_empty() {
            Some(get_layout_offsets(
                aggregate_functions.as_slice(),
                &mut states_offset,
            )?)
        } else {
            None
        };

        let params = Arc::new(AggregatorParams {
            schema: self.build_data_schema(&output_columns),
            before_schema: self.build_data_schema(&input_columns),
            group_columns_name,
            group_data_fields,
            aggregate_functions,
            aggregate_functions_column_name,
            aggregate_functions_arguments_name,
            layout,
            offsets_aggregate_states: states_offset,
        });

        pipeline.add_transform(|input, output| {
            TransformAggregator::try_create_partial(
                input.clone(),
                output.clone(),
                AggregatorTransformParams::try_create(input, output, &params)?,
                ctx.clone(),
            )
        })?;

        pipeline.resize(1)?;
        pipeline.add_transform(|input, output| {
            TransformAggregator::try_create_final(
                input.clone(),
                output.clone(),
                AggregatorTransformParams::try_create(input, output, &params)?,
                ctx.clone(),
            )
        })?;

        Ok(output_columns)
    }
}
