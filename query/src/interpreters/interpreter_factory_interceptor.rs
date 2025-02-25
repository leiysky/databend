// Copyright 2021 Datafuse Labs.
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
use std::time::SystemTime;

use common_exception::Result;
use common_infallible::Mutex;
use common_planners::PlanNode;
use common_streams::ErrorStream;
use common_streams::ProgressStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::interpreters::InterpreterQueryLog;
use crate::pipelines::new::SourcePipeBuilder;
use crate::sessions::QueryContext;

pub struct InterceptorInterpreter {
    ctx: Arc<QueryContext>,
    inner: InterpreterPtr,
    query_log: InterpreterQueryLog,
    source_pipe_builder: Mutex<Option<SourcePipeBuilder>>,
}

impl InterceptorInterpreter {
    pub fn create(ctx: Arc<QueryContext>, inner: InterpreterPtr, plan: PlanNode) -> Self {
        InterceptorInterpreter {
            ctx: ctx.clone(),
            inner,
            query_log: InterpreterQueryLog::create(ctx, Some(plan)),
            source_pipe_builder: Mutex::new(None),
        }
    }
}

#[async_trait::async_trait]
impl Interpreter for InterceptorInterpreter {
    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn execute(
        &self,
        input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let _ = self
            .inner
            .set_source_pipe_builder((*self.source_pipe_builder.lock()).clone());
        let result_stream = match self.inner.execute(input_stream).await {
            Ok(s) => s,
            Err(e) => {
                self.ctx.set_error(e.clone());
                return Err(e);
            }
        };

        let error_stream = ErrorStream::create(result_stream, self.ctx.get_error());
        let metric_stream =
            ProgressStream::try_create(Box::pin(error_stream), self.ctx.get_result_progress())?;
        Ok(Box::pin(metric_stream))
    }

    async fn start(&self) -> Result<()> {
        let session = self.ctx.get_current_session();
        let now = SystemTime::now();
        if session.get_type().is_user_session() {
            session
                .get_session_manager()
                .status
                .write()
                .query_start(now);
        }
        self.query_log.log_start(now, None).await
    }

    async fn finish(&self) -> Result<()> {
        let session = self.ctx.get_current_session();
        let now = SystemTime::now();
        session.get_status().write().query_finish();
        if session.get_type().is_user_session() {
            session
                .get_session_manager()
                .status
                .write()
                .query_finish(now)
        }
        let error = self.ctx.get_error_value();
        self.query_log.log_finish(now, error).await
    }

    fn set_source_pipe_builder(&self, builder: Option<SourcePipeBuilder>) -> Result<()> {
        let mut guard = self.source_pipe_builder.lock();
        *guard = builder;
        Ok(())
    }
}
