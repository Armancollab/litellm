use super::chain::ChainRuntime;
use super::{CallHooks, HookResume, HookRuntime, HookStep, RuntimeCallEvent};
use crate::{
    interceptors::{RequestContext, WireRequest},
    lifecycle::Timing,
};

pub enum ChainStep<R: HookRuntime, T> {
    Ready(T),
    Await(R::Awaitable),
}

enum Continuation<R: HookRuntime, H> {
    Arguments(HookResume<R, H, R::Arguments>),
    Wire(HookResume<R, H, Box<WireRequest>>),
    Response(HookResume<R, H, R::Response>),
    Event(HookResume<R, H, ()>),
}

pub struct HookAdapter<R: HookRuntime, H> {
    hooks: H,
    continuation: Option<Continuation<R, H>>,
}

impl<R: HookRuntime, H> HookAdapter<R, H> {
    pub fn new(hooks: H) -> Self {
        Self {
            hooks,
            continuation: None,
        }
    }

    pub fn hooks(&self) -> &H {
        &self.hooks
    }

    pub fn hooks_mut(&mut self) -> &mut H {
        &mut self.hooks
    }

    pub fn clear_pending(&mut self) {
        self.continuation = None;
    }

    fn step<T>(
        &mut self,
        step: HookStep<R, H, T>,
        continuation: impl FnOnce(HookResume<R, H, T>) -> Continuation<R, H>,
    ) -> ChainStep<R, T> {
        match step {
            HookStep::Ready(value) => ChainStep::Ready(value),
            HookStep::Await(awaitable, resume) => {
                self.continuation = Some(continuation(resume));
                ChainStep::Await(awaitable)
            }
        }
    }
}

pub trait DynCallHooks<R: HookRuntime> {
    fn prepare_arguments(
        &mut self,
        runtime: R::Context<'_>,
        arguments: R::Arguments,
        started_at: f64,
    ) -> Result<ChainStep<R, R::Arguments>, R::Error>;
    fn resume_arguments(
        &mut self,
        runtime: R::Context<'_>,
        result: Result<R::Resumed, R::Error>,
    ) -> Result<ChainStep<R, R::Arguments>, R::Error>;
    fn before_provider_request(
        &mut self,
        runtime: R::Context<'_>,
        wire: Box<WireRequest>,
        context: &RequestContext,
    ) -> Result<ChainStep<R, Box<WireRequest>>, R::Error>;
    fn resume_wire(
        &mut self,
        runtime: R::Context<'_>,
        result: Result<R::Resumed, R::Error>,
    ) -> Result<ChainStep<R, Box<WireRequest>>, R::Error>;
    fn transform_response(
        &mut self,
        runtime: R::Context<'_>,
        response: R::Response,
        timing: Timing,
    ) -> Result<ChainStep<R, R::Response>, R::Error>;
    fn resume_response(
        &mut self,
        runtime: R::Context<'_>,
        result: Result<R::Resumed, R::Error>,
    ) -> Result<ChainStep<R, R::Response>, R::Error>;
    fn on_event(
        &mut self,
        runtime: R::Context<'_>,
        event: RuntimeCallEvent<'_, R>,
    ) -> Result<ChainStep<R, ()>, R::Error>;
    fn resume_event(
        &mut self,
        runtime: R::Context<'_>,
        result: Result<R::Resumed, R::Error>,
    ) -> Result<ChainStep<R, ()>, R::Error>;
    fn arguments_prepared(
        &mut self,
        runtime: R::Context<'_>,
        arguments: &R::Arguments,
    ) -> Result<(), R::Error>;
    fn on_stream_open(
        &mut self,
        runtime: R::Context<'_>,
        head: &R::Response,
    ) -> Result<(), R::Error>;
    fn on_stream_chunk(
        &mut self,
        runtime: R::Context<'_>,
        chunk: &R::Chunk,
    ) -> Result<(), R::Error>;
}

impl<R: ChainRuntime, H: CallHooks<R>> DynCallHooks<R> for HookAdapter<R, H> {
    fn prepare_arguments(
        &mut self,
        runtime: R::Context<'_>,
        arguments: R::Arguments,
        started_at: f64,
    ) -> Result<ChainStep<R, R::Arguments>, R::Error> {
        let step = self
            .hooks
            .prepare_arguments(runtime, arguments, started_at)?;
        Ok(self.step(step, Continuation::Arguments))
    }

    fn resume_arguments(
        &mut self,
        runtime: R::Context<'_>,
        result: Result<R::Resumed, R::Error>,
    ) -> Result<ChainStep<R, R::Arguments>, R::Error> {
        let Some(Continuation::Arguments(resume)) = self.continuation.take() else {
            return Err(R::missing_state());
        };
        let step = resume(&mut self.hooks, runtime, result)?;
        Ok(self.step(step, Continuation::Arguments))
    }

    fn before_provider_request(
        &mut self,
        runtime: R::Context<'_>,
        wire: Box<WireRequest>,
        context: &RequestContext,
    ) -> Result<ChainStep<R, Box<WireRequest>>, R::Error> {
        let step = self.hooks.before_provider_request(runtime, wire, context)?;
        Ok(self.step(step, Continuation::Wire))
    }

    fn resume_wire(
        &mut self,
        runtime: R::Context<'_>,
        result: Result<R::Resumed, R::Error>,
    ) -> Result<ChainStep<R, Box<WireRequest>>, R::Error> {
        let Some(Continuation::Wire(resume)) = self.continuation.take() else {
            return Err(R::missing_state());
        };
        let step = resume(&mut self.hooks, runtime, result)?;
        Ok(self.step(step, Continuation::Wire))
    }

    fn transform_response(
        &mut self,
        runtime: R::Context<'_>,
        response: R::Response,
        timing: Timing,
    ) -> Result<ChainStep<R, R::Response>, R::Error> {
        let step = self.hooks.transform_response(runtime, response, timing)?;
        Ok(self.step(step, Continuation::Response))
    }

    fn resume_response(
        &mut self,
        runtime: R::Context<'_>,
        result: Result<R::Resumed, R::Error>,
    ) -> Result<ChainStep<R, R::Response>, R::Error> {
        let Some(Continuation::Response(resume)) = self.continuation.take() else {
            return Err(R::missing_state());
        };
        let step = resume(&mut self.hooks, runtime, result)?;
        Ok(self.step(step, Continuation::Response))
    }

    fn on_event(
        &mut self,
        runtime: R::Context<'_>,
        event: RuntimeCallEvent<'_, R>,
    ) -> Result<ChainStep<R, ()>, R::Error> {
        let step = self.hooks.on_event(runtime, event)?;
        Ok(self.step(step, Continuation::Event))
    }

    fn resume_event(
        &mut self,
        runtime: R::Context<'_>,
        result: Result<R::Resumed, R::Error>,
    ) -> Result<ChainStep<R, ()>, R::Error> {
        let Some(Continuation::Event(resume)) = self.continuation.take() else {
            return Err(R::missing_state());
        };
        let step = resume(&mut self.hooks, runtime, result)?;
        Ok(self.step(step, Continuation::Event))
    }

    fn arguments_prepared(
        &mut self,
        runtime: R::Context<'_>,
        arguments: &R::Arguments,
    ) -> Result<(), R::Error> {
        self.hooks.arguments_prepared(runtime, arguments)
    }

    fn on_stream_open(
        &mut self,
        runtime: R::Context<'_>,
        head: &R::Response,
    ) -> Result<(), R::Error> {
        self.hooks.on_stream_open(runtime, head)
    }

    fn on_stream_chunk(
        &mut self,
        runtime: R::Context<'_>,
        chunk: &R::Chunk,
    ) -> Result<(), R::Error> {
        self.hooks.on_stream_chunk(runtime, chunk)
    }
}
