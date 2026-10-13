use crate::{
    interceptors::{RawResponse, RequestContext, WireRequest},
    lifecycle::{CallEvent, Timing},
};

mod adapter;
mod chain;

pub use adapter::{ChainStep, DynCallHooks, HookAdapter};
pub use chain::{ChainRuntime, HookChain, HookGroupAdapter};

pub type HookResume<R, H, T> = fn(
    &mut H,
    <R as HookRuntime>::Context<'_>,
    Result<<R as HookRuntime>::Resumed, <R as HookRuntime>::Error>,
) -> Result<HookStep<R, H, T>, <R as HookRuntime>::Error>;

pub enum HookStep<R: HookRuntime, H, T> {
    Await(R::Awaitable, HookResume<R, H, T>),
    Ready(T),
}

pub trait HookRuntime: Sized {
    type Context<'a>: Copy;
    type Arguments;
    type Response;
    type Chunk;
    type Error;
    type Awaitable;
    type Resumed;
}

pub type RuntimeCallEvent<'a, R> =
    CallEvent<&'a <R as HookRuntime>::Response, &'a <R as HookRuntime>::Error, &'a RawResponse>;

pub trait CallHooks<R: HookRuntime>: Sized {
    fn prepare_arguments(
        &mut self,
        _runtime: R::Context<'_>,
        arguments: R::Arguments,
        _started_at: f64,
    ) -> Result<HookStep<R, Self, R::Arguments>, R::Error> {
        Ok(HookStep::Ready(arguments))
    }

    fn arguments_prepared(
        &mut self,
        _runtime: R::Context<'_>,
        _arguments: &R::Arguments,
    ) -> Result<(), R::Error> {
        Ok(())
    }

    fn before_provider_request(
        &mut self,
        _runtime: R::Context<'_>,
        wire: Box<WireRequest>,
        _context: &RequestContext,
    ) -> Result<HookStep<R, Self, Box<WireRequest>>, R::Error> {
        Ok(HookStep::Ready(wire))
    }

    fn transform_response(
        &mut self,
        _runtime: R::Context<'_>,
        response: R::Response,
        _timing: Timing,
    ) -> Result<HookStep<R, Self, R::Response>, R::Error> {
        Ok(HookStep::Ready(response))
    }

    fn on_event(
        &mut self,
        _runtime: R::Context<'_>,
        _event: RuntimeCallEvent<'_, R>,
    ) -> Result<HookStep<R, Self, ()>, R::Error> {
        Ok(HookStep::Ready(()))
    }

    fn on_stream_open(
        &mut self,
        _runtime: R::Context<'_>,
        _head: &R::Response,
    ) -> Result<(), R::Error> {
        Ok(())
    }

    fn on_stream_chunk(
        &mut self,
        _runtime: R::Context<'_>,
        _chunk: &R::Chunk,
    ) -> Result<(), R::Error> {
        Ok(())
    }
}
