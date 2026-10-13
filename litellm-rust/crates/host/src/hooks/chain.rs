use super::adapter::{ChainStep, DynCallHooks};
use super::{CallHooks, HookRuntime, HookStep, RuntimeCallEvent};
use crate::{
    interceptors::{RequestContext, WireRequest},
    lifecycle::{CallEvent, Timing},
};

pub trait ChainRuntime: HookRuntime {
    type Group: DynCallHooks<Self> + ?Sized;
    type OwnedEvent;

    fn missing_state() -> Self::Error;
    fn resume_result(
        _runtime: Self::Context<'_>,
        result: Result<Self::Resumed, Self::Error>,
    ) -> Result<Result<Self::Resumed, Self::Error>, Self::Error> {
        Ok(result)
    }
    fn retain_event(
        runtime: Self::Context<'_>,
        event: RuntimeCallEvent<'_, Self>,
    ) -> Self::OwnedEvent;
    fn dispatch_event(
        runtime: Self::Context<'_>,
        hooks: &mut Self::Group,
        event: &Self::OwnedEvent,
    ) -> Result<ChainStep<Self, ()>, Self::Error>;
    fn notification_result(
        _runtime: Self::Context<'_>,
        _terminal: bool,
        result: Result<ChainStep<Self, ()>, Self::Error>,
    ) -> Result<ChainStep<Self, ()>, Self::Error> {
        result
    }
}

pub trait HookGroupAdapter<H>: ChainRuntime {
    fn adapt_group(hooks: H) -> Box<Self::Group>;
}

pub struct HookChain<R: ChainRuntime> {
    hooks: Vec<Box<R::Group>>,
    arguments: Option<(usize, f64)>,
    wire: Option<(usize, RequestContext)>,
    response: Option<(usize, Timing)>,
    event: Option<(usize, bool, R::OwnedEvent)>,
}

impl<R: ChainRuntime> HookChain<R> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with<H>(mut self, hooks: H) -> Self
    where
        R: HookGroupAdapter<H>,
    {
        self.hooks.push(R::adapt_group(hooks));
        self
    }

    pub fn groups(&self) -> impl Iterator<Item = &R::Group> {
        self.hooks.iter().map(Box::as_ref)
    }

    pub fn groups_mut(&mut self) -> impl Iterator<Item = &mut R::Group> {
        self.hooks.iter_mut().map(Box::as_mut)
    }

    pub fn pending_event(&self) -> Option<&R::OwnedEvent> {
        self.event.as_ref().map(|(_, _, event)| event)
    }

    pub fn clear_pending(&mut self) {
        self.arguments = None;
        self.wire = None;
        self.response = None;
        self.event = None;
    }

    fn arguments_from(
        &mut self,
        runtime: R::Context<'_>,
        index: usize,
        arguments: R::Arguments,
        started_at: f64,
    ) -> Result<HookStep<R, Self, R::Arguments>, R::Error> {
        let Some(hooks) = self.hooks.get_mut(index) else {
            return Ok(HookStep::Ready(arguments));
        };
        let step = hooks.prepare_arguments(runtime, arguments, started_at)?;
        self.arguments_step(runtime, index, step, started_at)
    }

    fn arguments_step(
        &mut self,
        runtime: R::Context<'_>,
        index: usize,
        step: ChainStep<R, R::Arguments>,
        started_at: f64,
    ) -> Result<HookStep<R, Self, R::Arguments>, R::Error> {
        match step {
            ChainStep::Ready(value) => self.arguments_from(runtime, index + 1, value, started_at),
            ChainStep::Await(awaitable) => {
                self.arguments = Some((index, started_at));
                Ok(HookStep::Await(awaitable, Self::resume_arguments))
            }
        }
    }

    fn resume_arguments(
        &mut self,
        runtime: R::Context<'_>,
        result: Result<R::Resumed, R::Error>,
    ) -> Result<HookStep<R, Self, R::Arguments>, R::Error> {
        let (index, context) = self.arguments.take().ok_or_else(R::missing_state)?;
        let result = R::resume_result(runtime, result)?;
        let step = self.hooks[index].resume_arguments(runtime, result)?;
        self.arguments_step(runtime, index, step, context)
    }

    fn wire_from(
        &mut self,
        runtime: R::Context<'_>,
        index: usize,
        wire: Box<WireRequest>,
        context: &RequestContext,
    ) -> Result<HookStep<R, Self, Box<WireRequest>>, R::Error> {
        let Some(hooks) = self.hooks.get_mut(index) else {
            return Ok(HookStep::Ready(wire));
        };
        let step = hooks.before_provider_request(runtime, wire, context)?;
        self.wire_step(runtime, index, step, context)
    }

    fn wire_step(
        &mut self,
        runtime: R::Context<'_>,
        index: usize,
        step: ChainStep<R, Box<WireRequest>>,
        context: &RequestContext,
    ) -> Result<HookStep<R, Self, Box<WireRequest>>, R::Error> {
        match step {
            ChainStep::Ready(value) => self.wire_from(runtime, index + 1, value, context),
            ChainStep::Await(awaitable) => {
                self.wire = Some((index, context.clone()));
                Ok(HookStep::Await(awaitable, Self::resume_wire))
            }
        }
    }

    fn resume_wire(
        &mut self,
        runtime: R::Context<'_>,
        result: Result<R::Resumed, R::Error>,
    ) -> Result<HookStep<R, Self, Box<WireRequest>>, R::Error> {
        let (index, context) = self.wire.take().ok_or_else(R::missing_state)?;
        let result = R::resume_result(runtime, result)?;
        let step = self.hooks[index].resume_wire(runtime, result)?;
        self.wire_step(runtime, index, step, &context)
    }

    fn response_from(
        &mut self,
        runtime: R::Context<'_>,
        index: usize,
        response: R::Response,
        timing: Timing,
    ) -> Result<HookStep<R, Self, R::Response>, R::Error> {
        let Some(hooks) = self.hooks.get_mut(index) else {
            return Ok(HookStep::Ready(response));
        };
        let step = hooks.transform_response(runtime, response, timing)?;
        self.response_step(runtime, index, step, timing)
    }

    fn response_step(
        &mut self,
        runtime: R::Context<'_>,
        index: usize,
        step: ChainStep<R, R::Response>,
        timing: Timing,
    ) -> Result<HookStep<R, Self, R::Response>, R::Error> {
        match step {
            ChainStep::Ready(value) => self.response_from(runtime, index + 1, value, timing),
            ChainStep::Await(awaitable) => {
                self.response = Some((index, timing));
                Ok(HookStep::Await(awaitable, Self::resume_response))
            }
        }
    }

    fn resume_response(
        &mut self,
        runtime: R::Context<'_>,
        result: Result<R::Resumed, R::Error>,
    ) -> Result<HookStep<R, Self, R::Response>, R::Error> {
        let (index, context) = self.response.take().ok_or_else(R::missing_state)?;
        let result = R::resume_result(runtime, result)?;
        let step = self.hooks[index].resume_response(runtime, result)?;
        self.response_step(runtime, index, step, context)
    }

    fn event_from(
        &mut self,
        runtime: R::Context<'_>,
        index: usize,
        event: R::OwnedEvent,
        terminal: bool,
    ) -> Result<HookStep<R, Self, ()>, R::Error> {
        let Some(hooks) = self.hooks.get_mut(index) else {
            return Ok(HookStep::Ready(()));
        };
        let result = R::dispatch_event(runtime, hooks.as_mut(), &event);
        let step = R::notification_result(runtime, terminal, result)?;
        self.event_step(runtime, index, step, event, terminal)
    }

    fn event_step(
        &mut self,
        runtime: R::Context<'_>,
        index: usize,
        step: ChainStep<R, ()>,
        event: R::OwnedEvent,
        terminal: bool,
    ) -> Result<HookStep<R, Self, ()>, R::Error> {
        match step {
            ChainStep::Ready(()) => self.event_from(runtime, index + 1, event, terminal),
            ChainStep::Await(awaitable) => {
                self.event = Some((index, terminal, event));
                Ok(HookStep::Await(awaitable, Self::resume_event))
            }
        }
    }

    fn resume_event(
        &mut self,
        runtime: R::Context<'_>,
        result: Result<R::Resumed, R::Error>,
    ) -> Result<HookStep<R, Self, ()>, R::Error> {
        let (index, terminal, event) = self.event.take().ok_or_else(R::missing_state)?;
        let result = R::resume_result(runtime, result)?;
        let result = self.hooks[index].resume_event(runtime, result);
        let step = R::notification_result(runtime, terminal, result)?;
        self.event_step(runtime, index, step, event, terminal)
    }
}

impl<R: ChainRuntime> Default for HookChain<R> {
    fn default() -> Self {
        Self {
            hooks: Vec::new(),
            arguments: None,
            wire: None,
            response: None,
            event: None,
        }
    }
}

impl<R: ChainRuntime> CallHooks<R> for HookChain<R> {
    fn prepare_arguments(
        &mut self,
        runtime: R::Context<'_>,
        arguments: R::Arguments,
        started_at: f64,
    ) -> Result<HookStep<R, Self, R::Arguments>, R::Error> {
        self.arguments_from(runtime, 0, arguments, started_at)
    }

    fn arguments_prepared(
        &mut self,
        runtime: R::Context<'_>,
        arguments: &R::Arguments,
    ) -> Result<(), R::Error> {
        self.hooks
            .iter_mut()
            .try_for_each(|hooks| hooks.arguments_prepared(runtime, arguments))
    }

    fn before_provider_request(
        &mut self,
        runtime: R::Context<'_>,
        wire: Box<WireRequest>,
        context: &RequestContext,
    ) -> Result<HookStep<R, Self, Box<WireRequest>>, R::Error> {
        self.wire_from(runtime, 0, wire, context)
    }

    fn transform_response(
        &mut self,
        runtime: R::Context<'_>,
        response: R::Response,
        timing: Timing,
    ) -> Result<HookStep<R, Self, R::Response>, R::Error> {
        self.response_from(runtime, 0, response, timing)
    }

    fn on_event(
        &mut self,
        runtime: R::Context<'_>,
        event: RuntimeCallEvent<'_, R>,
    ) -> Result<HookStep<R, Self, ()>, R::Error> {
        let terminal = matches!(
            event,
            CallEvent::Succeeded { .. } | CallEvent::Failed { .. }
        );
        for (index, hooks) in self.hooks.iter_mut().enumerate() {
            let result = hooks.on_event(runtime, event.clone());
            match R::notification_result(runtime, terminal, result)? {
                ChainStep::Ready(()) => {}
                ChainStep::Await(awaitable) => {
                    self.event = Some((index, terminal, R::retain_event(runtime, event)));
                    return Ok(HookStep::Await(awaitable, Self::resume_event));
                }
            }
        }
        Ok(HookStep::Ready(()))
    }

    fn on_stream_open(
        &mut self,
        runtime: R::Context<'_>,
        head: &R::Response,
    ) -> Result<(), R::Error> {
        self.hooks
            .iter_mut()
            .try_for_each(|hooks| hooks.on_stream_open(runtime, head))
    }

    fn on_stream_chunk(
        &mut self,
        runtime: R::Context<'_>,
        chunk: &R::Chunk,
    ) -> Result<(), R::Error> {
        self.hooks
            .iter_mut()
            .try_for_each(|hooks| hooks.on_stream_chunk(runtime, chunk))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::hooks::{HookAdapter, HookResume};
    use crate::lifecycle::ExecutionEvent;
    use rstest::{fixture, rstest};
    use std::cell::RefCell;

    struct TestRuntime;

    impl HookRuntime for TestRuntime {
        type Context<'a> = &'a RefCell<Vec<String>>;
        type Arguments = String;
        type Response = String;
        type Chunk = u32;
        type Error = &'static str;
        type Awaitable = String;
        type Resumed = String;
    }

    impl ChainRuntime for TestRuntime {
        type Group = dyn DynCallHooks<Self>;
        type OwnedEvent = CallEvent<String, &'static str>;

        fn missing_state() -> Self::Error {
            "missing continuation"
        }

        fn retain_event(
            _: Self::Context<'_>,
            event: RuntimeCallEvent<'_, Self>,
        ) -> Self::OwnedEvent {
            match event {
                CallEvent::Started { start_time } => CallEvent::Started { start_time },
                CallEvent::Execution(ExecutionEvent::ResultReady { facts }) => {
                    CallEvent::Execution(ExecutionEvent::ResultReady { facts })
                }
                CallEvent::Execution(ExecutionEvent::ProviderResponseReceived { raw }) => {
                    CallEvent::Execution(ExecutionEvent::ProviderResponseReceived {
                        raw: raw.clone(),
                    })
                }
                CallEvent::Succeeded { timing, response } => CallEvent::Succeeded {
                    timing,
                    response: response.clone(),
                },
                CallEvent::Failed {
                    timing,
                    origin,
                    error,
                } => CallEvent::Failed {
                    timing,
                    origin,
                    error,
                },
                CallEvent::Cancelled { timing } => CallEvent::Cancelled { timing },
            }
        }

        fn dispatch_event(
            runtime: Self::Context<'_>,
            hooks: &mut Self::Group,
            event: &Self::OwnedEvent,
        ) -> Result<ChainStep<Self, ()>, Self::Error> {
            let borrowed = match event {
                CallEvent::Started { start_time } => CallEvent::Started {
                    start_time: *start_time,
                },
                CallEvent::Execution(ExecutionEvent::ResultReady { facts }) => {
                    CallEvent::Execution(ExecutionEvent::ResultReady {
                        facts: facts.clone(),
                    })
                }
                CallEvent::Execution(ExecutionEvent::ProviderResponseReceived { raw }) => {
                    CallEvent::Execution(ExecutionEvent::ProviderResponseReceived { raw })
                }
                CallEvent::Succeeded { timing, response } => CallEvent::Succeeded {
                    timing: *timing,
                    response,
                },
                CallEvent::Failed {
                    timing,
                    origin,
                    error,
                } => CallEvent::Failed {
                    timing: *timing,
                    origin: *origin,
                    error,
                },
                CallEvent::Cancelled { timing } => CallEvent::Cancelled { timing: *timing },
            };
            hooks.on_event(runtime, borrowed)
        }
    }

    impl<H: CallHooks<TestRuntime> + 'static> HookGroupAdapter<H> for TestRuntime {
        fn adapt_group(hooks: H) -> Box<dyn DynCallHooks<TestRuntime>> {
            Box::new(HookAdapter::new(hooks))
        }
    }

    struct Group {
        name: &'static str,
        suspensions: u8,
        remaining: u8,
        reject: bool,
    }

    impl Group {
        fn step<T>(
            &mut self,
            value: T,
            awaitable: String,
            resume: HookResume<TestRuntime, Self, T>,
        ) -> Result<HookStep<TestRuntime, Self, T>, &'static str> {
            if self.reject {
                return Err("rejected");
            }
            if self.suspensions == 0 {
                return Ok(HookStep::Ready(value));
            }
            self.remaining = self.suspensions - 1;
            Ok(HookStep::<TestRuntime, Self, T>::Await(awaitable, resume))
        }

        fn resume_value(
            &mut self,
            _: &RefCell<Vec<String>>,
            result: Result<String, &'static str>,
        ) -> Result<HookStep<TestRuntime, Self, String>, &'static str> {
            let value = result?;
            if self.remaining == 0 {
                return Ok(HookStep::Ready(value));
            }
            self.remaining -= 1;
            Ok(HookStep::<TestRuntime, Self, String>::Await(
                value,
                Self::resume_value,
            ))
        }

        fn resume_notification(
            &mut self,
            _: &RefCell<Vec<String>>,
            result: Result<String, &'static str>,
        ) -> Result<HookStep<TestRuntime, Self, ()>, &'static str> {
            let value = result?;
            if self.remaining == 0 {
                return Ok(HookStep::Ready(()));
            }
            self.remaining -= 1;
            Ok(HookStep::<TestRuntime, Self, ()>::Await(
                value,
                Self::resume_notification,
            ))
        }
    }

    impl CallHooks<TestRuntime> for Group {
        fn prepare_arguments(
            &mut self,
            runtime: &RefCell<Vec<String>>,
            arguments: String,
            _: f64,
        ) -> Result<HookStep<TestRuntime, Self, String>, &'static str> {
            runtime.borrow_mut().push(format!("prepare:{}", self.name));
            let value = format!("{arguments}{}", self.name);
            self.step(value.clone(), value, Self::resume_value)
        }

        fn transform_response(
            &mut self,
            runtime: &RefCell<Vec<String>>,
            response: String,
            _: Timing,
        ) -> Result<HookStep<TestRuntime, Self, String>, &'static str> {
            runtime.borrow_mut().push(format!("response:{}", self.name));
            let value = format!("{response}{}", self.name);
            self.step(value.clone(), value, Self::resume_value)
        }

        fn on_event(
            &mut self,
            runtime: &RefCell<Vec<String>>,
            event: RuntimeCallEvent<'_, TestRuntime>,
        ) -> Result<HookStep<TestRuntime, Self, ()>, &'static str> {
            let value = match event {
                CallEvent::Succeeded { response, .. } => response.clone(),
                _ => String::new(),
            };
            runtime
                .borrow_mut()
                .push(format!("event:{}:{value}", self.name));
            self.step((), value, Self::resume_notification)
        }

        fn on_stream_chunk(
            &mut self,
            runtime: &RefCell<Vec<String>>,
            chunk: &u32,
        ) -> Result<(), &'static str> {
            runtime
                .borrow_mut()
                .push(format!("chunk:{}:{chunk}", self.name));
            Ok(())
        }
    }

    fn group(name: &'static str, suspensions: u8, reject: bool) -> Group {
        Group {
            name,
            suspensions,
            remaining: 0,
            reject,
        }
    }

    fn finish<T>(
        runtime: &RefCell<Vec<String>>,
        hooks: &mut HookChain<TestRuntime>,
        step: HookStep<TestRuntime, HookChain<TestRuntime>, T>,
    ) -> Result<T, &'static str> {
        match step {
            HookStep::Ready(value) => Ok(value),
            HookStep::Await(value, resume) => {
                let next = resume(hooks, runtime, Ok(value))?;
                finish(runtime, hooks, next)
            }
        }
    }

    #[fixture]
    fn trace() -> RefCell<Vec<String>> {
        RefCell::new(Vec::new())
    }

    #[fixture]
    fn timing() -> Timing {
        Timing {
            start_time: 0.0,
            end_time: 1.0,
        }
    }

    #[rstest]
    fn groups_compose_without_python(
        trace: RefCell<Vec<String>>,
        timing: Timing,
        #[values(0, 1, 2)] suspensions: u8,
    ) {
        let mut hooks = HookChain::<TestRuntime>::new()
            .with(group("a", suspensions, false))
            .with(group("b", suspensions, false));
        let prepared = hooks
            .prepare_arguments(&trace, "input".into(), 0.0)
            .unwrap();
        assert_eq!(finish(&trace, &mut hooks, prepared).unwrap(), "inputab");
        let transformed = hooks
            .transform_response(&trace, "output".into(), timing)
            .unwrap();
        let response = finish(&trace, &mut hooks, transformed).unwrap();
        assert_eq!(response, "outputab");
        let notified = hooks
            .on_event(
                &trace,
                CallEvent::Succeeded {
                    timing,
                    response: &response,
                },
            )
            .unwrap();
        finish(&trace, &mut hooks, notified).unwrap();
        hooks.on_stream_chunk(&trace, &42).unwrap();
        assert_eq!(
            *trace.borrow(),
            [
                "prepare:a",
                "prepare:b",
                "response:a",
                "response:b",
                "event:a:outputab",
                "event:b:outputab",
                "chunk:a:42",
                "chunk:b:42",
            ]
        );
        assert!(hooks.pending_event().is_none());
    }

    #[rstest]
    fn transformation_failure_skips_later_groups(
        trace: RefCell<Vec<String>>,
        #[values(0, 1)] suspensions: u8,
    ) {
        let mut hooks = HookChain::<TestRuntime>::new()
            .with(group("a", suspensions, false))
            .with(group("b", 0, true))
            .with(group("c", 0, false));
        let result = hooks
            .prepare_arguments(&trace, String::new(), 0.0)
            .and_then(|step| finish(&trace, &mut hooks, step));
        assert!(matches!(result, Err("rejected")));
        assert_eq!(*trace.borrow(), ["prepare:a", "prepare:b"]);
    }

    #[rstest]
    fn terminal_notification_errors_follow_the_runtime_policy(
        trace: RefCell<Vec<String>>,
        timing: Timing,
    ) {
        let mut hooks = HookChain::<TestRuntime>::new()
            .with(group("a", 0, true))
            .with(group("b", 0, false));
        let response = "selected".to_owned();
        let result = hooks.on_event(
            &trace,
            CallEvent::Succeeded {
                timing,
                response: &response,
            },
        );
        assert!(matches!(result, Err("rejected")));
        assert_eq!(*trace.borrow(), ["event:a:selected"]);
    }

    #[rstest]
    fn resumption_failure_skips_later_groups(trace: RefCell<Vec<String>>) {
        let mut hooks = HookChain::<TestRuntime>::new()
            .with(group("a", 1, false))
            .with(group("b", 0, false));
        let step = hooks.prepare_arguments(&trace, String::new(), 0.0).unwrap();
        let HookStep::Await(_, resume) = step else {
            panic!("expected suspension")
        };
        assert!(matches!(
            resume(&mut hooks, &trace, Err("await failed")),
            Err("await failed")
        ));
        assert_eq!(*trace.borrow(), ["prepare:a"]);
    }
}
