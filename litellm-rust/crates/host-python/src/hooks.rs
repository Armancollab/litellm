use crate::{PythonOwned, missing_state};
use litellm_host::{
    hooks::{
        CallHooks, ChainRuntime, ChainStep, DynCallHooks, HookAdapter, HookGroupAdapter,
        HookRuntime, RuntimeCallEvent,
    },
    interceptors::RawResponse,
    lifecycle::{CallEvent, ExecutionEvent},
};
use pyo3::{
    exceptions::{PyBaseException, PyException},
    gc::{PyTraverseError, PyVisit},
    prelude::*,
    types::PyDict,
};

pub type HookResume<H, T> = litellm_host::hooks::HookResume<PythonRuntime, H, T>;
pub type HookStep<H, T> = litellm_host::hooks::HookStep<PythonRuntime, H, T>;

pub struct PythonRuntime;

impl HookRuntime for PythonRuntime {
    type Context<'a> = Python<'a>;
    type Arguments = Py<PyDict>;
    type Response = Py<PyAny>;
    type Chunk = Py<PyAny>;
    type Error = PyErr;
    type Awaitable = Py<PyAny>;
    type Resumed = Py<PyAny>;
}

pub type PythonCallEvent<'a> = RuntimeCallEvent<'a, PythonRuntime>;

pub trait PythonCallHooks: CallHooks<PythonRuntime> + PythonOwned {}

impl<H: CallHooks<PythonRuntime> + PythonOwned> PythonCallHooks for H {}

pub type HookChain = litellm_host::hooks::HookChain<PythonRuntime>;
pub type OwnedEvent = CallEvent<Py<PyAny>, Py<PyBaseException>, RawResponse>;

pub trait PythonHookGroup: DynCallHooks<PythonRuntime> + PythonOwned {}
impl<H: DynCallHooks<PythonRuntime> + PythonOwned> PythonHookGroup for H {}

impl<H: PythonCallHooks + 'static> HookGroupAdapter<H> for PythonRuntime {
    fn adapt_group(hooks: H) -> Box<dyn PythonHookGroup> {
        Box::new(HookAdapter::new(hooks))
    }
}

impl<H: PythonOwned> PythonOwned for HookAdapter<PythonRuntime, H> {
    fn close(&mut self, py: Python<'_>) {
        self.clear_pending();
        self.hooks_mut().close(py);
    }

    fn traverse(&self, visit: &PyVisit<'_>) -> Result<(), PyTraverseError> {
        self.hooks().traverse(visit)
    }
}

impl ChainRuntime for PythonRuntime {
    type Group = dyn PythonHookGroup;
    type OwnedEvent = OwnedEvent;

    fn missing_state() -> PyErr {
        missing_state()
    }

    fn resume_result(py: Python<'_>, result: PyResult<Py<PyAny>>) -> PyResult<PyResult<Py<PyAny>>> {
        match result {
            Err(error) if !error.is_instance_of::<PyException>(py) => Err(error),
            result => Ok(result),
        }
    }

    fn retain_event(py: Python<'_>, event: PythonCallEvent<'_>) -> OwnedEvent {
        match event {
            CallEvent::Execution(ExecutionEvent::ResultReady { facts }) => {
                CallEvent::Execution(ExecutionEvent::ResultReady { facts })
            }
            CallEvent::Started { start_time } => CallEvent::Started { start_time },
            CallEvent::Execution(ExecutionEvent::ProviderResponseReceived { raw }) => {
                CallEvent::Execution(ExecutionEvent::ProviderResponseReceived { raw: raw.clone() })
            }
            CallEvent::Succeeded { timing, response } => CallEvent::Succeeded {
                timing,
                response: response.clone_ref(py),
            },
            CallEvent::Failed {
                timing,
                origin,
                error,
            } => CallEvent::Failed {
                timing,
                origin,
                error: error.clone_ref(py).into_value(py),
            },
            CallEvent::Cancelled { timing } => CallEvent::Cancelled { timing },
        }
    }

    fn dispatch_event(
        py: Python<'_>,
        hooks: &mut (dyn PythonHookGroup + 'static),
        event: &OwnedEvent,
    ) -> PyResult<ChainStep<Self, ()>> {
        match event {
            CallEvent::Execution(ExecutionEvent::ResultReady { facts }) => hooks.on_event(
                py,
                CallEvent::Execution(ExecutionEvent::ResultReady {
                    facts: facts.clone(),
                }),
            ),
            CallEvent::Started { start_time } => hooks.on_event(
                py,
                CallEvent::Started {
                    start_time: *start_time,
                },
            ),
            CallEvent::Execution(ExecutionEvent::ProviderResponseReceived { raw }) => hooks
                .on_event(
                    py,
                    CallEvent::Execution(ExecutionEvent::ProviderResponseReceived { raw }),
                ),
            CallEvent::Succeeded { timing, response } => hooks.on_event(
                py,
                CallEvent::Succeeded {
                    timing: *timing,
                    response,
                },
            ),
            CallEvent::Failed {
                timing,
                origin,
                error,
            } => hooks.on_event(
                py,
                CallEvent::Failed {
                    timing: *timing,
                    origin: *origin,
                    error: &PyErr::from_value(error.bind(py).clone().into_any()),
                },
            ),
            CallEvent::Cancelled { timing } => {
                hooks.on_event(py, CallEvent::Cancelled { timing: *timing })
            }
        }
    }

    fn notification_result(
        py: Python<'_>,
        terminal: bool,
        result: PyResult<ChainStep<Self, ()>>,
    ) -> PyResult<ChainStep<Self, ()>> {
        match result {
            Err(error) if terminal && error.is_instance_of::<PyException>(py) => {
                error.write_unraisable(py, None);
                Ok(ChainStep::Ready(()))
            }
            result => result,
        }
    }
}

impl PythonOwned for HookChain {
    fn close(&mut self, py: Python<'_>) {
        self.clear_pending();
        for hooks in self.groups_mut() {
            hooks.close(py);
        }
    }

    fn traverse(&self, visit: &PyVisit<'_>) -> Result<(), PyTraverseError> {
        for hooks in self.groups() {
            hooks.traverse(visit)?;
        }
        match self.pending_event() {
            Some(CallEvent::Succeeded { response, .. }) => visit.call(response),
            Some(CallEvent::Failed { error, .. }) => visit.call(error),
            _ => Ok(()),
        }
    }
}
