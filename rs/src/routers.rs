//! This crate's implementation of alchemy's `Routers`: the interface a
//! compiled alchemy program makes its routing stages through.
//!
//! alchemy's runtime lowers a plan onto a [`Router`] for a program's
//! captures, the [`ScanEmit`] operator and the source's [`Guarded`]
//! limits, and constructs none of them: the host passes [`routers()`] to
//! `tabnas_alchemy::compile`, and each stage is made through it, exactly
//! as the constructor named would make it. The standard library's
//! `table-from-json`, `csv`, `transduce-tree` and `transduce-records` are
//! alchemy's own text, run by its interpreter over these stages in every
//! runtime; this crate offers nothing in their place.

use std::sync::Arc;

use tabnas_alchemy::shared::{
    AbortFlag, CaptureId, CaptureSpec, Duplicates, Fail, Flow, Limits, Metrics, RouteSink, Routers,
    ScanEmitter, ScanFinish, ScanOut, ScanStep, Selected, Sink, Transition,
};

use crate::route::Router;
use crate::scan::ScanEmit;
use crate::source::Guarded;

/// The routers this crate implements, for alchemy's `Routers`: every
/// method is the constructor of the same name.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TransduceRouters;

/// This crate's routers, to pass to `tabnas_alchemy::compile`.
pub fn routers() -> TransduceRouters {
    TransduceRouters
}

impl<V: Send + 'static> Routers<V> for TransduceRouters {
    fn router(
        &self,
        specs: Vec<CaptureSpec>,
        limits: &Limits,
        duplicates: Duplicates,
        metrics: Arc<Metrics>,
        downstream: Box<dyn RouteSink + Send>,
    ) -> Result<Box<dyn Sink + Send>, Fail> {
        Ok(Box::new(Router::new(
            specs,
            limits,
            duplicates,
            metrics,
            Downstream(downstream),
        )?))
    }

    fn scan_emit(
        &self,
        initial: V,
        step: ScanStep<V>,
        finish: ScanFinish<V>,
        out: ScanOut<V>,
    ) -> Box<dyn ScanEmitter<V>> {
        Box::new(ScanEmit::new(initial, step, finish, out))
    }

    fn guarded(
        &self,
        inner: Box<dyn Sink + Send>,
        limits: &Limits,
        abort: AbortFlag,
        metrics: Arc<Metrics>,
    ) -> Box<dyn Sink + Send> {
        Box::new(Guarded::new(inner, limits, abort, metrics))
    }
}

/// The operator, driven through alchemy's `ScanEmitter`.
impl<S, I, O, Step, Finish, Out> ScanEmitter<I> for ScanEmit<S, I, O, Step, Finish, Out>
where
    S: Send,
    I: Send,
    Step: FnMut(S, I) -> Result<Transition<S, O>, Fail> + Send,
    Finish: FnOnce(S) -> Result<Vec<O>, Fail> + Send,
    Out: FnMut(O) -> Result<Flow, Fail> + Send,
{
    fn item(&mut self, item: I) -> Result<Flow, Fail> {
        ScanEmit::item(self, item)
    }

    fn finish(&mut self) -> Result<Flow, Fail> {
        ScanEmit::finish(self)
    }
}

/// A router's downstream as the caller boxed it: each call passed on.
struct Downstream(Box<dyn RouteSink + Send>);

impl RouteSink for Downstream {
    fn began(&mut self, id: CaptureId, tag: &str) -> Result<(), Fail> {
        self.0.began(id, tag)
    }

    fn selected(&mut self, selected: Selected) -> Result<Flow, Fail> {
        self.0.selected(selected)
    }

    fn end(&mut self) -> Result<Flow, Fail> {
        self.0.end()
    }
}
