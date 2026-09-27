//! `scan-emit`: declarative state evolution over a stream.
//!
//! A pure step function takes the state and one item and returns the next
//! state with the items to emit; a finish function turns the final state
//! into the closing items. The operator owns the state and the iteration;
//! the functions own nothing. This is what the DSL's `scan-emit` lowers to,
//! and the table transducer is one instance of it (implemented natively for
//! speed, with the same contract).

use crate::error::Fail;
use crate::sink::Flow;

/// The result of one step: the next state and what to emit for it.
#[derive(Clone, Debug, PartialEq)]
pub struct Transition<S, O> {
    pub state: S,
    pub outputs: Vec<O>,
}

impl<S, O> Transition<S, O> {
    pub fn new(state: S, outputs: Vec<O>) -> Self {
        Transition { state, outputs }
    }

    /// A step that emits nothing.
    pub fn stay(state: S) -> Self {
        Transition {
            state,
            outputs: Vec::new(),
        }
    }

    /// A step that emits one item.
    pub fn emit(state: S, output: O) -> Self {
        Transition {
            state,
            outputs: vec![output],
        }
    }
}

/// The operator. Feed items with [`ScanEmit::item`], then call
/// [`ScanEmit::finish`] exactly once when the input completed
/// successfully; never call it after a failure or a cancellation.
pub struct ScanEmit<S, I, O, Step, Finish, Out>
where
    Step: FnMut(S, I) -> Result<Transition<S, O>, Fail>,
    Finish: FnOnce(S) -> Result<Vec<O>, Fail>,
    Out: FnMut(O) -> Result<Flow, Fail>,
{
    state: Option<S>,
    step: Step,
    finish: Option<Finish>,
    out: Out,
    _item: std::marker::PhantomData<I>,
}

impl<S, I, O, Step, Finish, Out> ScanEmit<S, I, O, Step, Finish, Out>
where
    Step: FnMut(S, I) -> Result<Transition<S, O>, Fail>,
    Finish: FnOnce(S) -> Result<Vec<O>, Fail>,
    Out: FnMut(O) -> Result<Flow, Fail>,
{
    pub fn new(initial: S, step: Step, finish: Finish, out: Out) -> Self {
        ScanEmit {
            state: Some(initial),
            step,
            finish: Some(finish),
            out,
            _item: std::marker::PhantomData,
        }
    }

    /// One input item. Its outputs go downstream before this returns.
    pub fn item(&mut self, item: I) -> Result<Flow, Fail> {
        let state = self
            .state
            .take()
            .ok_or_else(|| Fail::protocol("scan-emit received an item after it finished"))?;
        let transition = (self.step)(state, item)?;
        self.state = Some(transition.state);
        for o in transition.outputs {
            if (self.out)(o)? == Flow::Stop {
                return Ok(Flow::Stop);
            }
        }
        Ok(Flow::Continue)
    }

    /// The input completed: emit the closing items.
    pub fn finish(&mut self) -> Result<Flow, Fail> {
        let state = self
            .state
            .take()
            .ok_or_else(|| Fail::protocol("scan-emit finished twice"))?;
        let finish = self
            .finish
            .take()
            .ok_or_else(|| Fail::protocol("scan-emit finished twice"))?;
        for o in finish(state)? {
            if (self.out)(o)? == Flow::Stop {
                return Ok(Flow::Stop);
            }
        }
        Ok(Flow::Continue)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn running_sum_with_a_total_at_the_end() {
        let mut seen = Vec::new();
        {
            let mut scan = ScanEmit::new(
                0i64,
                |sum: i64, x: i64| Ok(Transition::emit(sum + x, format!("+{x}"))),
                |sum: i64| Ok(vec![format!("={sum}")]),
                |s: String| {
                    seen.push(s);
                    Ok(Flow::Continue)
                },
            );
            scan.item(1).unwrap();
            scan.item(2).unwrap();
            scan.finish().unwrap();
            assert_eq!(
                scan.finish().unwrap_err().code,
                crate::Code::ProtocolOrderError
            );
        }
        assert_eq!(seen, ["+1", "+2", "=3"]);
    }

    #[test]
    fn stop_propagates() {
        let mut scan = ScanEmit::new(
            (),
            |_: (), x: u8| Ok(Transition::emit((), x)),
            |_: ()| Ok(vec![]),
            |x: u8| Ok(if x == 2 { Flow::Stop } else { Flow::Continue }),
        );
        assert_eq!(scan.item(1).unwrap(), Flow::Continue);
        assert_eq!(scan.item(2).unwrap(), Flow::Stop);
    }
}
