//! [`TransduceRecords`]: alchemy's `transduce-records` run natively, the
//! parser's rule passes (`ParserEvents/1`) in and a document of records'
//! `JsonEvents/1` out, as a [`RecordsSpec`] says.
//!
//! The library's text (`stdlib/transduce.alc` in tabnas/alchemy) is the
//! specification, and alchemy's `RecordsSpec` documents what a native must
//! do, pass for pass. The state is the phase, the header's names and how
//! many cells of the current record have been read: nothing grows with
//! the document's length, only with a record's width, and that is held to
//! the spec's `max_state_bytes` by the measure the library's scan holds
//! its state to ([`records_state_bytes`]), so the native refuses a header
//! at the same pass the text would. A cell's events are worked out before
//! the first of them is written.
//!
//! The routers offer it ([`crate::routers`]), and the lowering asks for it
//! where a program applies the library's `transduce-records` to the host's
//! parser events with a spec of the standard shape: tabnas-csv's
//! transducer part is one.

use crate::error::{Code, Fail};
use crate::event::{JsonEvent, Number};
use crate::sink::{Flow, Sink};
use crate::{
    records_name, records_state_bytes, records_state_refusal, Datum, ParserEvent, ParserSink,
    RecordsPhase, RecordsSpec, RulePass,
};

/// The library's `transduce-records`, natively, writing to `down`.
pub struct TransduceRecords<S> {
    spec: RecordsSpec,
    phase: RecordsPhase,
    names: Vec<String>,
    /// The cells of the current record read so far.
    cells: usize,
    /// Whether the state has been measured since it was made: the
    /// library measures its initial state before anything is read.
    measured: bool,
    down: S,
}

impl<S: Sink> TransduceRecords<S> {
    /// The native over `spec`, writing to `down`.
    pub fn new(spec: RecordsSpec, down: S) -> TransduceRecords<S> {
        TransduceRecords {
            spec,
            phase: RecordsPhase::Before,
            names: Vec::new(),
            cells: 0,
            measured: false,
            down,
        }
    }

    /// The sink the events were written to.
    pub fn into_inner(self) -> S {
        self.down
    }

    /// The state held to the spec's bound, as the library's scan holds
    /// its own.
    fn held(&self) -> Result<(), Fail> {
        let bytes = records_state_bytes(
            self.phase,
            self.names.iter().map(String::as_str),
            self.cells,
        );
        if bytes > self.spec.max_state_bytes as u64 {
            return Err(records_state_refusal(self.spec.max_state_bytes));
        }
        Ok(())
    }

    fn write(&mut self, events: &[JsonEvent<'_>]) -> Result<Flow, Fail> {
        for ev in events {
            if self.down.event(*ev)? == Flow::Stop {
                return Ok(Flow::Stop);
            }
        }
        Ok(Flow::Continue)
    }

    /// A cell's value: a header's name, a row's member or element, or
    /// nothing outside the document.
    fn cell(&mut self, value: &Datum) -> Result<Flow, Fail> {
        match self.phase {
            RecordsPhase::Header => {
                let name = records_name(value)?;
                self.names.push(name);
                self.held()?;
                Ok(Flow::Continue)
            }
            RecordsPhase::Row => {
                let n = self.cells;
                self.cells += 1;
                self.held()?;
                let scalar = scalar(value);
                if self.spec.header {
                    let key = match self.names.get(n) {
                        Some(name) => name.clone(),
                        None => format!("{}{n}", self.spec.unnamed),
                    };
                    self.write(&[JsonEvent::Key(&key), scalar])
                } else {
                    self.write(&[scalar])
                }
            }
            RecordsPhase::Before | RecordsPhase::Done => Ok(Flow::Continue),
        }
    }

    /// Whether a pass's alternate is in the spec's `:empty` group.
    fn empty(&self, pass: &RulePass) -> bool {
        match (&self.spec.empty, &pass.alt) {
            (Some(group), Some(alt)) => alt.g.iter().any(|g| g == group),
            _ => false,
        }
    }

    fn open(&mut self, pass: &RulePass) -> Result<Flow, Fail> {
        if pass.name == self.spec.document {
            self.phase = if self.spec.header {
                RecordsPhase::Header
            } else {
                RecordsPhase::Row
            };
            self.names.clear();
            self.cells = 0;
            self.held()?;
            return self.write(&[JsonEvent::ArrayStart]);
        }
        if pass.name == self.spec.record {
            if self.phase != RecordsPhase::Row {
                return Ok(Flow::Continue);
            }
            self.cells = 0;
            let start = if self.spec.header {
                JsonEvent::ObjectStart
            } else {
                JsonEvent::ArrayStart
            };
            return self.write(&[start]);
        }
        if self.empty(pass) {
            let fill = self.spec.fill.clone();
            return self.cell(&fill);
        }
        Ok(Flow::Continue)
    }

    fn close(&mut self, pass: &RulePass) -> Result<Flow, Fail> {
        if pass.name == self.spec.cell {
            return match &pass.value {
                Some(value) => self.cell(value),
                None => Ok(Flow::Continue),
            };
        }
        if pass.name == self.spec.record {
            return match self.phase {
                RecordsPhase::Header => {
                    self.phase = RecordsPhase::Row;
                    self.cells = 0;
                    self.held()?;
                    Ok(Flow::Continue)
                }
                RecordsPhase::Row if self.spec.header => {
                    let missing: Vec<String> =
                        self.names.iter().skip(self.cells).cloned().collect();
                    self.cells = 0;
                    let fill = self.spec.fill.clone();
                    let fill = scalar(&fill);
                    for name in &missing {
                        if self.write(&[JsonEvent::Key(name), fill])? == Flow::Stop {
                            return Ok(Flow::Stop);
                        }
                    }
                    self.write(&[JsonEvent::ObjectEnd])
                }
                RecordsPhase::Row => {
                    self.cells = 0;
                    self.write(&[JsonEvent::ArrayEnd])
                }
                RecordsPhase::Before | RecordsPhase::Done => Ok(Flow::Continue),
            };
        }
        if pass.name == self.spec.document {
            self.phase = RecordsPhase::Done;
            self.names.clear();
            self.cells = 0;
            return self.write(&[JsonEvent::ArrayEnd]);
        }
        if self.empty(pass) {
            let fill = self.spec.fill.clone();
            return self.cell(&fill);
        }
        Ok(Flow::Continue)
    }
}

/// A cell's scalar event: the pass carries a scalar, and the spec's fill
/// is one.
fn scalar(value: &Datum) -> JsonEvent<'_> {
    match value {
        Datum::Null => JsonEvent::Null,
        Datum::Bool(b) => JsonEvent::Bool(*b),
        Datum::Number { value, lexeme } => JsonEvent::Number(Number {
            value: *value,
            lexeme: lexeme.as_deref(),
        }),
        Datum::String(s) => JsonEvent::String(s),
        // A pass's value is a scalar and the lowering hands over a scalar
        // fill, so a container never arrives; were one to, it is null, as
        // nothing here could write it.
        Datum::Array(_) | Datum::Object(_) => JsonEvent::Null,
    }
}

impl<S: Sink> ParserSink for TransduceRecords<S> {
    fn event(&mut self, ev: &ParserEvent) -> Result<Flow, Fail> {
        if !self.measured {
            self.measured = true;
            self.held()?;
        }
        match ev {
            ParserEvent::Open(pass) => self.open(pass),
            ParserEvent::Close(pass) => self.close(pass),
            ParserEvent::End => {
                match self.phase {
                    RecordsPhase::Before => {
                        if self.write(&[JsonEvent::ArrayStart, JsonEvent::ArrayEnd])?
                            == Flow::Stop
                        {
                            return Ok(Flow::Stop);
                        }
                    }
                    RecordsPhase::Done => {}
                    RecordsPhase::Header | RecordsPhase::Row => {
                        return Err(Fail::new(
                            Code::ProtocolOrderError,
                            "the rule passes ended inside the document, which a records grammar's never do",
                        ))
                    }
                }
                self.down.event(JsonEvent::End)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{OwnedJsonEvent as Ev, PassAlt};

    fn pass(name: &str, alt: Option<PassAlt>, value: Option<Datum>) -> RulePass {
        RulePass {
            name: name.into(),
            d: 0,
            i: 0,
            alt,
            tokens: vec![],
            forced: false,
            n: vec![],
            value,
        }
    }

    fn empty(groups: &[&str]) -> Option<PassAlt> {
        Some(PassAlt {
            p: "".into(),
            r: "".into(),
            b: 1,
            g: groups.iter().map(|g| Box::from(*g)).collect(),
        })
    }

    /// The passes the csv grammar makes for `records`: a `None` is an
    /// empty cell before a separator, and `at_end` an empty cell at the
    /// record's end.
    fn passes(records: &[&[Option<&str>]], at_end: &[bool]) -> Vec<ParserEvent> {
        let mut out = vec![ParserEvent::Open(pass("csv", None, None))];
        for (k, record) in records.iter().enumerate() {
            out.push(ParserEvent::Open(pass("record", None, None)));
            for cell in record.iter() {
                out.push(match cell {
                    Some(text) => {
                        ParserEvent::Close(pass("val", None, Some(Datum::String((*text).into()))))
                    }
                    None => ParserEvent::Open(pass("elem", empty(&["empty"]), None)),
                });
            }
            if at_end.get(k) == Some(&true) {
                out.push(ParserEvent::Close(pass(
                    "elem",
                    empty(&["comma", "empty"]),
                    None,
                )));
            }
            out.push(ParserEvent::Close(pass("record", None, None)));
        }
        out.push(ParserEvent::Close(pass("csv", None, None)));
        out.push(ParserEvent::End);
        out
    }

    fn csv_spec() -> RecordsSpec {
        RecordsSpec::new("csv", "record", "val")
            .with_empty("empty")
            .with_header("field~")
    }

    fn run(spec: RecordsSpec, events: &[ParserEvent]) -> (Result<Flow, Fail>, Vec<Ev>) {
        let mut native = TransduceRecords::new(spec, Vec::<Ev>::new());
        let mut outcome = Ok(Flow::Continue);
        for ev in events {
            outcome = native.event(ev);
            if !matches!(outcome, Ok(Flow::Continue)) {
                break;
            }
        }
        (outcome, native.into_inner())
    }

    fn s(text: &str) -> Ev {
        Ev::String(text.into())
    }

    fn k(name: &str) -> Ev {
        Ev::Key(name.into())
    }

    const RECORDS: &[&[Option<&str>]] = &[
        &[Some("a"), Some("b")],
        &[Some("1"), Some("2")],
        &[Some("3")],
        &[None, Some("4")],
        &[Some("5")],
        &[Some("6"), Some("7"), Some("8")],
    ];
    const AT_END: &[bool] = &[false, false, true, false, false, false];

    /// The library's events, as alchemy's lowering test holds its text
    /// to over the same passes.
    #[test]
    fn a_header_names_the_rows_cells() {
        let (r, events) = run(csv_spec(), &passes(RECORDS, AT_END));
        assert_eq!(r.unwrap(), Flow::Continue);
        use Ev::*;
        assert_eq!(
            events,
            vec![
                ArrayStart,
                ObjectStart,
                k("a"),
                s("1"),
                k("b"),
                s("2"),
                ObjectEnd,
                ObjectStart,
                k("a"),
                s("3"),
                k("b"),
                s(""),
                ObjectEnd,
                ObjectStart,
                k("a"),
                s(""),
                k("b"),
                s("4"),
                ObjectEnd,
                ObjectStart,
                k("a"),
                s("5"),
                k("b"),
                s(""),
                ObjectEnd,
                ObjectStart,
                k("a"),
                s("6"),
                k("b"),
                s("7"),
                k("field~2"),
                s("8"),
                ObjectEnd,
                ArrayEnd,
                End,
            ]
        );
    }

    #[test]
    fn without_a_header_each_record_is_an_array() {
        let spec = RecordsSpec::new("csv", "record", "val")
            .with_empty("empty")
            .with_fill(Datum::Null);
        let (r, events) = run(spec, &passes(RECORDS, AT_END));
        assert_eq!(r.unwrap(), Flow::Continue);
        use Ev::*;
        assert_eq!(
            events,
            vec![
                ArrayStart,
                ArrayStart,
                s("a"),
                s("b"),
                ArrayEnd,
                ArrayStart,
                s("1"),
                s("2"),
                ArrayEnd,
                ArrayStart,
                s("3"),
                Null,
                ArrayEnd,
                ArrayStart,
                Null,
                s("4"),
                ArrayEnd,
                ArrayStart,
                s("5"),
                ArrayEnd,
                ArrayStart,
                s("6"),
                s("7"),
                s("8"),
                ArrayEnd,
                ArrayEnd,
                End,
            ]
        );
    }

    #[test]
    fn an_empty_document_is_the_empty_array_and_one_cut_short_is_refused() {
        let (r, events) = run(csv_spec(), &[ParserEvent::End]);
        assert_eq!(r.unwrap(), Flow::Continue);
        assert_eq!(events, vec![Ev::ArrayStart, Ev::ArrayEnd, Ev::End]);
        let mut cut = passes(&[&[Some("a")]], &[false]);
        cut.truncate(2);
        cut.push(ParserEvent::End);
        let (r, _) = run(csv_spec(), &cut);
        let fail = r.unwrap_err();
        assert_eq!(fail.code, Code::ProtocolOrderError);
        assert_eq!(
            fail.message,
            "the rule passes ended inside the document, which a records grammar's never do"
        );
    }

    /// The state is held to the spec's bound by the scan's own measure:
    /// `[:header ["a" "b"] []]` is 6 nodes, 6 bytes of `header` and 2 of
    /// the names, 104 bytes, so a bound of 103 refuses at the second name
    /// and 104 lets the header through.
    #[test]
    fn the_state_is_held_as_the_librarys_scan_holds_it() {
        let header = passes(&[&[Some("a"), Some("b")]], &[false]);
        let (r, _) = run(csv_spec().with_max_state_bytes(104), &header);
        assert_eq!(r.unwrap(), Flow::Continue);
        let (r, events) = run(csv_spec().with_max_state_bytes(103), &header);
        let fail = r.unwrap_err();
        assert_eq!(fail.code, Code::ResourceLimitExceeded);
        assert_eq!(fail.limit.unwrap().name, "max_metadata_bytes");
        assert_eq!(
            fail.message,
            "the scan-emit state holds more than 103 bytes"
        );
        assert_eq!(events, vec![Ev::ArrayStart]);
        // The initial state, [:before [] []], is 70 bytes, measured before
        // the first pass.
        let (r, events) = run(csv_spec().with_max_state_bytes(69), &header);
        assert_eq!(r.unwrap_err().limit.unwrap().name, "max_metadata_bytes");
        assert!(events.is_empty());
    }

    #[test]
    fn a_stop_from_the_sink_stops_the_passes() {
        let mut seen = 0;
        let sink = crate::FnSink(move |_ev: JsonEvent<'_>| {
            seen += 1;
            Ok(if seen == 3 {
                Flow::Stop
            } else {
                Flow::Continue
            })
        });
        let mut native = TransduceRecords::new(csv_spec(), sink);
        let mut flows = Vec::new();
        for ev in passes(RECORDS, AT_END) {
            let flow = native.event(&ev).unwrap();
            flows.push(flow);
            if flow == Flow::Stop {
                break;
            }
        }
        assert_eq!(flows.last(), Some(&Flow::Stop));
    }
}
