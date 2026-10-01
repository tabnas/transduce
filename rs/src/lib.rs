//! Streaming transducers over any tabnas parser.
//!
//! A transducer decides what a source contributes to an output model; a
//! renderer (the `tabnas-render` crate) decides how that model becomes
//! text. This crate is the transducer side:
//!
//! - the source protocol [`JsonEvent`] (`JsonEvents/1`) and the push
//!   boundary [`Sink`];
//! - [`source`]s that produce it from a parsed value, from a live tabnas
//!   parse through the engine's rule events, or from line-delimited input;
//! - [`Selector`]s and the matcher and router that recognize selected
//!   scopes in one pass and materialize them under a byte limit;
//! - the table protocol [`TableEvent`] (`TableRows/1`) and the standard
//!   metadata-first table transducer;
//! - [`Limits`], [`Metrics`], [`AbortFlag`] and the stable failure
//!   [`Code`]s every stage reports.
//!
//! Everything is synchronous and push-based: the parser calls the first
//! sink from inside its own callback, and a slow writer at the end of the
//! chain slows the parse at the start. That is the backpressure.

#![forbid(unsafe_code)]

/// The README's Rust example runs as a doctest, so a stale one fails the
/// gate rather than misleading the reader.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
mod readme_examples {}

pub mod datum;
pub mod error;
pub mod event;
pub mod limits;
pub mod matcher;
pub mod route;
pub mod scan;
pub mod selector;
pub mod sink;
pub mod source;
pub mod table;
pub mod table_from_json;

pub use datum::{
    walk_datum, write_json, write_json_number, write_json_string, Datum, DatumBuilder, Duplicates,
};
pub use error::{Code, Fail, Limit};
pub use event::{JsonEvent, Number, OwnedJsonEvent};
pub use limits::{AbortFlag, Limits, Metrics};
pub use matcher::{CaptureId, Hit, HitKind, Matcher};
pub use route::{Budget, CaptureMode, CaptureSpec, FnRoute, RouteSink, Router, Selected};
pub use scan::{ScanEmit, Transition};
pub use selector::{Path, Segment, Selector, Step};
pub use sink::{replay, CountSink, Flow, FnSink, Sink, TreeContract};
pub use source::{
    capability, walk_value, Guarded, ParserSource, Prune, Source, SourceMode, ValueSource,
};
pub use table::{
    column_from_meta, BoundColumn, Cell, ColumnMapper, MissingPolicy, PublicColumn, Schema, Table,
    TableBinding, TableEvent, TableSink,
};
pub use table_from_json::TableFromJson;

/// This crate's version, as `Cargo.toml` declares it.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
