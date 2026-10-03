//! # Shared winnow parsers — field names, JSON paths, identifiers, operators.
//!
//! These mirror PostgREST's parsing primitives in `QueryParams.hs`:
//! - `pFieldName` → [`field_name`]
//! - `pIdentifier` → [`identifier`]
//! - `pJsonPath` → [`json_path`]
//! - `pQuotedValue` → [`quoted_value`]
//! - `aliasSeparator` → [`alias_sep`]
//! - operator keyword parsers shared by [`super::filter`] and [`super::logic`]
//!
//! All parsers operate on `&mut &str` and return `winnow::Result<T>`.

use winnow::ascii::digit1;
use winnow::combinator::{alt, delimited, not, opt, peek, preceded, repeat, separated};
use winnow::error::ContextError;
use winnow::token::{any, one_of, take_while};
use winnow::{Parser, Result};

use super::types::{Operator, Quantifier};
use crate::select_ast::{JsonOperand, JsonOperation};

/// Deepest `(` … `)` nesting a `select=` or `and=`/`or=` value may use.
///
/// Both parsers recurse once per level; unbounded input overflows the stack,
/// which aborts the whole process rather than failing one request.
pub const MAX_NESTING: usize = 32;

/// Reject input nested deeper than [`MAX_NESTING`], before any recursion.
/// Counts every parenthesis (quoted ones included), which only errs towards
/// rejecting absurd input.
pub fn check_nesting(input: &str) -> Result<(), String> {
    let (mut depth, mut max) = (0usize, 0usize);
    for b in input.bytes() {
        match b {
            b'(' => {
                depth += 1;
                max = max.max(depth);
            }
            b')' => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    if max > MAX_NESTING {
        return Err(format!("nesting deeper than {MAX_NESTING} levels"));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Field name
// ---------------------------------------------------------------------------

/// Parse a field name: quoted `"…"` *or* one-or-more identifiers joined by `-`
/// (but never `->` — that starts a JSON path).
pub fn field_name(input: &mut &str) -> Result<String> {
    alt((quoted_value, sep_by_dash)).parse_next(input)
}

/// Parse a quoted value: `"…"` with `\` escaping.
pub fn quoted_value(input: &mut &str) -> Result<String> {
    delimited('"', repeat(0.., quoted_char), '"').parse_next(input)
}

fn quoted_char(input: &mut &str) -> Result<char> {
    alt((preceded('\\', any), one_of(|c: char| c != '\\' && c != '"'))).parse_next(input)
}

/// Parse an identifier: letters / digits / `_` / `$` / spaces (trimmed).
pub fn identifier(input: &mut &str) -> Result<String> {
    take_while(1.., |c: char| {
        c.is_ascii_alphanumeric() || c == '_' || c == '$' || c == ' '
    })
    .map(|s: &str| s.trim().to_string())
    .parse_next(input)
}

/// One-or-more identifiers joined by `-` (never consumes `->`).
fn sep_by_dash(input: &mut &str) -> Result<String> {
    separated(1.., identifier, dash)
        .map(|parts: Vec<String>| parts.join("-"))
        .parse_next(input)
}

/// A `-` *not* followed by `>` (so it does not eat `->` / `->>`).
fn dash(input: &mut &str) -> Result<()> {
    ('-', peek(not('>'))).void().parse_next(input)
}

// ---------------------------------------------------------------------------
// Alias separator
// ---------------------------------------------------------------------------

/// `:` not followed by `:` (so it doesn't grab `::cast`).
pub fn alias_sep(input: &mut &str) -> Result<()> {
    (':', peek(not(':'))).void().parse_next(input)
}

// ---------------------------------------------------------------------------
// Delimiter
// ---------------------------------------------------------------------------

/// The `.` delimiter between operator parts.
pub fn dot(input: &mut &str) -> Result<()> {
    '.'.void().parse_next(input)
}

// ---------------------------------------------------------------------------
// JSON path
// ---------------------------------------------------------------------------

/// Zero or more JSON operations (`->key`, `->>key`, `->0`, `->>0`).
pub fn json_path(input: &mut &str) -> Result<Vec<JsonOperation>> {
    repeat(0.., json_operation).parse_next(input)
}

fn json_operation(input: &mut &str) -> Result<JsonOperation> {
    let double = alt(("->>".value(true), "->".value(false))).parse_next(input)?;
    let operand = json_operand.parse_next(input)?;
    Ok(if double {
        JsonOperation::DoubleArrow(operand)
    } else {
        JsonOperation::Arrow(operand)
    })
}

fn json_operand(input: &mut &str) -> Result<JsonOperand> {
    alt((json_index, json_key.map(JsonOperand::Key))).parse_next(input)
}

fn json_index(input: &mut &str) -> Result<JsonOperand> {
    let neg = opt('-').parse_next(input)?.is_some();
    let digits: &str = digit1.parse_next(input)?;
    // Request data: an index past i64 must be a parse error, not a panic.
    let n: i64 = digits.parse().map_err(|_| ContextError::new())?;
    Ok(JsonOperand::Index(if neg { -n } else { n }))
}

/// A JSON key: quoted, or identifiers joined by `-` (never the reserved
/// characters `( ) - : . , >`).
fn json_key(input: &mut &str) -> Result<String> {
    alt((quoted_value, sep_by_dash_json)).parse_next(input)
}

fn sep_by_dash_json(input: &mut &str) -> Result<String> {
    separated(1.., json_key_part, dash)
        .map(|parts: Vec<String>| parts.join("-"))
        .parse_next(input)
}

fn json_key_part(input: &mut &str) -> Result<String> {
    take_while(1.., |c: char| {
        !matches!(c, '(' | ')' | '-' | ':' | '.' | ',' | '>')
    })
    .map(|s: &str| s.trim().to_string())
    .parse_next(input)
}

// ---------------------------------------------------------------------------
// Field = name + optional JSON path
// ---------------------------------------------------------------------------

pub fn field(input: &mut &str) -> Result<(String, Vec<JsonOperation>)> {
    (field_name, json_path).parse_next(input)
}

// ===========================================================================
// Operator-keyword parsers (shared by filter + logic)
// ===========================================================================

/// Quantifiable operators: eq, gt, gte, lt, lte, neq, like, ilike, match, imatch.
///
/// Longer prefixes are listed first so `gte` is tried before `gt`.
pub fn quant_operator(input: &mut &str) -> Result<Operator> {
    alt((
        "gte".value(Operator::Gte),
        "gt".value(Operator::Gt),
        "lte".value(Operator::Lte),
        "lt".value(Operator::Lt),
        "neq".value(Operator::Neq),
        "eq".value(Operator::Eq),
        "ilike".value(Operator::ILike),
        "like".value(Operator::Like),
        "imatch".value(Operator::IMatch),
        "match".value(Operator::Match),
    ))
    .parse_next(input)
}

/// Non-quantifiable operators: cs, cd, ov, sl, sr, nxr, nxl, adj.
pub fn simple_operator(input: &mut &str) -> Result<Operator> {
    alt((
        "cs".value(Operator::Contains),
        "cd".value(Operator::ContainedBy),
        "ov".value(Operator::Overlap),
        "sl".value(Operator::StrictlyLeft),
        "sr".value(Operator::StrictlyRight),
        "nxr".value(Operator::NotExtendsRight),
        "nxl".value(Operator::NotExtendsLeft),
        "adj".value(Operator::Adjacent),
    ))
    .parse_next(input)
}

/// FTS keyword → constructor that takes the optional language config.
pub fn fts_op(input: &mut &str) -> Result<fn(Option<String>) -> Operator> {
    alt((
        "plfts".value(Operator::PlainFts as fn(Option<String>) -> Operator),
        "phfts".value(Operator::PhraseFts as fn(Option<String>) -> Operator),
        "wfts".value(Operator::WebFts as fn(Option<String>) -> Operator),
        "fts".value(Operator::Fts as fn(Option<String>) -> Operator),
    ))
    .parse_next(input)
}

/// Optional `(language)` after an FTS keyword.
pub fn fts_lang(input: &mut &str) -> Result<String> {
    delimited('(', identifier, ')').parse_next(input)
}

/// `(any)` or `(all)`.
pub fn quantifier(input: &mut &str) -> Result<Quantifier> {
    delimited(
        '(',
        alt(("any".value(Quantifier::Any), "all".value(Quantifier::All))),
        ')',
    )
    .parse_next(input)
}

// ===========================================================================
// Top-level scanners (paren/quote-aware string splitting)
// ===========================================================================

/// Split a string at commas that are not inside parens or quoted strings.
///
/// Single source of truth — used by select-list, IN-list values, logic items.
/// Handles backslash escapes inside quotes. Returns borrowed slices; callers
/// `.trim()` or `.to_string()` only if they need to.
pub fn split_top_level(input: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth: i32 = 0;
    let mut in_quotes = false;
    let mut escape_next = false;
    let mut start = 0usize;
    for (i, ch) in input.char_indices() {
        if escape_next {
            escape_next = false;
            continue;
        }
        if in_quotes {
            match ch {
                '\\' => escape_next = true,
                '"' => in_quotes = false,
                _ => {}
            }
            continue;
        }
        match ch {
            '"' => in_quotes = true,
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(&input[start..i]);
                start = i + ch.len_utf8();
            }
            _ => {}
        }
    }
    if start < input.len() {
        parts.push(&input[start..]);
    }
    parts
}
