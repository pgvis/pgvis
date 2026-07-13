//! # Prefer header parsing — typed preferences from the HTTP `Prefer` header.
//!
//! PostgREST uses the `Prefer` header extensively to control response behaviour.
//! This module parses the header value into a strongly-typed struct.
//!
//! ## PostgREST equivalent
//!
//! `Preferences` in `ApiRequest/Preferences.hs`.

use serde::{Deserialize, Serialize};

/// Parsed preferences from the `Prefer` HTTP header.
///
/// All fields are `Option` — absent preferences mean "use server default".
/// The `Preference-Applied` response header echoes back only the preferences
/// that were actually honoured.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Preferences {
    /// `Prefer: return=representation|minimal|headers-only|none`
    ///
    /// Controls what the response body contains after a mutation.
    pub return_repr: Option<PreferReturn>,

    /// `Prefer: count=exact|planned|estimated`
    ///
    /// Controls whether/how total count is computed for GET/HEAD.
    pub count: Option<PreferCount>,

    /// `Prefer: resolution=merge-duplicates|ignore-duplicates`
    ///
    /// Controls UPSERT conflict resolution strategy.
    pub resolution: Option<PreferResolution>,

    /// `Prefer: handling=strict|lenient`
    ///
    /// When `strict`, unknown preferences cause a 400 error.
    pub handling: Option<PreferHandling>,

    /// `Prefer: timezone=America/New_York`
    ///
    /// Sets the session timezone for the request. Gated on `dialect.supports_set_timezone`.
    pub timezone: Option<String>,

    /// `Prefer: missing=default|null`
    ///
    /// Controls handling of missing keys in JSON payloads.
    pub missing: Option<PreferMissing>,

    /// `Prefer: tx=commit|rollback`
    ///
    /// Controls transaction end behaviour. Gated on `Config::tx_allow_override`.
    pub tx: Option<PreferTx>,

    /// `Prefer: max-affected=N`
    ///
    /// Limits the number of affected rows. Requires `handling=strict`.
    pub max_affected: Option<u64>,

    /// `Prefer: params=single-object|multiple-objects`
    ///
    /// Controls how RPC body arguments are passed to the function.
    pub params: Option<PreferParams>,
}

/// `Prefer: return` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreferReturn {
    /// Return the full representation of affected rows.
    Representation,
    /// Return no body (204 No Content).
    Minimal,
    /// Return only headers (Location, Content-Range) without body.
    HeadersOnly,
    /// Return nothing (used with batch operations).
    None,
}

/// `Prefer: count` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreferCount {
    /// Exact count via `COUNT(*)` — potentially expensive.
    Exact,
    /// Estimated count via `EXPLAIN` — fast but approximate. Postgres only.
    Planned,
    /// Adaptive: use `EXPLAIN` estimate if high, exact if low. Postgres only.
    Estimated,
}

/// `Prefer: resolution` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreferResolution {
    /// `ON CONFLICT DO UPDATE` — merge incoming data with existing rows.
    MergeDuplicates,
    /// `ON CONFLICT DO NOTHING` — skip rows that conflict.
    IgnoreDuplicates,
}

/// `Prefer: handling` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreferHandling {
    /// Unknown preferences are silently ignored.
    Lenient,
    /// Unknown preferences cause a 400 error with details.
    Strict,
}

/// `Prefer: missing` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreferMissing {
    /// Missing keys use the column's DEFAULT value.
    Default,
    /// Missing keys are treated as NULL.
    Null,
}

/// `Prefer: tx` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreferTx {
    /// Commit the transaction after execution (normal behaviour).
    Commit,
    /// Roll back the transaction (dry-run / testing mode).
    Rollback,
}

/// `Prefer: params` values for RPC.
///
/// Note: `single-object` was removed in PostgREST v13 and is no longer supported.
/// If received, it is treated as an unknown preference (triggers 400 with `handling=strict`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreferParams {
    /// Pass body as multiple named arguments (default).
    MultipleObjects,
}

impl Preferences {
    /// Parse a `Prefer` header value into typed preferences.
    ///
    /// Multiple preferences are comma-separated in a single header,
    /// or may appear in multiple `Prefer` headers (both are valid per RFC 7240).
    ///
    /// Returns the parsed preferences and a list of unrecognised tokens
    /// (for `handling=strict` validation).
    pub fn parse(header_value: &str) -> (Self, Vec<String>) {
        let mut prefs = Self::default();
        let mut unknown = Vec::new();

        for part in header_value.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            // Split on '=' to get key=value pairs
            if let Some((key, value)) = part.split_once('=') {
                let key = key.trim();
                let value = value.trim();
                // Per RFC 7240 §2, the first occurrence of a preference wins and
                // an unrecognised value is ignored (never clears an already-parsed
                // value). Each arm only assigns when the field is still unset, and
                // an invalid value pushes to `unknown` without overwriting.
                match key {
                    "return" => {
                        let parsed = match value {
                            "representation" => Some(PreferReturn::Representation),
                            "minimal" => Some(PreferReturn::Minimal),
                            "headers-only" => Some(PreferReturn::HeadersOnly),
                            "none" => Some(PreferReturn::None),
                            _ => None,
                        };
                        match parsed {
                            Some(v) if prefs.return_repr.is_none() => prefs.return_repr = Some(v),
                            Some(_) => {}
                            None => unknown.push(part.to_string()),
                        }
                    }
                    "count" => {
                        let parsed = match value {
                            "exact" => Some(PreferCount::Exact),
                            "planned" => Some(PreferCount::Planned),
                            "estimated" => Some(PreferCount::Estimated),
                            _ => None,
                        };
                        match parsed {
                            Some(v) if prefs.count.is_none() => prefs.count = Some(v),
                            Some(_) => {}
                            None => unknown.push(part.to_string()),
                        }
                    }
                    "resolution" => {
                        let parsed = match value {
                            "merge-duplicates" => Some(PreferResolution::MergeDuplicates),
                            "ignore-duplicates" => Some(PreferResolution::IgnoreDuplicates),
                            _ => None,
                        };
                        match parsed {
                            Some(v) if prefs.resolution.is_none() => prefs.resolution = Some(v),
                            Some(_) => {}
                            None => unknown.push(part.to_string()),
                        }
                    }
                    "handling" => {
                        let parsed = match value {
                            "strict" => Some(PreferHandling::Strict),
                            "lenient" => Some(PreferHandling::Lenient),
                            _ => None,
                        };
                        match parsed {
                            Some(v) if prefs.handling.is_none() => prefs.handling = Some(v),
                            Some(_) => {}
                            None => unknown.push(part.to_string()),
                        }
                    }
                    "timezone" => {
                        if prefs.timezone.is_none() {
                            prefs.timezone = Some(value.to_string());
                        }
                    }
                    "missing" => {
                        let parsed = match value {
                            "default" => Some(PreferMissing::Default),
                            "null" => Some(PreferMissing::Null),
                            _ => None,
                        };
                        match parsed {
                            Some(v) if prefs.missing.is_none() => prefs.missing = Some(v),
                            Some(_) => {}
                            None => unknown.push(part.to_string()),
                        }
                    }
                    "tx" => {
                        let parsed = match value {
                            "commit" => Some(PreferTx::Commit),
                            "rollback" => Some(PreferTx::Rollback),
                            _ => None,
                        };
                        match parsed {
                            Some(v) if prefs.tx.is_none() => prefs.tx = Some(v),
                            Some(_) => {}
                            None => unknown.push(part.to_string()),
                        }
                    }
                    "max-affected" => match value.parse::<u64>() {
                        Ok(n) if prefs.max_affected.is_none() => prefs.max_affected = Some(n),
                        Ok(_) => {}
                        Err(_) => unknown.push(part.to_string()),
                    },
                    "params" => {
                        let parsed = match value {
                            "multiple-objects" => Some(PreferParams::MultipleObjects),
                            // "single-object" was removed in PostgREST v13
                            _ => None,
                        };
                        match parsed {
                            Some(v) if prefs.params.is_none() => prefs.params = Some(v),
                            Some(_) => {}
                            None => unknown.push(part.to_string()),
                        }
                    }
                    _ => {
                        unknown.push(part.to_string());
                    }
                }
            } else {
                unknown.push(part.to_string());
            }
        }

        (prefs, unknown)
    }

    /// Produce the `Preference-Applied` response header value.
    ///
    /// Contains only the preferences that were parsed and will be honoured.
    pub fn applied_header(&self) -> String {
        let mut parts = Vec::new();

        if let Some(r) = &self.return_repr {
            parts.push(format!(
                "return={}",
                match r {
                    PreferReturn::Representation => "representation",
                    PreferReturn::Minimal => "minimal",
                    PreferReturn::HeadersOnly => "headers-only",
                    PreferReturn::None => "none",
                }
            ));
        }
        if let Some(c) = &self.count {
            parts.push(format!(
                "count={}",
                match c {
                    PreferCount::Exact => "exact",
                    PreferCount::Planned => "planned",
                    PreferCount::Estimated => "estimated",
                }
            ));
        }
        if let Some(r) = &self.resolution {
            parts.push(format!(
                "resolution={}",
                match r {
                    PreferResolution::MergeDuplicates => "merge-duplicates",
                    PreferResolution::IgnoreDuplicates => "ignore-duplicates",
                }
            ));
        }
        if let Some(h) = &self.handling {
            parts.push(format!(
                "handling={}",
                match h {
                    PreferHandling::Lenient => "lenient",
                    PreferHandling::Strict => "strict",
                }
            ));
        }
        if let Some(tz) = &self.timezone {
            parts.push(format!("timezone={tz}"));
        }
        if let Some(m) = &self.missing {
            parts.push(format!(
                "missing={}",
                match m {
                    PreferMissing::Default => "default",
                    PreferMissing::Null => "null",
                }
            ));
        }
        if let Some(t) = &self.tx {
            parts.push(format!(
                "tx={}",
                match t {
                    PreferTx::Commit => "commit",
                    PreferTx::Rollback => "rollback",
                }
            ));
        }
        if let Some(n) = self.max_affected {
            parts.push(format!("max-affected={n}"));
        }
        if let Some(p) = &self.params {
            parts.push(format!(
                "params={}",
                match p {
                    PreferParams::MultipleObjects => "multiple-objects",
                }
            ));
        }

        parts.join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty() {
        let (prefs, unknown) = Preferences::parse("");
        assert_eq!(prefs, Preferences::default());
        assert!(unknown.is_empty());
    }

    #[test]
    fn parse_return_representation() {
        let (prefs, unknown) = Preferences::parse("return=representation");
        assert_eq!(prefs.return_repr, Some(PreferReturn::Representation));
        assert!(unknown.is_empty());
    }

    #[test]
    fn parse_multiple() {
        let (prefs, unknown) = Preferences::parse("return=minimal, count=exact, handling=strict");
        assert_eq!(prefs.return_repr, Some(PreferReturn::Minimal));
        assert_eq!(prefs.count, Some(PreferCount::Exact));
        assert_eq!(prefs.handling, Some(PreferHandling::Strict));
        assert!(unknown.is_empty());
    }

    #[test]
    fn parse_unknown_collected() {
        let (prefs, unknown) = Preferences::parse("return=representation, foo=bar, baz");
        assert_eq!(prefs.return_repr, Some(PreferReturn::Representation));
        assert_eq!(unknown, vec!["foo=bar", "baz"]);
    }

    #[test]
    fn parse_max_affected() {
        let (prefs, _) = Preferences::parse("max-affected=100");
        assert_eq!(prefs.max_affected, Some(100));
    }

    #[test]
    fn parse_tx_rollback() {
        let (prefs, _) = Preferences::parse("tx=rollback");
        assert_eq!(prefs.tx, Some(PreferTx::Rollback));
    }

    #[test]
    fn applied_header_round_trip() {
        let (prefs, _) = Preferences::parse("return=representation, count=exact, timezone=UTC");
        let header = prefs.applied_header();
        assert!(header.contains("return=representation"));
        assert!(header.contains("count=exact"));
        assert!(header.contains("timezone=UTC"));
    }
}
