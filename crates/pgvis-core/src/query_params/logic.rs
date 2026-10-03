//! # Parser for the `and=`/`or=` logic tree query parameters.
//!
//! Parses PostgREST's boolean logic expressions:
//! `or=(id.eq.1,name.eq.John)`, `and=(or(a.eq.1,b.eq.2),c.eq.3)`.
//!
//! Tree recursion is plain function calls — leaf filters reuse
//! [`super::filter::parse_logic_filter`] so the operator grammar isn't duplicated.

use super::common::{check_nesting, split_top_level};
use super::filter::parse_logic_filter;
use super::types::{LogicNode, LogicTree};

/// Parse a logic tree from a query parameter.
///
/// - `op`    = the parameter name, one of `and`, `or`, `not.and`, `not.or`
/// - `value` = the parenthesised body, e.g. `(id.eq.1,name.eq.John)`
pub fn parse_logic_tree(op: &str, value: &str) -> Result<LogicNode, String> {
    let (negate, logic_op) = match op.strip_prefix("not.") {
        Some(rest) => (true, rest),
        None => (false, op),
    };
    check_nesting(value)?;

    let inner = value
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .ok_or_else(|| format!("logic expression must be wrapped in parentheses: {value}"))?;

    let nodes = parse_logic_items(inner)?;
    let tree = match logic_op {
        "and" => LogicTree::And(nodes),
        "or" => LogicTree::Or(nodes),
        _ => return Err(format!("unknown logic operator: {logic_op}")),
    };

    let node = LogicNode::Tree(tree);
    Ok(if negate {
        LogicNode::Not(Box::new(node))
    } else {
        node
    })
}

/// Split a logic body at top-level commas, then parse each item.
fn parse_logic_items(input: &str) -> Result<Vec<LogicNode>, String> {
    let parts: Vec<&str> = split_top_level(input)
        .into_iter()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if parts.is_empty() {
        return Err("logic expression cannot be empty".into());
    }
    parts.into_iter().map(parse_logic_item).collect()
}

/// Parse one logic item: either a nested `[not.](and|or)(…)` tree or a leaf filter.
fn parse_logic_item(item: &str) -> Result<LogicNode, String> {
    let (negate, rest) = match item.strip_prefix("not.") {
        Some(r) => (true, r),
        None => (false, item),
    };

    let nested = nested_tree(rest)?;
    if let Some(tree) = nested {
        let node = LogicNode::Tree(tree);
        return Ok(if negate {
            LogicNode::Not(Box::new(node))
        } else {
            node
        });
    }

    // Leaf filter.
    let mut cur = item; // keep original `not.` because parse_logic_filter handles it
    let filter = parse_logic_filter(&mut cur)
        .map_err(|e| format!("failed to parse filter '{item}': {e}"))?;
    if !cur.is_empty() {
        return Err(format!(
            "unexpected trailing content in filter '{item}': '{cur}'"
        ));
    }
    Ok(LogicNode::Filter(filter))
}

/// If `s` is `and(…)` or `or(…)`, parse it. Otherwise return `Ok(None)`.
fn nested_tree(s: &str) -> Result<Option<LogicTree>, String> {
    for (prefix, ctor) in [
        ("and(", LogicTree::And as fn(Vec<LogicNode>) -> LogicTree),
        ("or(", LogicTree::Or as fn(Vec<LogicNode>) -> LogicTree),
    ] {
        if let Some(inner) = s.strip_prefix(prefix).and_then(|s| s.strip_suffix(')')) {
            let nodes = parse_logic_items(inner)?;
            return Ok(Some(ctor(nodes)));
        }
    }
    Ok(None)
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_params::types::{FilterValue, Operator};

    #[test]
    fn deep_nesting_is_rejected_not_a_stack_overflow() {
        let deep = format!("({}id.eq.1{})", "and(".repeat(100_000), ")".repeat(100_000));
        assert!(parse_logic_tree("and", &deep).is_err());
        let ok = format!("({}id.eq.1{})", "and(".repeat(8), ")".repeat(8));
        assert!(parse_logic_tree("and", &ok).is_ok());
    }

    #[test]
    fn parse_simple_or() {
        let node = parse_logic_tree("or", "(id.eq.1,name.eq.John)").unwrap();
        if let LogicNode::Tree(LogicTree::Or(items)) = node {
            assert_eq!(items.len(), 2);
            if let LogicNode::Filter(f) = &items[0] {
                assert_eq!(f.field, "id");
                assert_eq!(f.operator, Operator::Eq);
                assert_eq!(f.value, FilterValue::Single("1".into()));
            } else {
                panic!("expected filter, got {:?}", items[0]);
            }
            if let LogicNode::Filter(f) = &items[1] {
                assert_eq!(f.field, "name");
                assert_eq!(f.operator, Operator::Eq);
                assert_eq!(f.value, FilterValue::Single("John".into()));
            } else {
                panic!("expected filter");
            }
        } else {
            panic!("expected Or tree, got {node:?}");
        }
    }

    #[test]
    fn parse_simple_and() {
        let node = parse_logic_tree("and", "(a.gte.10,b.lte.20)").unwrap();
        if let LogicNode::Tree(LogicTree::And(items)) = node {
            assert_eq!(items.len(), 2);
        } else {
            panic!("expected And tree");
        }
    }

    #[test]
    fn parse_nested() {
        let node = parse_logic_tree("and", "(or(a.eq.1,b.eq.2),c.eq.3)").unwrap();
        if let LogicNode::Tree(LogicTree::And(items)) = node {
            assert_eq!(items.len(), 2);
            if let LogicNode::Tree(LogicTree::Or(or_items)) = &items[0] {
                assert_eq!(or_items.len(), 2);
            } else {
                panic!("expected nested Or tree, got {:?}", items[0]);
            }
            if let LogicNode::Filter(f) = &items[1] {
                assert_eq!(f.field, "c");
                assert_eq!(f.value, FilterValue::Single("3".into()));
            } else {
                panic!("expected filter, got {:?}", items[1]);
            }
        } else {
            panic!("expected And tree");
        }
    }

    #[test]
    fn parse_not_or() {
        let node = parse_logic_tree("not.or", "(a.eq.1,b.eq.2)").unwrap();
        if let LogicNode::Not(inner) = node {
            if let LogicNode::Tree(LogicTree::Or(items)) = *inner {
                assert_eq!(items.len(), 2);
            } else {
                panic!("expected Or tree inside Not");
            }
        } else {
            panic!("expected Not node");
        }
    }

    #[test]
    fn parse_in_inside_logic() {
        let node = parse_logic_tree("or", "(id.in.(1,2,3),name.eq.test)").unwrap();
        if let LogicNode::Tree(LogicTree::Or(items)) = node {
            if let LogicNode::Filter(f) = &items[0] {
                assert_eq!(f.operator, Operator::In);
                match &f.value {
                    FilterValue::List(v) => {
                        assert_eq!(v, &vec!["1".to_string(), "2".to_string(), "3".to_string()])
                    }
                    _ => panic!("expected List, got {:?}", f.value),
                }
            } else {
                panic!("expected filter");
            }
        } else {
            panic!("expected Or tree");
        }
    }

    #[test]
    fn parse_deeply_nested() {
        let node = parse_logic_tree("or", "(a.eq.1,not.and(b.eq.2,c.eq.3))").unwrap();
        if let LogicNode::Tree(LogicTree::Or(items)) = node {
            assert_eq!(items.len(), 2);
            if let LogicNode::Not(inner) = &items[1] {
                if let LogicNode::Tree(LogicTree::And(and_items)) = inner.as_ref() {
                    assert_eq!(and_items.len(), 2);
                } else {
                    panic!("expected And tree inside Not");
                }
            } else {
                panic!("expected Not node, got {:?}", items[1]);
            }
        } else {
            panic!("expected Or tree");
        }
    }

    #[test]
    fn parse_error_empty() {
        assert!(parse_logic_tree("and", "()").is_err());
    }

    #[test]
    fn parse_error_unwrapped() {
        assert!(parse_logic_tree("and", "a.eq.1").is_err());
    }
}
