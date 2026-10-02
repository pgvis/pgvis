//! # Parser for the `select=` query parameter.
//!
//! Recursive descent in winnow: nested relations are just regular function
//! recursion on `&mut &str`. After parsing `[alias:]name`, the next character
//! tells us whether to continue as a relation (`(…)`), or as a field
//! (with JSON path / cast / aggregate decorations).
//!
//! ## Grammar
//!
//! ```text
//! select_list = select_item ("," select_item)*
//! select_item = spread_rel | relation | field
//! field       = [alias ":"] (star | count() | name [json_path] ["::" cast] ["." agg "()" ["::" cast]])
//! relation    = [alias ":"] name embed_params "(" select_list ")"
//! spread_rel  = "..." name embed_params "(" select_list ")"
//! embed_params= ["!" (hint | join)] ["!" (hint | join)]
//! ```

use winnow::combinator::{alt, delimited, opt, preceded, separated, terminated};
use winnow::token::take_while;
use winnow::{Parser, Result};

use super::common::{alias_sep, field_name, json_path};
use crate::select_ast::{
    AggregateFunction, FieldSelect, JoinType, RelationSelect, SelectItem, SpreadSelect,
};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Parse the full `select=` value into a list of select items.
///
/// Empty input is treated as "select all columns" (returns `vec![]`).
pub fn parse_select(input: &str) -> Result<Vec<SelectItem>, String> {
    if input.is_empty() {
        return Ok(vec![]);
    }
    select_list
        .parse(input)
        .map_err(|e| format!("failed to parse select: {e}"))
}

// ---------------------------------------------------------------------------
// Grammar
// ---------------------------------------------------------------------------

fn select_list(input: &mut &str) -> Result<Vec<SelectItem>> {
    separated(1.., select_item, ',').parse_next(input)
}

fn select_item(input: &mut &str) -> Result<SelectItem> {
    // Spread: `...name…(…)`
    if input.starts_with("...") {
        "...".parse_next(input)?;
        let s = spread_body(input)?;
        return Ok(SelectItem::Spread(s));
    }
    field_or_relation(input)
}

/// Once we know it isn't a spread, parse a field or a relation.
///
/// We parse the common prefix (`[alias:]name`), then dispatch on what follows:
/// if `[!hint][!join]` followed by `(` → relation; otherwise field decorations.
fn field_or_relation(input: &mut &str) -> Result<SelectItem> {
    // Star is a complete item by itself (no decorations).
    if input.starts_with('*') {
        '*'.parse_next(input)?;
        return Ok(SelectItem::Star);
    }

    let alias = opt(terminated(field_name, alias_sep)).parse_next(input)?;

    // `count()` is special — no column name.
    if input.starts_with("count()") {
        "count()".parse_next(input)?;
        let agg_cast = opt(preceded("::", cast_type)).parse_next(input)?;
        return Ok(SelectItem::Field(FieldSelect {
            name: String::new(),
            alias,
            json_path: vec![],
            cast: None,
            aggregate: Some(AggregateFunction::Count),
            aggregate_cast: agg_cast,
        }));
    }

    let name = field_name(input)?;
    let (hint, join_type) = embed_params(input)?;

    // Relation if followed by `(`.
    if input.starts_with('(') {
        let children = delimited('(', select_list, ')').parse_next(input)?;
        return Ok(SelectItem::Relation(RelationSelect {
            name,
            alias,
            hint,
            join_type,
            children,
        }));
    }

    // Otherwise must be a field — but we may have consumed embed_params we
    // shouldn't have. `!` after a name on a non-relation is a parse error.
    if hint.is_some() || join_type.is_some() {
        return Err(winnow::error::ContextError::new());
    }

    let json = json_path(input)?;
    let cast = opt(preceded("::", cast_type)).parse_next(input)?;
    let aggregate = opt(preceded('.', terminated(aggregation, "()"))).parse_next(input)?;
    let aggregate_cast = if aggregate.is_some() {
        opt(preceded("::", cast_type)).parse_next(input)?
    } else {
        None
    };

    Ok(SelectItem::Field(FieldSelect {
        name,
        alias,
        json_path: json,
        cast,
        aggregate,
        aggregate_cast,
    }))
}

/// Spread body, after `...` is consumed: `name embed_params "(" select_list ")"`.
fn spread_body(input: &mut &str) -> Result<SpreadSelect> {
    let name = field_name(input)?;
    let (hint, join_type) = embed_params(input)?;
    let children = delimited('(', select_list, ')').parse_next(input)?;
    Ok(SpreadSelect {
        name,
        hint,
        join_type,
        children,
    })
}

/// Up to two `!token`s: each is a join keyword (`!left`/`!inner`) or a hint.
fn embed_params(input: &mut &str) -> Result<(Option<String>, Option<JoinType>)> {
    let p1 = opt(embed_param).parse_next(input)?;
    let p2 = opt(embed_param).parse_next(input)?;
    Ok((
        hint_of(p1.as_ref()).or_else(|| hint_of(p2.as_ref())),
        join_of(p1.as_ref()).or_else(|| join_of(p2.as_ref())),
    ))
}

#[derive(Clone)]
enum EmbedParam {
    Join(JoinType),
    Hint(String),
}

fn embed_param(input: &mut &str) -> Result<EmbedParam> {
    preceded(
        '!',
        alt((
            "left".value(EmbedParam::Join(JoinType::Left)),
            "inner".value(EmbedParam::Join(JoinType::Inner)),
            field_name.map(EmbedParam::Hint),
        )),
    )
    .parse_next(input)
}

fn hint_of(p: Option<&EmbedParam>) -> Option<String> {
    match p {
        Some(EmbedParam::Hint(h)) => Some(h.clone()),
        _ => None,
    }
}

fn join_of(p: Option<&EmbedParam>) -> Option<JoinType> {
    match p {
        Some(EmbedParam::Join(j)) => Some(*j),
        _ => None,
    }
}

fn aggregation(input: &mut &str) -> Result<AggregateFunction> {
    alt((
        "sum".value(AggregateFunction::Sum),
        "avg".value(AggregateFunction::Avg),
        "count".value(AggregateFunction::Count),
        "max".value(AggregateFunction::Max),
        "min".value(AggregateFunction::Min),
    ))
    .parse_next(input)
}

/// A `::cast` target type, e.g. `text`, `double precision`, `int[]`.
///
/// Accepts exactly the characters the renderer emits, so a cast is never
/// silently altered or dropped between parse and SQL. A blank type is rejected.
fn cast_type(input: &mut &str) -> Result<String> {
    take_while(1.., |c: char| {
        c.is_ascii_alphanumeric() || matches!(c, '_' | ' ' | '[' | ']')
    })
    .map(|s: &str| s.trim().to_string())
    .verify(|s: &String| !s.is_empty())
    .parse_next(input)
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::select_ast::*;

    #[test]
    fn parse_empty() {
        assert_eq!(parse_select("").unwrap(), vec![]);
    }

    #[test]
    fn parse_star() {
        let result = parse_select("*").unwrap();
        assert_eq!(result.len(), 1);
        assert!(
            matches!(&result[0], SelectItem::Star),
            "expected Star, got {:?}",
            result[0]
        );
    }

    #[test]
    fn parse_single_column() {
        let result = parse_select("name").unwrap();
        assert_eq!(
            result,
            vec![SelectItem::Field(FieldSelect {
                name: "name".into(),
                alias: None,
                json_path: vec![],
                cast: None,
                aggregate: None,
                aggregate_cast: None,
            })]
        );
    }

    #[test]
    fn parse_multiple_columns() {
        let result = parse_select("id,name,email").unwrap();
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn parse_aliased_column() {
        let result = parse_select("alias:name").unwrap();
        if let SelectItem::Field(f) = &result[0] {
            assert_eq!(f.name, "name");
            assert_eq!(f.alias, Some("alias".into()));
        } else {
            panic!("expected field");
        }
    }

    #[test]
    fn parse_cast() {
        let result = parse_select("name::cast").unwrap();
        if let SelectItem::Field(f) = &result[0] {
            assert_eq!(f.name, "name");
            assert_eq!(f.cast, Some("cast".into()));
        } else {
            panic!("expected field");
        }
    }

    #[test]
    fn parse_json_path() {
        let result = parse_select("name->jsonpath").unwrap();
        if let SelectItem::Field(f) = &result[0] {
            assert_eq!(f.name, "name");
            assert_eq!(f.json_path.len(), 1);
            assert_eq!(
                f.json_path[0],
                JsonOperation::Arrow(JsonOperand::Key("jsonpath".into()))
            );
        } else {
            panic!("expected field");
        }
    }

    #[test]
    fn parse_alias_json_cast() {
        let result = parse_select("alias:name->jsonpath::cast").unwrap();
        if let SelectItem::Field(f) = &result[0] {
            assert_eq!(f.name, "name");
            assert_eq!(f.alias, Some("alias".into()));
            assert_eq!(
                f.json_path[0],
                JsonOperation::Arrow(JsonOperand::Key("jsonpath".into()))
            );
            assert_eq!(f.cast, Some("cast".into()));
        } else {
            panic!("expected field");
        }
    }

    #[test]
    fn parse_aggregate() {
        let result = parse_select("amount.sum()").unwrap();
        if let SelectItem::Field(f) = &result[0] {
            assert_eq!(f.name, "amount");
            assert_eq!(f.aggregate, Some(AggregateFunction::Sum));
        } else {
            panic!("expected field");
        }
    }

    #[test]
    fn parse_count() {
        let result = parse_select("cnt:count()").unwrap();
        if let SelectItem::Field(f) = &result[0] {
            assert_eq!(f.name, "");
            assert_eq!(f.alias, Some("cnt".into()));
            assert_eq!(f.aggregate, Some(AggregateFunction::Count));
        } else {
            panic!("expected field");
        }
    }

    #[test]
    fn parse_aggregate_with_cast() {
        let result = parse_select("total:amount.sum()::text").unwrap();
        if let SelectItem::Field(f) = &result[0] {
            assert_eq!(f.name, "amount");
            assert_eq!(f.alias, Some("total".into()));
            assert_eq!(f.aggregate, Some(AggregateFunction::Sum));
            assert_eq!(f.aggregate_cast, Some("text".into()));
        } else {
            panic!("expected field");
        }
    }

    #[test]
    fn parse_cast_accepts_what_the_renderer_emits() {
        let result = parse_select("tags::text[],price::double precision").unwrap();
        let casts: Vec<_> = result
            .iter()
            .map(|item| match item {
                SelectItem::Field(f) => f.cast.clone(),
                _ => panic!("expected field"),
            })
            .collect();
        assert_eq!(
            casts,
            [Some("text[]".into()), Some("double precision".into())]
        );
    }

    #[test]
    fn parse_cast_rejects_types_the_renderer_would_drop() {
        // Previously `$` parsed but was stripped at render, silently dropping the cast.
        assert!(parse_select("x::$").is_err());
        assert!(parse_select("x::int$").is_err());
        assert!(parse_select("x:: ").is_err());
        assert!(parse_select("count()::$").is_err());
    }

    #[test]
    fn parse_relation() {
        let result = parse_select("orders(id,total)").unwrap();
        if let SelectItem::Relation(r) = &result[0] {
            assert_eq!(r.name, "orders");
            assert_eq!(r.alias, None);
            assert_eq!(r.hint, None);
            assert_eq!(r.children.len(), 2);
        } else {
            panic!("expected relation, got {:?}", result[0]);
        }
    }

    #[test]
    fn parse_relation_with_alias() {
        let result = parse_select("my_orders:orders(id)").unwrap();
        if let SelectItem::Relation(r) = &result[0] {
            assert_eq!(r.name, "orders");
            assert_eq!(r.alias, Some("my_orders".into()));
        } else {
            panic!("expected relation");
        }
    }

    #[test]
    fn parse_relation_with_hint() {
        let result = parse_select("orders!customer_fk(id)").unwrap();
        if let SelectItem::Relation(r) = &result[0] {
            assert_eq!(r.name, "orders");
            assert_eq!(r.hint, Some("customer_fk".into()));
        } else {
            panic!("expected relation");
        }
    }

    #[test]
    fn parse_relation_inner_join() {
        let result = parse_select("orders!inner(id)").unwrap();
        if let SelectItem::Relation(r) = &result[0] {
            assert_eq!(r.name, "orders");
            assert_eq!(r.join_type, Some(JoinType::Inner));
            assert_eq!(r.hint, None);
        } else {
            panic!("expected relation");
        }
    }

    #[test]
    fn parse_relation_hint_and_join() {
        let result = parse_select("orders!customer_fk!inner(id)").unwrap();
        if let SelectItem::Relation(r) = &result[0] {
            assert_eq!(r.name, "orders");
            assert_eq!(r.hint, Some("customer_fk".into()));
            assert_eq!(r.join_type, Some(JoinType::Inner));
        } else {
            panic!("expected relation");
        }
    }

    #[test]
    fn parse_relation_join_and_hint() {
        let result = parse_select("o:orders!inner!customer_fk(id)").unwrap();
        if let SelectItem::Relation(r) = &result[0] {
            assert_eq!(r.name, "orders");
            assert_eq!(r.alias, Some("o".into()));
            assert_eq!(r.hint, Some("customer_fk".into()));
            assert_eq!(r.join_type, Some(JoinType::Inner));
        } else {
            panic!("expected relation");
        }
    }

    #[test]
    fn parse_spread() {
        let result = parse_select("...customers(name)").unwrap();
        if let SelectItem::Spread(s) = &result[0] {
            assert_eq!(s.name, "customers");
            assert_eq!(s.children.len(), 1);
        } else {
            panic!("expected spread, got {:?}", result[0]);
        }
    }

    #[test]
    fn parse_spread_with_hint_and_join() {
        let result = parse_select("...customers!fk!inner(name)").unwrap();
        if let SelectItem::Spread(s) = &result[0] {
            assert_eq!(s.name, "customers");
            assert_eq!(s.hint, Some("fk".into()));
            assert_eq!(s.join_type, Some(JoinType::Inner));
        } else {
            panic!("expected spread");
        }
    }

    #[test]
    fn parse_complex_select() {
        let result =
            parse_select("id,name,total:amount.sum(),orders!inner(id,items(name,qty))").unwrap();
        assert_eq!(result.len(), 4);
        if let SelectItem::Relation(r) = &result[3] {
            assert_eq!(r.name, "orders");
            assert_eq!(r.join_type, Some(JoinType::Inner));
            assert_eq!(r.children.len(), 2);
            if let SelectItem::Relation(nested) = &r.children[1] {
                assert_eq!(nested.name, "items");
                assert_eq!(nested.children.len(), 2);
            } else {
                panic!("expected nested relation");
            }
        } else {
            panic!("expected relation");
        }
    }

    #[test]
    fn parse_json_double_arrow() {
        let result = parse_select("data->>name").unwrap();
        if let SelectItem::Field(f) = &result[0] {
            assert_eq!(f.name, "data");
            assert_eq!(
                f.json_path[0],
                JsonOperation::DoubleArrow(JsonOperand::Key("name".into()))
            );
        } else {
            panic!("expected field");
        }
    }

    #[test]
    fn parse_nested_json() {
        let result = parse_select("data->address->>city").unwrap();
        if let SelectItem::Field(f) = &result[0] {
            assert_eq!(f.name, "data");
            assert_eq!(f.json_path.len(), 2);
        } else {
            panic!("expected field");
        }
    }

    #[test]
    fn parse_json_index() {
        let result = parse_select("data->0->>name").unwrap();
        if let SelectItem::Field(f) = &result[0] {
            assert_eq!(f.name, "data");
            assert_eq!(f.json_path[0], JsonOperation::Arrow(JsonOperand::Index(0)));
        } else {
            panic!("expected field");
        }
    }

    #[test]
    fn parse_error_mismatched_paren() {
        assert!(parse_select("orders(id").is_err());
    }

    #[test]
    fn parse_error_trailing_bang() {
        // `!hint` not followed by `(` should fail.
        assert!(parse_select("orders!hint").is_err());
    }
}
