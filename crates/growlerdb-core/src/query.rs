//! The **query AST** and the **Lucene/KQL query-string parser** ([Design 03]).
//!
//! The AST is canonical; the string form parses into it. The parser ([`Query::parse`]
//! / [`Query::parse_kql`]) covers `field:value`, phrases `"…"~slop`, ranges
//! `[a TO b}`, wildcards `*`/`?`, fuzzy `~n`, CIDR `addr/n`, regex `/…/`, boost `^n`,
//! `AND`/`OR`/`NOT`/`-`, and grouping. Field existence and **type rules** are
//! validated at **execution** (against the index schema), where the field types are
//! known — see `growlerdb-index`.
//!
//! [Design 03]: ../../../design/03-query-schema.md

use std::fmt;

/// How [`Match`](Query::Match) combines its analyzed tokens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchOp {
    /// Any token may match (the default).
    Or,
    /// Every token must match.
    And,
}

/// A parsed query ([Design 03] AST). The text-family leaves (`Match` … `Regex`)
/// and `Exists` are M2 (task-21); typed `Range`/`IpCidr` land with the typed-bound
/// work. Field existence and **type rules** are validated at **execution** (against
/// the index schema), where the field types are known — see `growlerdb-index`.
///
/// [Design 03]: ../../../design/03-query-schema.md
#[derive(Debug, Clone, PartialEq)]
pub enum Query {
    /// Match every document.
    MatchAll,
    /// A single-token leaf: `field:value`, or an unqualified `value` (`field:
    /// None`) against the index's default TEXT field. On a TEXT field the value
    /// is analyzed (lowercased); on a KEYWORD field it matches exactly.
    Term {
        /// Target field, or `None` for the default field.
        field: Option<String>,
        /// The single-token value.
        value: String,
    },
    /// Set membership (`field IN (values)`) on a keyword/text field.
    Terms {
        /// Target field.
        field: String,
        /// The candidate values (matches any).
        values: Vec<String>,
    },
    /// Full-text match: analyze `text` into tokens and combine them by `op`.
    Match {
        /// Target field, or `None` for the default field.
        field: Option<String>,
        /// The free text to analyze.
        text: String,
        /// How the resulting tokens combine.
        op: MatchOp,
    },
    /// Ordered token sequence with positional tolerance `slop` (text fields only).
    Phrase {
        /// Target field, or `None` for the default field.
        field: Option<String>,
        /// The phrase tokens, in order.
        terms: Vec<String>,
        /// Positional slop (0 = exact adjacency).
        slop: u32,
    },
    /// Prefix match (`web-*` style) on a text/keyword field.
    Prefix {
        /// Target field, or `None` for the default field.
        field: Option<String>,
        /// The literal prefix.
        prefix: String,
    },
    /// Glob match using `*` (multi-char) and `?` (single-char).
    Wildcard {
        /// Target field, or `None` for the default field.
        field: Option<String>,
        /// The glob pattern.
        pattern: String,
    },
    /// Edit-distance match with `distance` (0/1/2) on a text/keyword field.
    Fuzzy {
        /// Target field, or `None` for the default field.
        field: Option<String>,
        /// The base value.
        value: String,
        /// Levenshtein distance.
        distance: u8,
    },
    /// Regular-expression match against indexed terms (text/keyword field).
    Regex {
        /// Target field, or `None` for the default field.
        field: Option<String>,
        /// The regex pattern (anchored to the whole term).
        pattern: String,
    },
    /// The field has a (non-null) value. Requires a fast field.
    Exists {
        /// Target field.
        field: String,
    },
    /// A bounded interval over a typed field (numeric / date / ip / keyword). Bounds
    /// are strings parsed to the field's type at execution; an absent bound is
    /// unbounded on that side (`[lower TO upper]`, `[lower TO ]`, …).
    Range {
        /// Target field.
        field: String,
        /// Lower bound value (`None` = unbounded below).
        lower: Option<String>,
        /// Whether the lower bound is inclusive (`[` vs `{`).
        lower_inclusive: bool,
        /// Upper bound value (`None` = unbounded above).
        upper: Option<String>,
        /// Whether the upper bound is inclusive (`]` vs `}`).
        upper_inclusive: bool,
    },
    /// An IP field contained in a CIDR block, e.g. `gateway_ip:10.0.0.0/8`.
    IpCidr {
        /// Target field (must be an IP field).
        field: String,
        /// The CIDR block (`addr/prefix`).
        cidr: String,
    },
    /// A boolean combination: `must` (AND, scored), `should` (OR, scored),
    /// `must_not` (NOT), and `filter` (AND, **non-scoring**). A purely-negative or
    /// filter-only `Bool` is executed as match-all-then-constrain.
    Bool {
        /// AND clauses (scored).
        must: Vec<Query>,
        /// OR clauses (scored).
        should: Vec<Query>,
        /// NOT clauses.
        must_not: Vec<Query>,
        /// AND clauses applied without affecting the score.
        filter: Vec<Query>,
    },
    /// Scale the wrapped query's score by `boost`.
    Boost {
        /// The wrapped query.
        query: Box<Query>,
        /// Multiplicative score factor.
        boost: f32,
    },
}

/// Query-string syntax mode ([Design 03]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Syntax {
    /// Lucene: case-insensitive `AND`/`OR`/`NOT`, `&&`/`||`, leading `-` = NOT.
    #[default]
    Lucene,
    /// KQL: lowercase `and`/`or`/`not` operators.
    Kql,
}

impl Query {
    /// Parse a **Lucene**-style query string into the canonical AST ([Design 03]):
    /// `field:value`, phrases `"…"~slop`, ranges `[a TO b}`, wildcards `*`/`?`,
    /// fuzzy `~n`, CIDR `addr/n`, regex `/…/`, boost `^n`, `AND`/`OR`/`NOT`/`-`, and
    /// grouping.
    pub fn parse(input: &str) -> Result<Query, ParseError> {
        Self::parse_with(input, Syntax::Lucene)
    }

    /// Parse a **KQL**-style query string (lowercase `and`/`or`/`not`).
    pub fn parse_kql(input: &str) -> Result<Query, ParseError> {
        Self::parse_with(input, Syntax::Kql)
    }

    /// Parse with an explicit [`Syntax`] mode.
    pub fn parse_with(input: &str, syntax: Syntax) -> Result<Query, ParseError> {
        let tokens = lex(input, syntax);
        if tokens.is_empty() {
            return Err(ParseError::Empty);
        }
        let mut parser = Parser {
            tokens,
            pos: 0,
            depth: 0,
        };
        let query = parser.parse_or()?;
        if parser.pos != parser.tokens.len() {
            return Err(ParseError::UnexpectedToken(parser.describe_pos()));
        }
        Ok(query)
    }

    /// AND a mandatory, **non-scoring** `field = value` constraint onto this query — the
    /// mechanism behind tenant scoping (task-38). The original query becomes a single `must`
    /// clause and the constraint a sibling `filter`, so a match requires **both**: no `OR` (or
    /// any structure) in the caller's query can widen past the constraint. The caller's
    /// scoring is preserved (the constraint is a non-scoring filter).
    pub fn and_filter(self, field: impl Into<String>, value: impl Into<String>) -> Query {
        Query::Bool {
            must: vec![self],
            should: Vec::new(),
            must_not: Vec::new(),
            filter: vec![Query::Term {
                field: Some(field.into()),
                value: value.into(),
            }],
        }
    }

    /// The conjunctive integer `[lower, upper]` bounds this query constrains `field` to — from
    /// `Range` clauses on `field` in **must/filter** (conjunctive) positions, intersected (tightest
    /// wins). `should`/`must_not` can't narrow the result, so they're ignored. `None` = unbounded;
    /// a bound that doesn't parse as `i64` is treated as unbounded. Used for time-window pruning
    /// (task-81): both the ingest-time window field and the event-time field are queried this way.
    pub fn range_bounds(&self, field: &str) -> (Option<i64>, Option<i64>) {
        fn parse(s: &Option<String>) -> Option<i64> {
            s.as_ref().and_then(|v| v.parse::<i64>().ok())
        }
        match self {
            Query::Range {
                field: f,
                lower,
                upper,
                ..
            } if f == field => (parse(lower), parse(upper)),
            Query::Bool { must, filter, .. } => {
                let (mut lo, mut hi): (Option<i64>, Option<i64>) = (None, None);
                for q in must.iter().chain(filter) {
                    let (l, h) = q.range_bounds(field);
                    if let Some(l) = l {
                        lo = Some(lo.map_or(l, |c| c.max(l)));
                    }
                    if let Some(h) = h {
                        hi = Some(hi.map_or(h, |c| c.min(h)));
                    }
                }
                (lo, hi)
            }
            _ => (None, None),
        }
    }
}

/// A query-string parse failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    /// The input was empty or only whitespace.
    #[error("empty query")]
    Empty,
    /// A `(` had no matching `)`.
    #[error("unbalanced parenthesis")]
    UnbalancedParen,
    /// An operator (AND/OR/NOT) was missing its operand.
    #[error("expected a term but found {0}")]
    MissingOperand(String),
    /// Unexpected trailing or misplaced token.
    #[error("unexpected token: {0}")]
    UnexpectedToken(String),
    /// Parenthesis nesting exceeded [`MAX_QUERY_DEPTH`] — bounds parser recursion so a crafted
    /// query can't overflow the stack (task-146).
    #[error("query nested too deeply (max {0})")]
    TooDeep(usize),
    /// A `field:value` clause was malformed (bad range / phrase / number),
    /// reported with the byte offset of the offending atom.
    #[error("invalid value at position {at}: {message}")]
    InvalidValue {
        /// What went wrong.
        message: String,
        /// Byte offset of the atom in the input.
        at: usize,
    },
}

// ---- Lexer -----------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    LParen,
    RParen,
    And,
    Or,
    Not,
    Term(String),
}

impl fmt::Display for Tok {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Tok::LParen => write!(f, "`(`"),
            Tok::RParen => write!(f, "`)`"),
            Tok::And => write!(f, "`AND`"),
            Tok::Or => write!(f, "`OR`"),
            Tok::Not => write!(f, "`NOT`"),
            Tok::Term(t) => write!(f, "`{t}`"),
        }
    }
}

/// A token plus the byte offset where it began (for located errors).
struct Spanned {
    tok: Tok,
    start: usize,
}

/// Split the input into tokens. `(`/`)` delimit; `AND`/`OR`/`NOT` (per `syntax`) and
/// `&&`/`||` are operators; a leading `-` is `NOT`. Everything else is a `Term` atom
/// — whitespace **inside** a quoted phrase (`"…"`) or a range (`[…]`/`{…}`) does not
/// split it, so those reach the value parser intact.
fn lex(input: &str, syntax: Syntax) -> Vec<Spanned> {
    let mut tokens = Vec::new();
    let mut word = String::new();
    let mut start = 0usize;
    let mut in_quote = false;
    let mut in_range = false;

    let flush = |word: &mut String, start: usize, tokens: &mut Vec<Spanned>| {
        if word.is_empty() {
            return;
        }
        let w = std::mem::take(word);
        if let Some(op) = operator(&w, syntax) {
            tokens.push(Spanned { tok: op, start });
            return;
        }
        // A leading '-' on a longer word is a NOT prefix (Lucene).
        if syntax == Syntax::Lucene {
            if let Some(rest) = w.strip_prefix('-') {
                if !rest.is_empty() {
                    tokens.push(Spanned {
                        tok: Tok::Not,
                        start,
                    });
                    tokens.push(Spanned {
                        tok: Tok::Term(rest.to_string()),
                        start: start + 1,
                    });
                    return;
                }
            }
        }
        tokens.push(Spanned {
            tok: Tok::Term(w),
            start,
        });
    };

    for (i, ch) in input.char_indices() {
        if word.is_empty() {
            start = i;
        }
        if in_quote {
            word.push(ch);
            if ch == '"' {
                in_quote = false;
            }
            continue;
        }
        if in_range {
            word.push(ch);
            if ch == ']' || ch == '}' {
                in_range = false;
            }
            continue;
        }
        match ch {
            '"' => {
                word.push(ch);
                in_quote = true;
            }
            '[' | '{' => {
                word.push(ch);
                in_range = true;
            }
            '(' | ')' => {
                flush(&mut word, start, &mut tokens);
                tokens.push(Spanned {
                    tok: if ch == '(' { Tok::LParen } else { Tok::RParen },
                    start: i,
                });
            }
            c if c.is_whitespace() => flush(&mut word, start, &mut tokens),
            c => word.push(c),
        }
    }
    flush(&mut word, start, &mut tokens);
    tokens
}

/// Recognize an operator word for the given syntax (Lucene is case-insensitive;
/// KQL operators are lowercase).
fn operator(word: &str, syntax: Syntax) -> Option<Tok> {
    match syntax {
        Syntax::Lucene => match word.to_ascii_uppercase().as_str() {
            "AND" | "&&" => Some(Tok::And),
            "OR" | "||" => Some(Tok::Or),
            "NOT" => Some(Tok::Not),
            _ => None,
        },
        Syntax::Kql => match word {
            "and" => Some(Tok::And),
            "or" => Some(Tok::Or),
            "not" => Some(Tok::Not),
            _ => None,
        },
    }
}

// ---- Parser (recursive descent: OR < AND < NOT < clause) -------------------

/// Max parenthesis nesting the parser accepts. The recursive descent recurses once per `(`
/// (`parse_clause` → `parse_or`), so without a bound a crafted query like `"(".repeat(200_000)`
/// overflows the stack — an *uncatchable* abort that kills the whole process for every tenant
/// (task-146 / F1). Set far above any human-authored query.
const MAX_QUERY_DEPTH: usize = 128;

/// Max fuzzy edit distance accepted at parse. Levenshtein automata are only meaningful to 2, and the
/// execution engine rejects more anyway — this fails fast with a clear message (task-146 / G1).
const MAX_FUZZY_DISTANCE: u8 = 2;

/// Max phrase slop accepted at parse. Proximity beyond this is pathological and super-linear at
/// execution; realistic slop is single digits (task-146 / G1).
const MAX_PHRASE_SLOP: u32 = 100;

struct Parser {
    tokens: Vec<Spanned>,
    pos: usize,
    /// Current parenthesis nesting depth, bounded by [`MAX_QUERY_DEPTH`].
    depth: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Tok> {
        self.tokens.get(self.pos).map(|s| &s.tok)
    }

    fn describe_pos(&self) -> String {
        self.peek()
            .map(|t| t.to_string())
            .unwrap_or_else(|| "end of input".to_string())
    }

    /// `or_expr = and_expr { OR and_expr }`
    fn parse_or(&mut self) -> Result<Query, ParseError> {
        let mut shoulds = vec![self.parse_and()?];
        while matches!(self.peek(), Some(Tok::Or)) {
            self.pos += 1;
            shoulds.push(self.parse_and()?);
        }
        Ok(if shoulds.len() == 1 {
            shoulds.pop().unwrap()
        } else {
            Query::Bool {
                must: vec![],
                should: shoulds,
                must_not: vec![],
                filter: vec![],
            }
        })
    }

    /// `and_expr = unary { [AND] unary }` — adjacent clauses are an implicit AND.
    fn parse_and(&mut self) -> Result<Query, ParseError> {
        let mut must = Vec::new();
        let mut must_not = Vec::new();
        loop {
            let negated = matches!(self.peek(), Some(Tok::Not));
            if negated {
                self.pos += 1;
            }
            let clause = self.parse_clause()?;
            if negated {
                must_not.push(clause);
            } else {
                must.push(clause);
            }

            match self.peek() {
                Some(Tok::And) => {
                    self.pos += 1;
                }
                // Implicit AND: another clause starts without an operator.
                Some(Tok::Term(_) | Tok::LParen | Tok::Not) => {}
                _ => break,
            }
        }
        Ok(if must.len() == 1 && must_not.is_empty() {
            must.pop().unwrap()
        } else {
            Query::Bool {
                must,
                should: vec![],
                must_not,
                filter: vec![],
            }
        })
    }

    /// `clause = "(" or_expr ")" | term`
    fn parse_clause(&mut self) -> Result<Query, ParseError> {
        match self.peek() {
            Some(Tok::LParen) => {
                self.pos += 1;
                // Bound recursion: a group descends into `parse_or` again, so unbounded nesting
                // would overflow the stack (task-146 / F1).
                self.depth += 1;
                if self.depth > MAX_QUERY_DEPTH {
                    return Err(ParseError::TooDeep(MAX_QUERY_DEPTH));
                }
                let inner = self.parse_or()?;
                self.depth -= 1;
                match self.peek() {
                    Some(Tok::RParen) => {
                        self.pos += 1;
                        Ok(inner)
                    }
                    _ => Err(ParseError::UnbalancedParen),
                }
            }
            Some(Tok::Term(_)) => {
                let Some(Spanned {
                    tok: Tok::Term(t),
                    start,
                }) = self.tokens.get(self.pos)
                else {
                    unreachable!()
                };
                // Field-grouped set: `field:(a OR b [OR c])` distributes the field prefix over the
                // group (→ `field:a OR field:b`). The lexer splits at `(`, so a bare `field:` term
                // (identifier + trailing `:`, no value) lands here immediately before the group's
                // `LParen`; parse the group and stamp the field onto its field-less leaves.
                if let Some(field) = field_prefix(t) {
                    if matches!(
                        self.tokens.get(self.pos + 1).map(|s| &s.tok),
                        Some(Tok::LParen)
                    ) {
                        self.pos += 1; // consume the `field:` term; `LParen` is next
                        let group = self.parse_clause()?;
                        return Ok(apply_field_prefix(group, &field));
                    }
                }
                let query = atom_to_query(t, *start)?;
                self.pos += 1;
                Ok(query)
            }
            other => Err(ParseError::MissingOperand(
                other
                    .map(|t| t.to_string())
                    .unwrap_or_else(|| "end of input".to_string()),
            )),
        }
    }
}

/// Parse a clause atom (`field:value` or a bare value at byte offset `at`) into a
/// leaf node, detecting the value shape per the [Design 03] mapping table.
fn atom_to_query(atom: &str, at: usize) -> Result<Query, ParseError> {
    let (field, value) = split_field(atom);
    let (core, boost) = strip_boost(value);
    // Reject non-finite / negative boosts (task-146 / B1): `f32::from_str` accepts `nan`/`inf`/
    // negatives, which poison the top-k and sort comparators (NaN breaks total ordering → panic or
    // corrupt ranking) once the boost reaches execution.
    if let Some(b) = boost {
        if !b.is_finite() || b < 0.0 {
            return Err(ParseError::InvalidValue {
                message: "boost must be a finite, non-negative number".to_string(),
                at,
            });
        }
    }
    let node = value_node(field, core, at)?;
    Ok(match boost {
        Some(b) => Query::Boost {
            query: Box::new(node),
            boost: b,
        },
        None => node,
    })
}

/// Split a leading `field:` (identifier + `:`) from the value; unqualified otherwise.
fn split_field(atom: &str) -> (Option<String>, &str) {
    if let Some(colon) = atom.find(':') {
        let (f, rest) = atom.split_at(colon);
        let value = &rest[1..];
        let ident = !f.is_empty()
            && f.chars()
                .all(|c| c.is_alphanumeric() || c == '_' || c == '.');
        if ident && !value.is_empty() {
            return (Some(f.to_string()), value);
        }
    }
    (None, atom)
}

/// Recognize a bare `field:` prefix token (a valid identifier followed by a trailing `:` and
/// nothing else) — the lexer emits this immediately before a `(` in `field:(a OR b)`. Returns the
/// field name, or `None` if the atom isn't a lone field prefix.
fn field_prefix(atom: &str) -> Option<String> {
    let field = atom.strip_suffix(':')?;
    let ident = !field.is_empty()
        && field
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '.');
    ident.then(|| field.to_string())
}

/// Distribute a `field:` prefix over a parsed group (`field:(a OR b)` → `field:a OR field:b`),
/// stamping the field onto every field-less leaf. Leaves that already carry a field (e.g. an inner
/// `other:x`) keep it; `Bool`/`Boost` recurse into their children.
fn apply_field_prefix(query: Query, field: &str) -> Query {
    let set = |f: &mut Option<String>| {
        if f.is_none() {
            *f = Some(field.to_string());
        }
    };
    match query {
        Query::Term {
            field: mut f,
            value,
        } => {
            set(&mut f);
            Query::Term { field: f, value }
        }
        Query::Match {
            field: mut f,
            text,
            op,
        } => {
            set(&mut f);
            Query::Match { field: f, text, op }
        }
        Query::Phrase {
            field: mut f,
            terms,
            slop,
        } => {
            set(&mut f);
            Query::Phrase {
                field: f,
                terms,
                slop,
            }
        }
        Query::Prefix {
            field: mut f,
            prefix,
        } => {
            set(&mut f);
            Query::Prefix { field: f, prefix }
        }
        Query::Wildcard {
            field: mut f,
            pattern,
        } => {
            set(&mut f);
            Query::Wildcard { field: f, pattern }
        }
        Query::Fuzzy {
            field: mut f,
            value,
            distance,
        } => {
            set(&mut f);
            Query::Fuzzy {
                field: f,
                value,
                distance,
            }
        }
        Query::Regex {
            field: mut f,
            pattern,
        } => {
            set(&mut f);
            Query::Regex { field: f, pattern }
        }
        Query::Bool {
            must,
            should,
            must_not,
            filter,
        } => {
            let map = |qs: Vec<Query>| {
                qs.into_iter()
                    .map(|q| apply_field_prefix(q, field))
                    .collect()
            };
            Query::Bool {
                must: map(must),
                should: map(should),
                must_not: map(must_not),
                filter: map(filter),
            }
        }
        Query::Boost { query, boost } => Query::Boost {
            query: Box::new(apply_field_prefix(*query, field)),
            boost,
        },
        // MatchAll and the leaves that already require a field (Terms/Exists/Range/IpCidr) can't be
        // produced field-less inside a group, so they pass through unchanged.
        other => other,
    }
}

/// Strip a trailing `^<number>` boost factor (not inside a `/regex/`).
fn strip_boost(value: &str) -> (&str, Option<f32>) {
    if let Some(caret) = value.rfind('^') {
        if let Ok(b) = value[caret + 1..].parse::<f32>() {
            return (&value[..caret], Some(b));
        }
    }
    (value, None)
}

/// Map a (boost-stripped) value to its leaf node by shape.
fn value_node(field: Option<String>, core: &str, at: usize) -> Result<Query, ParseError> {
    let invalid = |m: &str| ParseError::InvalidValue {
        message: m.to_string(),
        at,
    };
    // Universal match-all: an unqualified `*`, or the Lucene/Elasticsearch idiom `*:*`, matches
    // every document (executed as a cheap `AllQuery`, not a term scan — so it isn't blocked by the
    // leading-wildcard cost guard). A *qualified* `field:*` stays a wildcard over that field.
    if field.is_none() && (core == "*" || core == "*:*") {
        return Ok(Query::MatchAll);
    }
    // Phrase: "…" with an optional ~slop.
    if let Some(rest) = core.strip_prefix('"') {
        let end = rest
            .find('"')
            .ok_or_else(|| invalid("unterminated phrase"))?;
        let terms = rest[..end].split_whitespace().map(String::from).collect();
        let suffix = &rest[end + 1..];
        let slop: u32 = match suffix.strip_prefix('~') {
            Some(n) => n.parse().map_err(|_| invalid("bad phrase slop"))?,
            None if suffix.is_empty() => 0,
            None => return Err(invalid("unexpected text after phrase")),
        };
        // Bound proximity (task-146 / G1): huge slop is super-linear at execution.
        if slop > MAX_PHRASE_SLOP {
            return Err(invalid("phrase slop too large (max 100)"));
        }
        return Ok(Query::Phrase { field, terms, slop });
    }
    // Range: [a TO b] / {a TO b} (any inclusivity), `*` or empty = unbounded.
    if core.starts_with('[') || core.starts_with('{') {
        let field = field.ok_or_else(|| invalid("range needs a field"))?;
        if !core.ends_with(']') && !core.ends_with('}') {
            return Err(invalid("unterminated range"));
        }
        let (lo, hi) = core[1..core.len() - 1]
            .split_once(" TO ")
            .ok_or_else(|| invalid("range needs ` TO `"))?;
        let bound = |s: &str| {
            let s = s.trim();
            (!s.is_empty() && s != "*").then(|| s.to_string())
        };
        return Ok(Query::Range {
            field,
            lower: bound(lo),
            lower_inclusive: core.starts_with('['),
            upper: bound(hi),
            upper_inclusive: core.ends_with(']'),
        });
    }
    // Regex: /…/.
    if core.len() >= 2 && core.starts_with('/') && core.ends_with('/') {
        return Ok(Query::Regex {
            field,
            pattern: core[1..core.len() - 1].to_string(),
        });
    }
    // CIDR `addr/prefix` (needs a field).
    if let Some(f) = &field {
        if is_cidr(core) {
            return Ok(Query::IpCidr {
                field: f.clone(),
                cidr: core.to_string(),
            });
        }
    }
    // Fuzzy: value~ or value~n.
    if let Some(tilde) = core.rfind('~') {
        let base = &core[..tilde];
        if !base.is_empty() {
            let suffix = &core[tilde + 1..];
            let distance: u8 = if suffix.is_empty() {
                2
            } else {
                suffix.parse().map_err(|_| invalid("bad fuzzy distance"))?
            };
            // Bound edit distance (task-146 / G1): only 0/1/2 are meaningful; the execution engine
            // rejects more anyway — fail fast with a clear message.
            if distance > MAX_FUZZY_DISTANCE {
                return Err(invalid("fuzzy distance must be 0, 1, or 2"));
            }
            return Ok(Query::Fuzzy {
                field,
                value: base.to_string(),
                distance,
            });
        }
    }
    // Wildcard: contains * or ?.
    if core.contains('*') || core.contains('?') {
        return Ok(Query::Wildcard {
            field,
            pattern: core.to_string(),
        });
    }
    Ok(Query::Term {
        field,
        value: core.to_string(),
    })
}

/// Whether `s` is `<ip-address>/<prefix>` (an IPv4/IPv6 CIDR block).
fn is_cidr(s: &str) -> bool {
    s.split_once('/').is_some_and(|(addr, prefix)| {
        addr.parse::<std::net::IpAddr>().is_ok() && prefix.parse::<u8>().is_ok()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn term(field: Option<&str>, value: &str) -> Query {
        Query::Term {
            field: field.map(String::from),
            value: value.to_string(),
        }
    }

    #[test]
    fn and_filter_wraps_the_query_as_must_with_the_constraint_in_filter() {
        // The user query becomes the sole `must`; the constraint is a non-scoring `filter`
        // sibling — so a match requires both and no `OR` inside the query can widen past it.
        let user = Query::parse("a OR b").unwrap();
        let scoped = user.clone().and_filter("tenant", "acme");
        assert_eq!(
            scoped,
            Query::Bool {
                must: vec![user],
                should: vec![],
                must_not: vec![],
                filter: vec![term(Some("tenant"), "acme")],
            }
        );
    }

    #[test]
    fn parses_bare_and_field_terms() {
        assert_eq!(Query::parse("error").unwrap(), term(None, "error"));
        assert_eq!(
            Query::parse("status:error").unwrap(),
            term(Some("status"), "error")
        );
    }

    #[test]
    fn parses_and_or_not() {
        assert_eq!(
            Query::parse("a AND b").unwrap(),
            Query::Bool {
                must: vec![term(None, "a"), term(None, "b")],
                should: vec![],
                must_not: vec![],
                filter: vec![],
            }
        );
        assert_eq!(
            Query::parse("a OR b").unwrap(),
            Query::Bool {
                must: vec![],
                should: vec![term(None, "a"), term(None, "b")],
                must_not: vec![],
                filter: vec![],
            }
        );
        assert_eq!(
            Query::parse("a AND NOT b").unwrap(),
            Query::Bool {
                must: vec![term(None, "a")],
                should: vec![],
                must_not: vec![term(None, "b")],
                filter: vec![],
            }
        );
    }

    #[test]
    fn dash_prefix_is_not() {
        assert_eq!(
            Query::parse("a -b").unwrap(),
            Query::parse("a NOT b").unwrap()
        );
    }

    #[test]
    fn implicit_and_between_adjacent_clauses() {
        assert_eq!(
            Query::parse("a b").unwrap(),
            Query::parse("a AND b").unwrap()
        );
    }

    #[test]
    fn precedence_and_binds_tighter_than_or() {
        // a OR b AND c  ==  a OR (b AND c)
        assert_eq!(
            Query::parse("a OR b AND c").unwrap(),
            Query::Bool {
                must: vec![],
                should: vec![
                    term(None, "a"),
                    Query::Bool {
                        must: vec![term(None, "b"), term(None, "c")],
                        should: vec![],
                        must_not: vec![],
                        filter: vec![],
                    }
                ],
                must_not: vec![],
                filter: vec![],
            }
        );
    }

    #[test]
    fn parentheses_override_precedence() {
        // (a OR b) AND c  →  must:[Bool{should:[a,b]}, c]
        assert_eq!(
            Query::parse("(a OR b) AND c").unwrap(),
            Query::Bool {
                must: vec![
                    Query::Bool {
                        must: vec![],
                        should: vec![term(None, "a"), term(None, "b")],
                        must_not: vec![],
                        filter: vec![],
                    },
                    term(None, "c"),
                ],
                should: vec![],
                must_not: vec![],
                filter: vec![],
            }
        );
    }

    #[test]
    fn field_grouped_set_distributes_the_prefix() {
        // `field:(a OR b)` must parse identically to `field:a OR field:b` — the field prefix
        // distributes over the group (task-247 / issue 3).
        assert_eq!(
            Query::parse("category:(guide OR reference)").unwrap(),
            Query::parse("category:guide OR category:reference").unwrap()
        );
        // Three-way, and the inner leaves all carry the field.
        assert_eq!(
            Query::parse("category:(a OR b OR c)").unwrap(),
            Query::Bool {
                must: vec![],
                should: vec![
                    term(Some("category"), "a"),
                    term(Some("category"), "b"),
                    term(Some("category"), "c"),
                ],
                must_not: vec![],
                filter: vec![],
            }
        );
        // Implicit-AND group distributes too.
        assert_eq!(
            Query::parse("category:(a b)").unwrap(),
            Query::parse("category:a AND category:b").unwrap()
        );
        // A leaf inside the group that already names a field keeps its own field.
        assert_eq!(
            Query::parse("category:(a OR other:b)").unwrap(),
            Query::Bool {
                must: vec![],
                should: vec![term(Some("category"), "a"), term(Some("other"), "b")],
                must_not: vec![],
                filter: vec![],
            }
        );
    }

    #[test]
    fn case_insensitive_operators() {
        assert_eq!(
            Query::parse("a and b").unwrap(),
            Query::parse("a AND b").unwrap()
        );
    }

    #[test]
    fn errors_are_clear_not_silent() {
        assert_eq!(Query::parse("").unwrap_err(), ParseError::Empty);
        assert_eq!(Query::parse("   ").unwrap_err(), ParseError::Empty);
        assert!(matches!(
            Query::parse("(a OR b").unwrap_err(),
            ParseError::UnbalancedParen
        ));
        assert!(matches!(
            Query::parse("AND b").unwrap_err(),
            ParseError::MissingOperand(_)
        ));
        assert!(matches!(
            Query::parse("a AND").unwrap_err(),
            ParseError::MissingOperand(_)
        ));
    }

    // ---- task-22: the full string grammar -------------------------------------

    #[test]
    fn parses_phrase_with_slop() {
        assert_eq!(
            Query::parse(r#"body:"disk full"~2"#).unwrap(),
            Query::Phrase {
                field: Some("body".into()),
                terms: vec!["disk".into(), "full".into()],
                slop: 2,
            }
        );
    }

    #[test]
    fn parses_range_with_inclusivity() {
        assert_eq!(
            Query::parse("bytes:[100 TO 200}").unwrap(),
            Query::Range {
                field: "bytes".into(),
                lower: Some("100".into()),
                lower_inclusive: true,
                upper: Some("200".into()),
                upper_inclusive: false,
            }
        );
        // `*` bound → unbounded on that side.
        assert_eq!(
            Query::parse("ts:[* TO 5]").unwrap(),
            Query::Range {
                field: "ts".into(),
                lower: None,
                lower_inclusive: true,
                upper: Some("5".into()),
                upper_inclusive: true,
            }
        );
    }

    #[test]
    fn parses_wildcard_fuzzy_cidr_regex() {
        assert_eq!(
            Query::parse("device_id:sensor-*").unwrap(),
            Query::Wildcard {
                field: Some("device_id".into()),
                pattern: "sensor-*".into(),
            }
        );
        assert_eq!(
            Query::parse("firmware:beta~1").unwrap(),
            Query::Fuzzy {
                field: Some("firmware".into()),
                value: "beta".into(),
                distance: 1,
            }
        );
        assert_eq!(
            Query::parse("gateway_ip:10.0.0.0/8").unwrap(),
            Query::IpCidr {
                field: "gateway_ip".into(),
                cidr: "10.0.0.0/8".into(),
            }
        );
        assert_eq!(
            Query::parse("name:/jd.e/").unwrap(),
            Query::Regex {
                field: Some("name".into()),
                pattern: "jd.e".into(),
            }
        );
    }

    #[test]
    fn parses_boost() {
        assert_eq!(
            Query::parse("status:error^2").unwrap(),
            Query::Boost {
                query: Box::new(term(Some("status"), "error")),
                boost: 2.0,
            }
        );
    }

    #[test]
    fn deep_nesting_is_rejected_not_crashed() {
        // task-146 / F1: a crafted deeply-nested query must return an error, never overflow the
        // stack. 200k open-parens would abort the process without the depth bound.
        let bomb = "(".repeat(200_000);
        assert_eq!(
            Query::parse(&bomb).unwrap_err(),
            ParseError::TooDeep(MAX_QUERY_DEPTH)
        );
        // A legitimately-nested query well under the bound still parses.
        let ok = format!("{}status:error{}", "(".repeat(20), ")".repeat(20));
        assert!(Query::parse(&ok).is_ok());
    }

    #[test]
    fn non_finite_or_negative_boost_is_rejected() {
        // task-146 / B1: NaN/Inf/negative boosts poison the scoring/sort comparators.
        for bad in ["x^nan", "x^inf", "x^-2", "status:error^infinity"] {
            assert!(
                matches!(
                    Query::parse(bad).unwrap_err(),
                    ParseError::InvalidValue { .. }
                ),
                "{bad} should be rejected"
            );
        }
        // A finite non-negative boost still parses.
        assert!(Query::parse("x^2.5").is_ok());
        assert!(Query::parse("x^0").is_ok());
    }

    #[test]
    fn out_of_range_fuzzy_and_slop_are_rejected() {
        // task-146 / G1: bound fuzzy distance and phrase slop at parse.
        assert!(matches!(
            Query::parse("name:jon~3").unwrap_err(),
            ParseError::InvalidValue { .. }
        ));
        assert!(matches!(
            Query::parse("name:jon~255").unwrap_err(),
            ParseError::InvalidValue { .. }
        ));
        assert!(matches!(
            Query::parse("\"a b\"~101").unwrap_err(),
            ParseError::InvalidValue { .. }
        ));
        // At the bounds is fine.
        assert!(Query::parse("name:jon~2").is_ok());
        assert!(Query::parse("\"a b\"~100").is_ok());
    }

    #[test]
    fn kql_mode_uses_lowercase_operators() {
        // `and` is an operator in KQL → one AND of two terms.
        assert_eq!(
            Query::parse_kql("a and b").unwrap(),
            Query::Bool {
                must: vec![term(None, "a"), term(None, "b")],
                should: vec![],
                must_not: vec![],
                filter: vec![],
            }
        );
        // Uppercase `AND` is a plain term in KQL → three implicit-AND clauses.
        let q = Query::parse_kql("a AND b").unwrap();
        if let Query::Bool { must, .. } = q {
            assert_eq!(must.len(), 3, "AND is a term, not an operator, in KQL");
        } else {
            panic!("expected a Bool, got {q:?}");
        }
    }

    #[test]
    fn malformed_value_is_a_located_error() {
        // Range without ` TO `, offset 5 (after "x OR ").
        assert!(matches!(
            Query::parse("x OR bytes:[1 2]").unwrap_err(),
            ParseError::InvalidValue { at: 5, .. }
        ));
    }

    #[test]
    fn range_bounds_extracts_conjunctive_field_bounds() {
        let rb = |q: &str| Query::parse(q).unwrap().range_bounds("ts");
        assert_eq!(rb("ts:[100 TO 500]"), (Some(100), Some(500)));
        assert_eq!(rb("ts:[100 TO *]"), (Some(100), None)); // open upper
        assert_eq!(
            rb("ts:[100 TO 500] AND status:active"),
            (Some(100), Some(500))
        );
        assert_eq!(rb("status:active"), (None, None)); // field absent
                                                       // Two ranges on the field → intersected (tightest wins).
        assert_eq!(
            rb("ts:[100 TO 900] AND ts:[200 TO 500]"),
            (Some(200), Some(500))
        );
        // A range under OR (should) can't narrow → ignored.
        assert_eq!(rb("ts:[100 TO 500] OR id:x"), (None, None));
    }
}
