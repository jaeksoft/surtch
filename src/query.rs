use reader::FieldReader;
use std::collections::HashMap;
use roaring::bitmap::RoaringBitmap;

pub trait Query {
    fn execute(&self, field_reader: &HashMap<String, FieldReader>) -> RoaringBitmap;
}

pub struct TermQuery {
    field: String,
    term: String,
}

impl Query for TermQuery {
    fn execute(&self, field_reader: &HashMap<String, FieldReader>) -> RoaringBitmap { return RoaringBitmap::new(); }
}

pub enum Occur {
    Filter,
    Must,
    MustNot,
    Should,
}

pub struct BooleanClause {
    query: Box<Query>,
    occur: Occur,
}

pub struct BooleanQuery {
    clauses: Vec<BooleanClause>,
    min_should_match: u16,
}

impl Query for BooleanQuery {
    fn execute(&self, field_reader: &HashMap<String, FieldReader>) -> RoaringBitmap { return RoaringBitmap::new(); }
}

impl BooleanQuery {
    pub fn new(min_should_match: u16) -> BooleanQuery {
        return BooleanQuery { clauses: Vec::new(), min_should_match };
    }

    fn push(&mut self, query: Box<Query>, occur: Occur) -> &mut BooleanQuery {
        self.clauses.push(BooleanClause { query, occur });
        return self;
    }

    pub fn term(&mut self, field: &str, term: &str, occur: Occur) -> &mut BooleanQuery {
        return self.push(Box::new(TermQuery { field: field.to_string(), term: field.to_string() }), occur);
    }

    pub fn boolean(&mut self, query: BooleanQuery, occur: Occur) -> &mut BooleanQuery {
        return self.push(Box::new(query), occur);
    }
}