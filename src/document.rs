use std::collections::BTreeMap;
use std::collections::HashMap;

///
/// This struct represents an indexable document.
/// An indexable document is a set of `Terms` grouped by field.
///
pub struct Document { pub fields: HashMap<String, Terms> }

impl Document {
    pub fn new() -> Document {
        return Document { fields: HashMap::new() };
    }

    pub fn field(&mut self, field_name: &str) -> &mut Terms {
        return self.fields.entry(field_name.to_string()).or_insert_with(|| Terms::new());
    }
}

///
/// A `Terms` structure is a map of terms associated to a list of positions.
/// A position is the location/index of the term in the original document.
///
pub struct Terms {
    pub term_positions: BTreeMap<String, Vec<u32>>,
}

impl Terms {
    fn new() -> Terms {
        return Terms { term_positions: BTreeMap::new() };
    }

    pub fn term(&mut self, term: &str, position: u32) -> &mut Terms {
        self.term_positions.entry(term.to_string()).or_insert_with(|| Vec::new()).push(position);
        return self;
    }
}