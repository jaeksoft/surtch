pub mod terms {
    use std::collections::BTreeMap;

    pub struct Terms {
        pub term_positions: BTreeMap<String, Vec<i32>>
    }

    impl Terms {
        pub fn new() -> Terms {
            return Terms { term_positions: BTreeMap::new() };
        }

        pub fn term(&mut self, term: &str, position: i32) -> &mut Terms {
            self.term_positions.entry(term.to_string()).or_insert(Vec::new()).push(position);
            return self;
        }
    }
}