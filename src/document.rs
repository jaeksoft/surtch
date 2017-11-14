pub mod document {
    use terms::terms::Terms;
    use std::collections::HashMap;

    pub struct Document { pub fields: HashMap<String, Terms> }

    impl Document {
        pub fn new() -> Document {
            return Document { fields: HashMap::new() };
        }

        pub fn field(&mut self, field_name: &str) -> &mut Terms {
            return self.fields.entry(field_name.to_string()).or_insert(Terms::new());
        }
    }
}