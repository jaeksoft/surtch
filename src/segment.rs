pub mod segment {
    use std::collections::HashMap;
    use std::io;
    use field::field::Field;

    pub struct Segment {
        name: String,
        fields: HashMap<String, Field>
    }

    impl Segment {
        pub fn new(name: &str) -> io::Result<Segment> {
            //TODO load fields
            return Ok(Segment { name: name.to_string(), fields: HashMap::new() });
        }
    }
}