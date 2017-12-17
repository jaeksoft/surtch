use std::collections::HashMap;
use std::io;
use uuid::Uuid;

pub struct Catalog {
    version: Uuid,
    /// name -> Field
    fields: HashMap<String, String>,
}

impl Catalog {
    pub fn new(version: Uuid) -> io::Result<Catalog> {
        //TODO load fields
        return Ok(Catalog { version, fields: HashMap::new() });
    }

    pub fn add(&mut self, name: String, field: String) {
        self.fields.insert(name, field);
    }
}

// TODO Serialize

// TODO Deserialize