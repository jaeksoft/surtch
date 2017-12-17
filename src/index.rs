use std::collections::HashMap;
use reader::FieldReader;
use document::Document;
use std::path::Path;
use std::fs;
use uuid::Uuid;
use writer::SegmentWriter;
use fst::Result;

pub struct Index {
    /// The path of the index directory
    pub path: String,
    /// The available catalogs
    fields: HashMap<String, FieldReader>,
}

impl Index {
    /// Open an existing index, or create a new one.
    pub fn open(index_path: &str) -> Result<Index> {
        let p = Path::new(index_path);
        if !p.exists() {
            fs::create_dir(p)?
        }
        // Read the fields
        let mut fields = HashMap::new();
        for entry in fs::read_dir(p)? {
            let dir_entry = entry?;
            if dir_entry.file_type()?.is_dir() {
                let field_name = dir_entry.file_name().into_string().unwrap();
                fields.insert(field_name.to_string(), FieldReader::open(&index_path, &field_name)?);
            }
        }
        return Ok(Index { path: index_path.to_string(), fields });
    }

    ///
    /// Create a new segment which will contains all the documents
    ///
    pub fn put(&self, documents: &Vec<Document>) -> Result<()> {
        SegmentWriter::index(&self.path, documents)?;
        return Ok({});
    }
}
