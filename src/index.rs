use std::collections::HashMap;
use catalog::Catalog;
use document::Document;
use std::path::Path;
use std::fs;
use uuid::Uuid;
use fieldwriter::SegmentWriter;
use fst::Result;

pub struct Index {
    /// The path of the index directory
    pub path: String,
    /// The available catalogs
    catalogs: HashMap<Uuid, Catalog>,
}

impl Index {
    /// Open an existing index, or create a new one.
    pub fn new(index_path: &str) -> Result<Index> {
        let p = Path::new(index_path);
        if !p.exists() {
            fs::create_dir(p)?
        }
        // Read the existing segments
        let segments = HashMap::new();
        //for entry in fs::read_dir(p)? {
        //    let dir_entry = entry?;
        //    if dir_entry.file_type()?.is_dir() {
        //        let dir_name = dir_entry.file_name().into_string().unwrap();
        //        segments.insert(dir_name.to_string(), Segment::new(dir_name.as_ref())?);
        //    }
        //}
        return Ok(Index { path: index_path.to_string(), catalogs: segments });
    }

    ///
    /// Create a new segment which will contains all the documents
    ///
    pub fn put(&self, documents: &Vec<Document>) -> Result<()> {
        SegmentWriter::index(&self.path, documents)?;
        return Ok({});
    }
}
