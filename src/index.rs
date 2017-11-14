pub mod index {
    use std::collections::HashMap;
    use segment::segment::Segment;
    use terms::terms::Terms;
    use document::document::Document;
    use std::io;
    use std::path::Path;
    use std::path::PathBuf;
    use std::fs;
    use std::fs::File;
    use fst::{IntoStreamer, Streamer, Map, MapBuilder, Result};

    pub struct Index {
        pub path: String,
        segments: HashMap<String, Segment>
    }

    impl Index {
        /// Open an existing index, or create a new one.
        pub fn new(index_path: &str) -> io::Result<Index> {
            let p = Path::new(index_path);
            if !p.exists() {
                fs::create_dir(p)?
            }
            // Read the existing segments
            let mut segments = HashMap::new();
            for entry in fs::read_dir(p)? {
                let dir_entry = entry?;
                if dir_entry.file_type()?.is_dir() {
                    let dir_name = dir_entry.file_name().into_string().unwrap();
                    segments.insert(dir_name.to_string(), Segment::new(dir_name.as_ref())?);
                }
            }
            return Ok(Index { path: index_path.to_string(), segments: segments });
        }

        pub fn insert(&self, documents: &Vec<Document>) -> Result<()> {
            //TODO get next segment number
            for document in documents { self.insert_document(document); }
            return Ok({});
        }

        fn insert_document(&self, document: &Document) -> Result<()> {
            for (field, terms) in &document.fields {
                self.insert_field(1, field.as_ref(), terms)?;
            }
            return Ok({});
        }

        fn insert_field(&self, segment_number: u64, field: &str, terms: &Terms) -> Result<()> {
            let field_fst = field.to_string() + ".fst";
            let field_path: PathBuf = [&self.path, &field_fst].iter().collect();
            let mut wtr = io::BufWriter::new(try!(File::create(field_path)));
            let mut build = try!(MapBuilder::new(wtr));
            let mut pos = 0;
            for (term, positions) in &terms.term_positions {
                build.insert(term, pos)?;
                pos = pos + 1;
            }
            try!(build.finish());
            return Ok({});
        }
    }
}
