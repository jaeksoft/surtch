pub mod index {
    use std::collections::HashMap;
    use segment::segment::Segment;
    use document::document::{Document, Terms};
    use std::io;
    use std::io::Write;
    use std::path::Path;
    use std::path::PathBuf;
    use std::fs;
    use std::fs::File;
    use uuid::{Uuid, UuidV1Context};
    use fst::{MapBuilder, Result};
    use time;
    use snap;
    use bincode::{serialize, deserialize, Infinite};

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
            let ctx = UuidV1Context::new(42);
            let v1uuid = Uuid::new_v1(&ctx, time::precise_time_s() as u64, time::precise_time_ns() as u32, &[1, 2, 3, 4, 5, 6]).unwrap();
            let segment_name = v1uuid.hyphenated().to_string();
            let segment_path: PathBuf = [&self.path, &segment_name].iter().collect();
            fs::create_dir(segment_path)?;
            for document in documents { self.insert_document(&segment_name, document); }
            return Ok({});
        }

        fn insert_document(&self, segment_name: &str, document: &Document) -> Result<()> {
            for (field, terms) in &document.fields {
                self.insert_field(segment_name, field.as_ref(), terms)?;
            }
            return Ok({});
        }

        fn insert_field(&self, segment_name: &str, field: &str, terms: &Terms) -> Result<()> {
            // Setup the FST file
            let fst_name = field.to_string() + ".fst";
            let fst_path: PathBuf = [&self.path, segment_name, &fst_name].iter().collect();
            let mut fst_writer = io::BufWriter::new(File::create(fst_path)?);
            let mut build = try!(MapBuilder::new(fst_writer));

            // Setup the POS file
            let pos_name = field.to_string() + ".pos";
            let pos_path: PathBuf = [&self.path, segment_name, &pos_name].iter().collect();
            let mut pos_writer = io::BufWriter::new(File::create(pos_path)?);
            let mut snap_writer = snap::Writer::new(pos_writer);

            let mut pos = 0;
            for (term, positions) in terms.term_positions.iter() {
                build.insert(term, pos)?;
                let encoded: Vec<u8> = serialize(positions, Infinite).unwrap();
                let usize = snap_writer.write(&encoded)?;
                pos += 1;
            }
            build.finish()?;
            snap_writer.flush()?;
            return Ok({});
        }
    }
}
