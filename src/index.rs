pub mod index {
    use std::collections::{HashMap, BTreeMap};
    use segment::segment::Segment;
    use document::document::{Document, Terms};
    use std::io;
    use std::io::Write;
    use std::io::BufWriter;
    use std::path::Path;
    use std::path::PathBuf;
    use std::fs;
    use std::fs::File;
    use uuid::{Uuid, UuidV1Context};
    use fst::{MapBuilder, Result};
    use time;
    use snap;
    use bincode::{serialize, deserialize, Infinite};
    use roaring::bitmap::RoaringBitmap;

    pub struct Index {
        pub path: String,
        segments: HashMap<String, Segment>
    }

    struct SegmentBuilder {
        index_path: String,
        segment_name: String,
        field_infos: BTreeMap<String, FieldInfosBuilder>,
        doc_num: u32
    }

    ///
    /// There is exactly one structure of FstFieldBuilder par term, per field, in a segment
    ///
    struct FieldInfosBuilder {
        field_name: String,
        fst_path: PathBuf,
        dox_path: PathBuf,
        doc_path: PathBuf,
        pox_path: PathBuf,
        pos_path: PathBuf,
        terms_infos: BTreeMap<String, TermInfos>
    }

    struct TermInfos {
        doc_ids: RoaringBitmap,
        positions: Vec<Vec<u32>>
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

        ///
        /// Create a new segment which will contains all the documents
        ///
        pub fn insert(&self, documents: &Vec<Document>) -> Result<()> {
            let mut segment_builder = SegmentBuilder::new(&self.path)?;
            segment_builder.insert(documents)?;
            segment_builder.finish()?;
            return Ok({});
        }
    }

    impl SegmentBuilder {
        ///
        /// Create a new segment directory. The name of the segment is a new UUID v1.
        ///
        fn new(index_path: &str) -> io::Result<SegmentBuilder> {
            let ctx = UuidV1Context::new(42);
            let v1uuid = Uuid::new_v1(&ctx, time::precise_time_s() as u64, time::precise_time_ns() as u32, &[1, 2, 3, 4, 5, 6]).unwrap();
            let segment_name = v1uuid.hyphenated().to_string();
            let segment_path: PathBuf = [index_path, &segment_name].iter().collect();
            fs::create_dir(segment_path)?;
            return Ok(SegmentBuilder {
                index_path: index_path.to_string(),
                segment_name,
                field_infos: BTreeMap::new(),
                doc_num: 0
            });
        }

        ///
        /// Insert a collection of document in the segment
        ///
        fn insert(&mut self, documents: &Vec<Document>) -> Result<()> {
            // Document Loop
            for document in documents {
                for (field, terms) in &document.fields {
                    self.field_infos.entry(field.to_string()).or_insert(FieldInfosBuilder::new(&self.index_path, &self.segment_name, field)?).insert(self.doc_num, terms);
                }
                self.doc_num += 1;
            }
            return Ok({});
        }

        ///
        /// Complete the indexing (flush)
        ///
        fn finish(&mut self) -> Result<()> {
            for fieldInfosBuilder in self.field_infos.values_mut() {
                fieldInfosBuilder.finish()?;
            }
            return Ok({});
        }
    }

    impl FieldInfosBuilder {
        ///
        /// Create a new FstFieldBuilder
        ///
        fn new(index_path: &str, segment_name: &str, field: &str) -> Result<FieldInfosBuilder> {
            // Setup the FST file
            let fst_name = field.to_string() + ".fst";
            let fst_path: PathBuf = [index_path, segment_name, &fst_name].iter().collect();

            // Setup the DOX file
            let dox_name = field.to_string() + ".dox";
            let dox_path: PathBuf = [index_path, segment_name, &dox_name].iter().collect();
            // let mut dox_writer = io::BufWriter::new(File::create(dox_path)?);
            // let mut dox_snap_writer = snap::Writer::new(dox_writer);

            // Setup the DOC file
            let doc_name = field.to_string() + ".doc";
            let doc_path: PathBuf = [index_path, segment_name, &doc_name].iter().collect();
            // let mut doc_writer = io::BufWriter::new(File::create(doc_path)?);
            // let mut doc_snap_writer = snap::Writer::new(doc_writer);

            // Setup the POX file
            let pox_name = field.to_string() + ".pox";
            let pox_path: PathBuf = [index_path, segment_name, &pox_name].iter().collect();
            //  let mut pox_writer = io::BufWriter::new(File::create(pox_path)?);
            // let mut pox_snap_writer = snap::Writer::new(pox_writer);

            // Setup the POS file
            let pos_name = field.to_string() + ".pos";
            let pos_path: PathBuf = [index_path, segment_name, &pos_name].iter().collect();
            // let mut pos_writer = io::BufWriter::new(File::create(pos_path)?);
            // let mut pos_snap_writer = snap::Writer::new(pos_writer);

            return Ok(FieldInfosBuilder { field_name: field.to_string(), fst_path, dox_path, doc_path, pox_path, pos_path, terms_infos: BTreeMap::new() });
        }

        fn insert(&mut self, doc_id: u32, terms: &Terms) -> Result<()> {
            // Write loop
            //let mut term_offset = 0;
            for (term, positions) in terms.term_positions.iter() {
                // Check the bitmap
                self.terms_infos.entry(term.to_string()).or_insert(TermInfos::new()).insert(doc_id, &self.field_name, positions);
                //let encoded: Vec<u8> = serialize(positions, Infinite).unwrap();
                //let usize = self.pos_snap_writer.write(&encoded)?;
                //term_offset += usize;
            }
            return Ok({});
        }

        fn finish(mut self) -> Result<()> {
            let mut fst_writer = io::BufWriter::new(File::create(self.fst_path)?);
            let mut fst_builder = MapBuilder::new(fst_writer)?;

            let mut term_idx = 0;
            for (term, term_infos) in self.terms_infos.iter() {
                fst_builder.insert(term, term_idx);
                term_idx += 1;
            }

            fst_builder.finish()?;

            // self.pox_snap_writer.flush()?;
            // self.pos_snap_writer.flush()?;
            // self.dox_snap_writer.flush()?;
            // self.doc_snap_writer.flush()?;
            return Ok({});
        }
    }

    impl TermInfos {
        fn new() -> TermInfos { return TermInfos { doc_ids: RoaringBitmap::new(), positions: Vec::new() }; }

        fn insert(&mut self, doc_id: u32, field_name: &str, positions: &Vec<u32>) {
            self.doc_ids.insert(doc_id);
            self.positions.push(positions.clone());
        }
    }
}
