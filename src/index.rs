pub mod index {
    use std::collections::{HashMap, BTreeMap};
    use segment::segment::Segment;
    use document::document::Document;
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
    use roaring::bitmap::RoaringBitmap;
    use byteorder::{LittleEndian, WriteBytesExt};
    use conv::*;

    pub struct Index {
        pub path: String,
        segments: HashMap<String, Segment>
    }

    /// For one term, the documents ids and the positions in the current field
    struct TermInfos { doc_ids: RoaringBitmap, positions: Vec<Vec<u32>> }

    /// Per field TermInfos
    type TermMap = BTreeMap<String, TermInfos>;

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
        /// Create a new segment directory. The name of the segment is a new UUID v1.
        ///
        fn new_segment(&self) -> Result<String> {
            let ctx = UuidV1Context::new(42);
            let v1uuid = Uuid::new_v1(&ctx, time::precise_time_s() as u64, time::precise_time_ns() as u32, &[1, 2, 3, 4, 5, 6]).unwrap();
            let segment_name = v1uuid.hyphenated().to_string();
            // let index_path = &self.path;
            let segment_path: PathBuf = [&self.path, &segment_name].iter().collect();
            fs::create_dir(segment_path)?;
            return Ok({ segment_name });
        }

        ///
        /// Create a new writer for a file in the segment
        ///
        fn new_segment_writer(&self, segment_name: &str, field_name: &str, extension: &str) -> Result<io::BufWriter<File>> {
            let file_name = field_name.to_string() + extension;
            let file_path: PathBuf = [&self.path, segment_name, &file_name].iter().collect();
            return Ok(io::BufWriter::new(File::create(file_path)?));
        }

        ///
        /// Create a new segment which will contains all the documents
        ///
        pub fn create_segment(&self, documents: &Vec<Document>) -> Result<()> {
            // Prepare the segment
            let segment_name = self.new_segment()?;

            let mut field_infos: BTreeMap<String, TermMap> = BTreeMap::new();

            let mut doc_num: u32 = 0;

            // Document Loop
            for document in documents {
                // Fields loop
                for (field, terms) in &document.fields {
                    let field_info_builder = field_infos.entry(field.to_string()).or_insert(BTreeMap::new());
                    // Terms loop
                    for (term, positions) in terms.term_positions.iter() {
                        let term_infos = field_info_builder.entry(term.to_string()).or_insert(TermInfos {
                            doc_ids: RoaringBitmap::new(),
                            positions: Vec::new()
                        });
                        term_infos.doc_ids.insert(doc_num);
                        term_infos.positions.push(positions.clone());
                    }
                }
                doc_num += 1;
            }

            for (field, term_map) in field_infos {
                let mut fst_builder = MapBuilder::new(self.new_segment_writer(&segment_name, &field, ".fst")?)?;
                let mut dox_writer = snap::Writer::new(self.new_segment_writer(&segment_name, &field, ".dox")?);
                let mut docs_writer = snap::Writer::new(self.new_segment_writer(&segment_name, &field, ".docs")?);
                let mut pox_writer = snap::Writer::new(self.new_segment_writer(&segment_name, &field, ".pox")?);
                let mut posx_writer = snap::Writer::new(self.new_segment_writer(&segment_name, &field, ".posx")?);
                let mut posi_writer = snap::Writer::new(self.new_segment_writer(&segment_name, &field, ".posi")?);

                let mut term_idx: u64 = 0;
                let mut docs_offset: u32 = 0;
                let mut posx_offset: u32 = 0;
                let mut posi_offset: u32 = 0;
                for (term, term_infos) in term_map {
                    // Write FST
                    fst_builder.insert(term, term_idx)?;

                    // Write DOX
                    dox_writer.write_u32::<LittleEndian>(docs_offset)?;

                    //Write DOCS bitset
                    let rb: RoaringBitmap = term_infos.doc_ids;
                    let rb_size: u32 = u32::value_from(rb.serialized_size()).unwrap();
                    docs_writer.write_u32::<LittleEndian>(rb_size)?;
                    rb.serialize_into(&mut docs_writer)?;
                    docs_offset += 4 + rb_size;

                    //Write POX
                    pox_writer.write_u32::<LittleEndian>(posx_offset)?;
                    for positions in term_infos.positions {
                        //println!("IDX: {} - DOX: {} - POSX: {} - POSI: {}", term_idx, docs_offset, posx_offset, posi_offset);
                        //Write POSX = current position offset and positions length
                        posx_writer.write_u32::<LittleEndian>(posi_offset)?;
                        posx_writer.write_u32::<LittleEndian>(u32::value_from(positions.len()).unwrap())?;
                        posx_offset += 8;
                        // Write positions
                        for position in positions {
                            posi_writer.write_u32::<LittleEndian>(position)?;
                            posi_offset += 4;
                        }
                    }
                    term_idx += 1;
                }

                fst_builder.finish()?;
                posx_writer.flush()?;
                posi_writer.flush()?;
                pox_writer.flush()?;
                docs_writer.flush()?;
                dox_writer.flush()?;
            }
            return Ok({});
        }
    }
}
