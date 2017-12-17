use std::collections::HashMap;
use uuid::Uuid;
use std::path::PathBuf;
use std::fs;
use fst::Result;
use fst::Map;
use std::io;
use std::mem;
use std::fs::File;
use snap;
use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use roaring::bitmap::RoaringBitmap;

pub struct FieldReader {
    fields: HashMap<Uuid, SegmentReader>,
}

impl FieldReader {
    pub fn open(index_path: &str, field_name: &str) -> Result<FieldReader> {
        let mut fields: HashMap<Uuid, SegmentReader> = HashMap::new();
        let field_path: PathBuf = [index_path, field_name].iter().collect();
        // Load segments
        for entry in fs::read_dir(field_path)? {
            let dir_entry = entry?;
            if dir_entry.file_type()?.is_dir() {
                let segment_name = dir_entry.file_name().into_string().unwrap();
                let segment_uuid = Uuid::parse_str(&segment_name).unwrap();
                fields.insert(segment_uuid, SegmentReader::open(index_path, field_name, &segment_name)?);
            }
        }
        return Ok(FieldReader { fields });
    }
}

struct SegmentReader {
    fst_map: Map,
    term_docs: HashMap<u32, RoaringBitmap>,
}

impl SegmentReader {
    fn open(index_path: &str, field_name: &str, segment_name: &str) -> Result<SegmentReader> {
        /// Load FST
        let fst_path: PathBuf = [index_path, field_name, segment_name, "fst"].iter().collect();
        let fst_map = Map::from_path(fst_path)?;

        /// Load Term/Docs
        let term_docs = SegmentReader::read_term_docs(index_path, field_name, segment_name, fst_map.len() as u32)?;
        return Ok(SegmentReader { fst_map, term_docs });
    }

    ///
    /// Create a new reader for a file in the segment
    ///
    fn read_term_docs(index_path: &str, field_name: &str, segment_name: &str, term_count: u32) -> Result<HashMap<u32, RoaringBitmap>> {
        // Prepare dox buffer
        let dox_path: PathBuf = [index_path, field_name, segment_name, "dox"].iter().collect();
        let mut dox_reader: snap::Reader<io::BufReader<File>> = snap::Reader::new(io::BufReader::new(File::open(&dox_path)?));

        // Prepare docs buffer
        let docs_path: PathBuf = [index_path, field_name, segment_name, "docs"].iter().collect();
        let mut docs_reader: snap::Reader<io::BufReader<File>> = snap::Reader::new(io::BufReader::new(File::open(&docs_path)?));

        //let mut docs_reader = io::BufReader::new(File::open(&docs_path)?);

        let mut term_docs: HashMap<u32, RoaringBitmap> = HashMap::new();

        // Read the docs
        for n in 0..term_count {
            let size: u32 = dox_reader.read_u32::<LittleEndian>()?;
            //Read the bitset bytes
            let mut buffer: Vec<u8> = Vec::with_capacity(size as usize);
            for m in 0..size as u32 {
                buffer.push(docs_reader.read_u8()?);
            }
            let bitset = RoaringBitmap::deserialize_from(&buffer[..])?;
            term_docs.insert(n, bitset);
        }
        return Ok({ term_docs });
    }
}