use std::collections::HashMap;
use uuid::Uuid;
use std::path::PathBuf;
use std::fs;
use fst::Result;
use fst::Map;
use std::io::BufReader;
use std::fs::File;
use std::str::Split;
use snap;
use byteorder::{LittleEndian, ReadBytesExt};
use roaring::bitmap::RoaringBitmap;

///
/// A FieldReader is in charge of maintaining a list of segments in memory.
/// The segments are stored in a directory which contains the segments.
///
pub struct FieldReader {
    field_path: PathBuf,
    segments: HashMap<Uuid, SegmentReader>,
    pub record_count: u64,
}

impl FieldReader {
    pub fn open(field_path: PathBuf) -> Result<FieldReader> {
        let segments: HashMap<Uuid, SegmentReader> = HashMap::new();
        let field_reader = FieldReader { field_path, segments, record_count: 0 };
        return Ok(field_reader);
    }

    ///
    /// Parse the field directory and load the segments from the file system.
    /// It can be called again to load only the missing segments.
    /// Already loaded segments are not loaded again.
    ///
    pub fn reload(&mut self) -> Result<()> {
        let mut record_count: u64 = 0;
        for entry in fs::read_dir(&self.field_path)? {
            let dir_entry = entry?;
            if dir_entry.file_type()?.is_dir() {
                let segment_name = dir_entry.file_name().into_string().unwrap();
                let mut split = segment_name.split(".");
                let segment_uuid = Uuid::parse_str(split.next().unwrap()).unwrap();
                let segment_reader = self.segments.entry(segment_uuid).or_insert_with(|| {
                    let segment_offset_str = split.next().unwrap();
                    let segment_offset: u64 = segment_offset_str.parse().unwrap();
                    let segment_doc_count_str = split.next().unwrap();
                    let segment_doc_count: u32 = segment_doc_count_str.parse().unwrap();
                    return SegmentReader::open(dir_entry.path(), segment_offset, segment_doc_count).unwrap();
                });
                let new_record_count = segment_reader.offset + segment_reader.doc_count as u64;
                if (new_record_count > record_count) {
                    record_count = new_record_count;
                }
            }
        }
        self.record_count = record_count;
        return Ok({});
    }
}

struct SegmentReader {
    fst_map: Map,
    term_docs: HashMap<u32, RoaringBitmap>,
    offset: u64,
    doc_count: u32,
}

impl SegmentReader {
    fn open(segment_path: PathBuf, offset: u64, doc_count: u32) -> Result<SegmentReader> {
        println!("Load segment: {}", segment_path.to_str().unwrap());
        // Load FST
        let mut fst_path: PathBuf = segment_path.to_path_buf();
        fst_path.push("fst");
        let fst_map = unsafe { Map::from_path(fst_path)? };

        // Load Term/Docs
        let term_docs = SegmentReader::read_term_docs(segment_path, fst_map.len() as u32)?;
        return Ok(SegmentReader { fst_map, term_docs, offset, doc_count });
    }

    ///
    /// Create a new reader for a file in the segment
    ///
    fn read_term_docs(segment_path: PathBuf, term_count: u32) -> Result<HashMap<u32, RoaringBitmap>> {
        // Prepare dox buffer
        let mut dox_path: PathBuf = segment_path.to_path_buf();
        dox_path.push("dox");
        let mut dox_reader: snap::Reader<File> = snap::Reader::new(File::open(&dox_path)?);

        // Prepare docs buffer
        let mut docs_path: PathBuf = segment_path.to_path_buf();
        docs_path.push("docs");
        let mut docs_reader: BufReader<File> = BufReader::new(File::open(&docs_path)?);

        let mut term_docs: HashMap<u32, RoaringBitmap> = HashMap::new();

        // Read the docs
        for n in 0..term_count {
            let size: u32 = dox_reader.read_u32::<LittleEndian>()?;
            //Read the bitset bytes
            let mut buffer: Vec<u8> = Vec::with_capacity(size as usize);
            for i in 0..size {
                buffer.push(docs_reader.read_u8()?);
            }
            let bitset = RoaringBitmap::deserialize_from(&buffer[..])?;
            term_docs.insert(n, bitset);
        }
        return Ok({ term_docs });
    }
}