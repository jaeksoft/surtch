use std::collections::BTreeMap;
use uuid::{Uuid, UuidV1Context};
use fst::{MapBuilder, Result};
use std::io::Write;
use std::io::BufWriter;
use std::path::PathBuf;
use std::io;
use std::fs;
use std::fs::File;
use time;
use snap;
use roaring::bitmap::RoaringBitmap;
use byteorder::{LittleEndian, WriteBytesExt};
use document::Document;
use rayon::prelude::*;

/// For one term, the documents ids and the positions in the current field
struct TermInfos { doc_ids: RoaringBitmap, positions: Vec<Vec<u32>> }

impl TermInfos {
    /// Write the bitset
    fn serialize_bitset(&self, write: &mut Write) -> Result<u32> {
        self.doc_ids.serialize_into(write)?;
        return Ok({ self.doc_ids.serialized_size() as u32 });
    }

    fn serialize_positions(&self, mut posx_offset: u32, mut posi_offset: u32, posx_writer: &mut Write, posi_writer: &mut Write) -> Result<(u32, u32)> {
        for positions in self.positions.iter() {
            posx_writer.write_u32::<LittleEndian>(posi_offset)?;
            posx_writer.write_u32::<LittleEndian>(positions.len() as u32)?;
            posx_offset += 8;
            // Write positions
            for position in positions {
                posi_writer.write_u32::<LittleEndian>(*position)?;
                posi_offset += 4;
            }
        }
        return Ok({ (posx_offset, posi_offset) });
    }
}

/// Per term -> TermInfos
type TermMap = BTreeMap<String, TermInfos>;

pub struct SegmentWriter {
    segment_path_temp: PathBuf,
    segment_path_final: PathBuf,
    fst_builder: MapBuilder<BufWriter<File>>,
    dox_writer: snap::Writer<BufWriter<File>>,
    pox_writer: snap::Writer<BufWriter<File>>,
    docs_writer: BufWriter<File>,
    posx_writer: snap::Writer<BufWriter<File>>,
    posi_writer: snap::Writer<BufWriter<File>>,
}

impl SegmentWriter {
    fn new_term_map(documents: &Vec<Document>) -> BTreeMap<String, TermMap> {
        let mut field_infos: BTreeMap<String, TermMap> = BTreeMap::new();

        let mut doc_num: u32 = 0;

        // Document Loop
        for document in documents {
            // Fields loop
            for (field, terms) in &document.fields {
                //TODO Do we really need to_string ?
                let field_info_builder = field_infos.entry(field.to_string()).or_insert_with(|| BTreeMap::new());
                // Terms loop
                for (term, positions) in terms.term_positions.iter() {
                    //TODO Do we really need to_string ?
                    if !field_info_builder.contains_key(term) {
                        let term_infos = field_info_builder.entry(term.to_string()).or_insert_with(|| TermInfos {
                            doc_ids: RoaringBitmap::new(),
                            positions: Vec::new(),
                        });
                        term_infos.doc_ids.insert(doc_num);
                        term_infos.positions.push(positions.clone());
                    }
                }
            }
            doc_num += 1;
        }
        return field_infos;
    }

    pub fn index(index_path: &str, documents: &Vec<Document>) -> Result<()> {
        println!("Index {} document(s)", documents.len());
        // Create the segment/transaction id
        let ctx = UuidV1Context::new(42);
        let v1uuid = Uuid::new_v1(&ctx, time::precise_time_s() as u64, time::precise_time_ns() as u32, &[1, 2, 3, 4, 5, 6]).unwrap();
        let segment_name: String = v1uuid.hyphenated().to_string();

        let mut field_infos = SegmentWriter::new_term_map(documents);

        field_infos.par_iter_mut().for_each(|(field_name, term_map)| {
            let mut segment_writer = SegmentWriter::new(index_path, &field_name, &segment_name).unwrap();
            segment_writer.index_terms(term_map).unwrap();
            segment_writer.finish().unwrap();
        });
        return Ok({});
    }

    ///
    /// Create new SegmentWriter
    ///
    fn new(index_path: &str, field_name: &str, segment_name: &str) -> Result<SegmentWriter> {
        // Create the directory
        let segment_name_temp = segment_name.to_string() + ".temp";
        let segment_path_temp: PathBuf = [index_path, field_name, &segment_name_temp].iter().collect();
        let segment_path_final: PathBuf = [index_path, field_name, segment_name].iter().collect();

        fs::create_dir_all(&segment_path_temp)?;
        // Create the writers
        let fst_builder = MapBuilder::new(SegmentWriter::new_file_writer(index_path, field_name, &segment_name_temp, "fst")?)?;
        let dox_writer = snap::Writer::new(SegmentWriter::new_file_writer(index_path, field_name, &segment_name_temp, "dox")?);
        let docs_writer = SegmentWriter::new_file_writer(index_path, field_name, &segment_name_temp, "docs")?;
        let pox_writer = snap::Writer::new(SegmentWriter::new_file_writer(index_path, field_name, &segment_name_temp, "pox")?);
        let posx_writer = snap::Writer::new(SegmentWriter::new_file_writer(index_path, field_name, &segment_name_temp, "posx")?);
        let posi_writer = snap::Writer::new(SegmentWriter::new_file_writer(index_path, field_name, &segment_name_temp, "posi")?);
        return Ok(SegmentWriter { segment_path_temp, segment_path_final, fst_builder, dox_writer, docs_writer, pox_writer, posx_writer, posi_writer });
    }

    ///
    /// Create a new writer for a file in the segment
    ///
    pub fn new_file_writer(index_path: &str, field_name: &str, segment_name: &str, file_name: &str) -> io::Result<io::BufWriter<File>> {
        let file_path: PathBuf = [index_path, field_name, segment_name, &file_name].iter().collect();
        return Ok(BufWriter::new(File::create(file_path)?));
    }

    fn index_terms(&mut self, term_map: &TermMap) -> Result<()> {
        let mut term_idx: u64 = 0;
        let mut posx_offset: u32 = 0;
        let mut posi_offset: u32 = 0;
        for (term, term_infos) in term_map.iter() {
            // Write FST
            self.fst_builder.insert(&term, term_idx)?;

            //Write DOCS bitset
            let rb_size: u32 = term_infos.serialize_bitset(&mut self.docs_writer)?;
            // Write DOX -> offset to bitset AND size of the serialized RoaringBitmap
            self.dox_writer.write_u32::<LittleEndian>(rb_size)?;

            //Write POX
            self.pox_writer.write_u32::<LittleEndian>(posx_offset)?;
            let r = term_infos.serialize_positions(posx_offset, posi_offset, &mut self.posx_writer, &mut self.posi_writer)?;
            posx_offset = r.0;
            posi_offset = r.1;
            term_idx += 1;
        }
        return Ok({});
    }


    ///
    /// Close and flush the file writers
    ///
    pub fn finish(mut self) -> Result<()> {
        self.fst_builder.finish()?;
        self.posx_writer.flush()?;
        self.posi_writer.flush()?;
        self.pox_writer.flush()?;
        self.docs_writer.flush()?;
        self.dox_writer.flush()?;
        fs::rename(self.segment_path_temp, self.segment_path_final)?;
        return Ok({});
    }
}