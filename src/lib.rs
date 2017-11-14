extern crate fst;
extern crate uuid;
extern crate time;
extern crate snap;
extern crate bincode;

pub mod field;
pub mod segment;
pub mod index;
pub mod document;

use index::index::Index;

#[cfg(test)]
mod tests;