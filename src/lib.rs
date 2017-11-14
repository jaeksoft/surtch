extern crate fst;

use std::collections::HashMap;

pub mod field;
pub mod segment;
pub mod index;
pub mod terms;
pub mod document;

use index::index::Index;

#[cfg(test)]
mod tests;