use std::error::Error;
use std::str::from_utf8;
use std::sync::Arc;

use crate::{
    Metadata,
    DB
};

/// This is an iterator struct that represents the Database main iterator
/// struct.
pub struct DataIter {
    pub data: (sled::Iter, Arc<sled::Tree>)
}

impl Iterator for DataIter {
    type Item = Result<(String, String, Metadata), Box<dyn Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        let data_iter = &mut self.data.0;

        let data = match data_iter.next()? {
            Ok(a) => a,
            Err(e) => {
                return Some(Err(Box::new(e)));
            }
        };

        let (kb, vb) = data;

        let meta_tree = &mut self.data.1;

        let mb = match meta_tree.get(&kb) {
            Ok(a) => a,
            Err(e) => {
                return Some(Err(Box::new(e)));
            }
        }?;

        let key = match from_utf8(&kb) {
            Ok(a) => a,
            Err(e) => {
                return Some(Err(Box::new(e)));
            }
        }
        .to_string();

        let value = match from_utf8(&vb) {
            Ok(a) => a,
            Err(e) => {
                return Some(Err(Box::new(e)));
            }
        }
        .to_string();

        let meta = match Metadata::from_u8(&mb) {
            Ok(a) => a,
            Err(e) => {
                return Some(Err(Box::new(e)));
            }
        };

        Some(Ok((key, value, meta)))
    }
}

impl DB {
    /// This function returns the iterator of the database, which will contain a
    /// key and its corresponding value in each iteration, (key, value).
    pub fn iter(&mut self) -> DataIter {
        DataIter {
            data: (self.data_tree.iter(), self.meta_tree.clone())
        }
    }
}
