use std::path::PathBuf;

use std::collections::HashMap;
use std::io::Result;
use std::io::{BufReader, Read, Seek, SeekFrom};

use bincode;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};

use super::Store;

const ENTRY_SIZE: usize = 8;

#[derive(Eq, Ord, Debug, PartialEq, PartialOrd, Serialize, Deserialize, Clone, Copy)]
pub struct Entry {
    pub offset: u32,
    pub size: u32,
}

impl Entry {
    pub fn new(offset: u32, size: u32) -> Self {
        Self { offset, size }
    }
}

#[derive(Debug)]
pub struct Index {
    start_offset: usize,
    store: Store,
    entries: HashMap<u32, Entry>,
}

// Simple Index File implementation:
// Improvements:
// - Synchronize file with entries
// - Implement a method to load new entries only
// - Parallelize deserialization when readind
impl Index {
    pub fn new(path: PathBuf, start_offset: usize, max_size: usize) -> Result<Self> {
        Ok(Self {
            start_offset,
            store: Store::new(path.join(format!("{}.log", start_offset)), max_size)?,
            entries: HashMap::new(),
        })
    }

    pub fn append(&self, entries: Vec<Entry>) -> Result<()> {
        let buf = entries
            .par_iter()
            .map(|e| bincode::serialize(&e).unwrap())
            .reduce(|| vec![], |a, b| [a, b].concat());

        self.store.append(&buf[..])
    }

    pub fn flush(&self) -> std::io::Result<()> {
        self.store.flush()
    }

    pub fn read(&mut self) -> Result<()> {
        let bytes = self.store.read_all()?;
        let n = bytes.len();
        if n > 0 {
            for i in (0..n).step_by(ENTRY_SIZE) {
                let e: Entry = bincode::deserialize(&bytes[i..i + ENTRY_SIZE]).unwrap();
                self.entries.insert(e.offset, e);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::iter::FromIterator;

    use tempfile::tempdir;

    fn create_tmp_folder() -> PathBuf {
        let tmp_dir = tempdir().unwrap().path().to_owned();
        fs::create_dir_all(tmp_dir.clone()).unwrap();
        tmp_dir
    }

    #[test]
    fn test_write() {
        let tmp_dir = create_tmp_folder();
        let index = Index::new(tmp_dir, 0, 2048).unwrap();

        let indices: Vec<Entry> = vec![Entry::new(0, 1024), Entry::new(1024, 2048)];

        index.append(indices.clone()).unwrap();
        index.flush().unwrap();

        let bytes = indices
            .par_iter()
            .map(|i| bincode::serialize(&i).unwrap())
            .reduce(|| vec![], |a, b| [a, b].concat());

        let mut buf = BufReader::new(index.store.file);
        buf.seek(SeekFrom::Start(0)).unwrap();

        let mut written = vec![];
        buf.read_to_end(&mut written).unwrap();
        assert_eq!(bytes, written);
    }

    #[test]
    fn test_read() {
        let tmp_dir = create_tmp_folder();
        let mut index = Index::new(tmp_dir.clone(), 0, 2048).unwrap();

        let mut indices: Vec<Entry> = vec![Entry::new(0, 1024), Entry::new(1024, 2048)];

        index.append(indices.clone()).unwrap();
        index.flush().unwrap();

        index.read().unwrap();

        let mut read = Vec::from_iter(index.entries.values().map(|a| *a).collect::<Vec<Entry>>());

        read.sort();
        indices.sort();

        assert_eq!(indices, read,);
    }
}
