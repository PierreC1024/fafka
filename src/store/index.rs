use std::fs::{File, OpenOptions};
use std::path::PathBuf;

use std::collections::HashMap;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::io::{Error, ErrorKind, Result};

use std::sync::{Arc, Mutex};

use bincode;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct Index {
    file: File,
    // Start offset of the index file
    start_offset: usize,
    // Performs synchronous writes to the index file
    writer: Arc<Mutex<BufWriter<File>>>,

    entries: HashMap<usize, Entry>,
}

impl Index {
    pub fn new(path: PathBuf, start_offset: usize, max_size: usize) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.join(format!("{}.index", start_offset)))?;

        let mut writer = BufWriter::new(file.try_clone()?);
        writer.seek(SeekFrom::End(0))?;

        Ok(Self {
            file: file.try_clone()?,
            start_offset,
            writer: Arc::new(Mutex::new(writer)),
            entries: HashMap::new(),
        })
    }

    pub fn append(&self, entries: Vec<Entry>) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        // writer.write_all(buf.as_slice())
        Ok(())
    }

    pub fn flush(&self) -> std::io::Result<()> {
        let mut writer = self.writer.lock().unwrap();
        writer.flush()
    }

    pub fn load(&mut self) -> Result<()> {
        let mut bytes = vec![];
        self.file.read_to_end(&mut bytes)?;
        let n = bytes.len();
        Ok(())
    }
}

const ENTRY_SIZE: usize = 64;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    pub offset: u32,
    pub size: u32,
}

impl Entry {
    pub fn new(offset: u32, size: u32) -> Self {
        Self { offset, size }
    }
}
