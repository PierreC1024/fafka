pub mod index;
pub mod log;
pub mod segment;

use std::fs::{File, OpenOptions};
use std::path::PathBuf;

use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::io::{Error, ErrorKind, Result};

use std::sync::{Arc, Mutex};

#[derive(Debug)]
struct Store {
    file: File,
    max_size: usize,
    writer: Arc<Mutex<BufWriter<File>>>,
    reader: BufReader<File>,
}

impl Store {
    pub fn new(path: PathBuf, max_size: usize) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let mut writer = BufWriter::new(file.try_clone()?);
        writer.seek(SeekFrom::End(0))?;

        Ok(Self {
            file: file.try_clone()?,
            max_size,
            writer: Arc::new(Mutex::new(writer)),
            reader: BufReader::new(file),
        })
    }

    pub fn append(&self, buf: &[u8]) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        let position = writer.seek(SeekFrom::Current(0))? as usize;
        if position + buf.len() >= self.max_size {
            Err(Error::new(ErrorKind::UnexpectedEof, ""))
        } else {
            writer.write_all(buf)
        }
    }

    pub fn flush(&self) -> std::io::Result<()> {
        let mut writer = self.writer.lock().unwrap();
        writer.flush()
    }

    pub fn read(&mut self, offset: usize, size: usize) -> std::io::Result<Vec<u8>> {
        if offset + size >= self.max_size {
            Err(Error::new(ErrorKind::UnexpectedEof, ""))
        } else {
            let mut buf = vec![0u8; size];
            self.reader.seek(SeekFrom::Start(offset as u64))?;
            let n = self.reader.read(&mut buf)?;
            buf.truncate(n);
            Ok(buf)
        }
    }

    pub fn read_all(&mut self) -> Result<Vec<u8>> {
        let mut buf = vec![];
        self.reader.seek(SeekFrom::Start(0))?;
        let n = self.reader.read_to_end(&mut buf)?;
        buf.truncate(n);
        Ok(buf)
    }
}
