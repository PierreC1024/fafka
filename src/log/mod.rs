use std::fs::{File, OpenOptions};
use std::path::PathBuf;

use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::io::{Error, ErrorKind, Result};

use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct Log {
    // File Descriptor
    file: File,

    // Index of the log file
    start_offset: usize,

    // Maximum file size in bytes
    max_size: usize,

    // Performs synchronous writes to the log file
    writer: Arc<Mutex<BufWriter<File>>>,

    // Performs reads to the log file
    reader: BufReader<File>,
}

impl Log {
    pub fn new(path: PathBuf, start_offset: usize, max_size: usize) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path.join(format!("{}.log", start_offset)))?;

        let mut writer = BufWriter::new(file.try_clone()?);
        writer.seek(SeekFrom::End(0))?;

        Ok(Self {
            file: file.try_clone()?,
            start_offset,
            max_size,
            writer: Arc::new(Mutex::new(writer)),
            reader: BufReader::new(file),
        })
    }

    pub fn append(&self, buf: &[u8]) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        let position = writer.seek(SeekFrom::Current(0))? as usize;
        println!("{}", position);
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    fn create_tmp_folder() -> PathBuf {
        let tmp_dir = tempdir().unwrap().path().to_owned();
        fs::create_dir_all(tmp_dir.clone()).unwrap();
        tmp_dir
    }

    #[test]
    fn test_create() {
        let tmp_dir = create_tmp_folder();

        for index in 0..5 {
            let expected_file = tmp_dir.clone().join(format!("{}.log", index));
            let log = Log::new(tmp_dir.clone(), index, 1).unwrap();

            assert!(expected_file.as_path().exists());
            assert_eq!(log.start_offset, index);
        }
    }

    #[test]
    fn test_write() {
        let tmp_dir = create_tmp_folder();
        let log = Log::new(tmp_dir, 0, 2048).unwrap();

        let seq: Vec<u8> = (0_u8..255_u8).collect();
        log.append(seq.as_slice()).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_greedy_write() {
        let tmp_dir = create_tmp_folder();
        let log = Log::new(tmp_dir, 0, 100).unwrap();

        let seq: Vec<u8> = (0_u8..255_u8).collect();
        log.append(seq.as_slice()).unwrap();
    }

    #[test]
    fn test_read() {
        let tmp_dir = create_tmp_folder();
        let mut log = Log::new(tmp_dir, 0, 2048).unwrap();
        let seq: Vec<u8> = (0_u8..255_u8).collect();

        log.append(seq.as_slice()).unwrap();
        log.flush();
        assert_eq!(log.read(0, 255).unwrap(), seq)
    }

    #[test]
    #[should_panic]
    fn test_greedy_read() {
        let tmp_dir = create_tmp_folder();
        let mut log = Log::new(tmp_dir, 0, 256).unwrap();
        let seq: Vec<u8> = (0_u8..255_u8).collect();

        log.append(seq.as_slice()).unwrap();
        log.flush();
        log.read(555, 1).unwrap();
    }
}
