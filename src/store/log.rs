use std::path::PathBuf;

use std::io::Result;

use super::Store;

#[derive(Debug)]
pub struct Log {
    // Index of the log file
    start_offset: u32,
    store: Store,
}

impl Log {
    pub fn new(path: PathBuf, start_offset: u32, max_size: u32) -> Result<Self> {
        Ok(Self {
            start_offset,
            store: Store::new(path.join(format!("{}.log", start_offset)), max_size)?,
        })
    }

    pub fn append(&self, buf: &[u8]) -> Result<()> {
        self.store.append(buf)
    }

    pub fn flush(&self) -> std::io::Result<()> {
        self.store.flush()
    }

    pub fn read(&mut self, start: u32, size: u32) -> std::io::Result<Vec<u8>> {
        self.store.read(start, size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    use tempfile::tempdir;
    use test::{black_box, Bencher};

    use std::io::{BufReader, Read, Seek, SeekFrom};

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
        log.flush().unwrap();

        let mut buf = BufReader::new(log.store.file);
        buf.seek(SeekFrom::Start(0)).unwrap();

        let mut written = vec![];
        buf.read_to_end(&mut written).unwrap();
        assert_eq!(seq, written);
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
        log.flush().unwrap();
        assert_eq!(log.read(0, 255).unwrap(), seq)
    }

    #[test]
    #[should_panic]
    fn test_greedy_read() {
        let tmp_dir = create_tmp_folder();
        let mut log = Log::new(tmp_dir, 0, 256).unwrap();
        let seq: Vec<u8> = (0_u8..255_u8).collect();

        log.append(seq.as_slice()).unwrap();
        log.flush().unwrap();
        log.read(555, 1).unwrap();
    }

    #[bench]
    fn bench_write(b: &mut Bencher) {
        let tmp_dir = create_tmp_folder();
        let log = Log::new(tmp_dir, 0, 1024e+9 as u32).unwrap();

        let seq: Vec<u8> = vec![255_u8; 2048];

        b.iter(|| {
            // Inner closure, the actual test
            for _ in 1..1000 {
                black_box(log.append(seq.as_slice()).unwrap());
            }
            log.flush()
        });
    }

    #[bench]
    fn bench_read(b: &mut Bencher) {
        let tmp_dir = create_tmp_folder();
        let mut log = Log::new(tmp_dir, 0, 1024e+9 as u32).unwrap();

        let seq: Vec<u8> = vec![255_u8; 2048000];
        log.append(seq.as_slice()).unwrap();
        log.flush().unwrap();

        b.iter(|| {
            // Inner closure, the actual test
            for i in 1..1000 {
                black_box(log.read(2048 * i, 256).unwrap());
            }
        });
    }
}
