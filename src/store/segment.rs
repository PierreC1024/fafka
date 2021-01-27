use std::cmp;
use std::io::Result;
use std::path::PathBuf;

// use rayon::prelude::*;
use super::{index::Entry, index::Index, log::Log};

struct LogChunk {
    pub entries: Vec<Entry>,
    pub start: u32,
    pub size: u32,
}

struct Segment {
    start_offset: u32,
    log: Log,
    index: Index,
}

impl Segment {
    pub fn new(path: PathBuf, start_offset: u32) -> Result<Self> {
        let log = Log::new(path.clone(), start_offset, 1024e+9 as u32)?;
        let index = Index::new(path.clone(), start_offset, 1024e+9 as u32)?;

        Ok(Self {
            start_offset,
            log,
            index,
        })
    }

    // Not parrallel any failure will lead to data loss as
    // a record might be written in the log but not in the index
    pub fn append(&mut self, index: Vec<Entry>, records: &[u8]) -> Result<()> {
        self.log.append(records)?;
        self.index.append(index)?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.log.flush()?;
        self.index.flush()?;
        Ok(())
    }

    // TODO: Remove mut!
    // Returning a vec might not be optimal. We may need to tweak this
    // function when implementing clients.
    pub fn read(&mut self, from_offset: u32, to_offset: u32) -> Result<(Vec<Entry>, Vec<u8>)> {
        let mut current_offset = from_offset;
        let end_offset = cmp::min(to_offset, self.index.last_offset);
        let mut current_chunk = LogChunk {
            entries: vec![],
            start: 0,
            size: 0,
        };

        let mut chunks: Vec<LogChunk> = vec![];

        while current_offset <= end_offset {
            match self.index.get(current_offset) {
                Some(i) => {
                    if (i.start == current_chunk.start + current_chunk.size)
                        || current_chunk.start == 0 && current_chunk.size == 0
                    {
                        current_chunk.size += i.size;
                        current_chunk.entries.push(*i);
                    } else {
                        chunks.push(current_chunk);
                        current_chunk = LogChunk {
                            entries: vec![],
                            start: i.start,
                            size: i.size,
                        };
                    }
                }
                None => {}
            };
            current_offset += 1;
        }

        chunks.push(current_chunk);
        // TODO fix log.read mutability / or add a way to clone/copy
        // a Log to paralellize!
        // chunks.par_iter().map(|chunk| {
        //     self.log.read(chunk.start, chunk.size).unwrap()
        // });
        let data: Vec<u8> = chunks
            .iter()
            .map(|chunk| self.log.read(chunk.start, chunk.size).unwrap())
            .fold(vec![], |acc, chunk| [acc, chunk].concat());

        // Oh my
        let indices: Vec<Entry> = chunks
            .iter()
            .fold(vec![], |acc, chunk| [acc, chunk.entries.clone()].concat());
        Ok((indices, data))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    use tempfile::tempdir;
    // use test::{black_box, Bencher};

    fn create_tmp_folder() -> PathBuf {
        let tmp_dir = tempdir().unwrap().path().to_owned();
        fs::create_dir_all(tmp_dir.clone()).unwrap();
        tmp_dir
    }

    #[test]
    fn test_create() {
        let tmp_dir = create_tmp_folder();

        for i in 0..5 {
            let expected_file = tmp_dir.clone();
            let segment = Segment::new(tmp_dir.clone(), i).unwrap();

            assert!(expected_file.as_path().exists());
            assert_eq!(segment.start_offset, i);
        }
    }

    #[test]
    fn test_read_write() {
        let tmp_dir = create_tmp_folder();

        let index: Vec<Entry> = vec![
            Entry::new(0, 100, 0),
            Entry::new(1, 102, 100),
            Entry::new(2, 50, 202),
        ];

        let seq = (0_u8..252_u8).collect::<Vec<u8>>();
        let records: &[u8] = seq.as_ref();
        let mut segment = Segment::new(tmp_dir, 0).unwrap();

        segment.append(index.clone(), records).unwrap();
        segment.flush().unwrap();

        let (entries, written) = segment.read(0, 2).unwrap();
        assert_eq!(written, records.to_vec());
        assert_eq!(entries, index);
    }

    //#[bench]
    //fn bench_write(b: &mut Bencher) {
    //}

    //#[bench]
    //fn bench_read(b: &mut Bencher) {
    //}
}
