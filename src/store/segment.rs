use super::{index::Index, log::Log};

struct Segment {
    start_offset: usize,
    last_offset: usize,
    log: Log,
    index: Index,
}

impl Segment {}
