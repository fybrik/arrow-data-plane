#[repr(C)]
#[derive(Debug)]
pub(crate) struct FFI32_ArrowArray {
    pub(crate) length: i64,
    pub(crate) null_count: i64,
    pub(crate) offset: i64,
    pub(crate) n_buffers: i64,
    pub(crate) n_children: i64,
    pub(crate) buffers: u32,
    children: u32,
    dictionary: u32,
    release: Option<u32>,
    private_data: u32,
}

#[repr(C)]
#[derive(Debug)]
pub(crate) struct FFI64_ArrowArray {
    pub(crate) length: i64,
    pub(crate) null_count: i64,
    pub(crate) offset: i64,
    pub(crate) n_buffers: i64,
    pub(crate) n_children: i64,
    pub(crate) buffers: u64,
    children: u64,
    dictionary: u64,
    release: Option<u64>,
    private_data: u64,
}

impl FFI32_ArrowArray {
    pub(crate) fn empty() -> Self {
        Self {
            length: 0,
            null_count: 0,
            offset: 0,
            n_buffers: 0,
            n_children: 0,
            buffers: 0,
            children: 0,
            dictionary: 0,
            release: None,
            private_data: 0,
        }
    }
}

impl FFI64_ArrowArray {
    pub(crate) fn empty() -> Self {
        Self {
            length: 0,
            null_count: 0,
            offset: 0,
            n_buffers: 0,
            n_children: 0,
            buffers: 0,
            children: 0,
            dictionary: 0,
            release: None,
            private_data: 0,
        }
    }
}

impl From<&FFI64_ArrowArray> for FFI32_ArrowArray {
    fn from(s64: &FFI64_ArrowArray) -> Self {
        let mut root = Self::empty();

        root.length = s64.length;
        root.null_count = s64.null_count;
        root.offset = s64.offset;
        root.n_buffers = s64.n_buffers;
        root.n_children = s64.n_children;

        // root.buffers
        // root.children
        // root.dictionary
        // root.release
        // root.private_data
        root
    }
}
