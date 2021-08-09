use std::convert::TryInto;

#[repr(C)]
#[derive(Debug)]
pub(crate) struct FFI32_ArrowSchema {
    format: u32,
    name: u32,
    metadata: u32,
    flags: i64,
    n_children: i64,
    children: u32,
    dictionary: u32,
    release: Option<u32>,
    private_data: u32,
}

impl FFI32_ArrowSchema {
    pub(crate) fn empty() -> Self {
        Self {
            format: 0,
            name: 0,
            metadata: 0,
            flags: 0,
            n_children: 0,
            children: 0,
            dictionary: 0,
            release: None,
            private_data: 0,
        }
    }
}

#[repr(C)]
#[derive(Debug)]
pub(crate) struct FFI64_ArrowSchema {
    format: u64,
    name: u64,
    metadata: u64,
    flags: i64,
    n_children: i64,
    children: u64,
    dictionary: u64,
    release: Option<u64>,
    private_data: u64,
}

impl FFI64_ArrowSchema {
    pub(crate) fn empty() -> Self {
        Self {
            format: 0,
            name: 0,
            metadata: 0,
            flags: 0,
            n_children: 0,
            children: 0,
            dictionary: 0,
            release: None,
            private_data: 0,
        }
    }
}

impl From<&FFI64_ArrowSchema> for FFI32_ArrowSchema {
    fn from(s64: &FFI64_ArrowSchema) -> Self {
        let mut root = FFI32_ArrowSchema::empty();

        root.format = s64.format.try_into().unwrap();
        root.name = s64.name.try_into().unwrap();
        root.metadata = s64.metadata.try_into().unwrap();
        root.flags = s64.flags;
        root.n_children = s64.n_children;
        root.dictionary = s64.dictionary.try_into().unwrap();

        // let children = root.children as *mut u32;
        // root.release
        // root.private_data

        root
    }
}

impl From<&FFI32_ArrowSchema> for FFI64_ArrowSchema {
    fn from(s32: &FFI32_ArrowSchema) -> Self {
        let mut root = FFI64_ArrowSchema::empty();

        root.format = s32.format.try_into().unwrap();
        root.name = s32.name.try_into().unwrap();
        root.metadata = s32.metadata.try_into().unwrap();
        root.flags = s32.flags;
        root.n_children = s32.n_children;
        root.dictionary = s32.dictionary.try_into().unwrap();

        // let children = root.children as *mut u32;
        // root.release
        // root.private_data

        root
    }
}
