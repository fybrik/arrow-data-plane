use std::{convert::TryInto, mem};

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

fn to32(base: u64, ptr: u64) -> u32 {
    if ptr == 0 {
        return 0;
    }
    (ptr - base).try_into().unwrap()
} 

fn to64(base: u64, ptr: u32) -> u64 {
    if ptr == 0 {
        return 0;
    }
    base + ptr as u64
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

    pub fn from(base: u64, s64: &mut FFI64_ArrowSchema) -> Self {
        let mut root = Self::empty();

        root.format = to32(base, s64.format);
        root.name = to32(base, s64.name);
        root.metadata = to32(base, s64.metadata);
        root.flags = s64.flags;
        root.n_children = s64.n_children;
        let children_array = to32(base, s64.children) as *const u64;
        let child_data: Vec<FFI32_ArrowSchema> = (0..s64.n_children as usize)
            .map(|i| {
                let child = unsafe { children_array.add(i) };
                let child = unsafe { *child };
                let child = to32(base, child);
                let child = unsafe {&mut*(child as *mut FFI64_ArrowSchema)};
                FFI32_ArrowSchema::from(base, child)
            })
            .collect();
        let children_ptr = child_data
            .into_iter()
            .map(Box::new)
            .map(Box::into_raw)
            .collect::<Box<_>>();
        root.children = children_ptr.as_ptr() as u32;
        mem::forget(children_ptr);
        
        if s64.dictionary != 0 {
            let dictionary = to32(base, s64.dictionary) as *mut FFI64_ArrowSchema;
            let dictionary = unsafe { &mut*dictionary };
            let dictionary = FFI32_ArrowSchema::from(base, dictionary);
            let dictionary = Box::from(dictionary);
            let dictionary = Box::into_raw(dictionary);
            root.dictionary = dictionary as u32;
        }

        root.release = Some(s64.release.unwrap() as u32);
        // root.private_data = 

        // Move old schema
        s64.release = None;

        root
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

    pub fn from(base: u64, s32: &mut FFI32_ArrowSchema) -> Self {
        let mut root = Self::empty();

        root.format = to64(base, s32.format);
        root.name = to64(base, s32.name);
        root.metadata = to64(base, s32.metadata);
        root.flags = s32.flags;
        root.n_children = s32.n_children;
        // let children_array = to64(base, s32.children) as *const u32;
        let children_array = s32.children as *const u32;
        let child_data: Vec<FFI64_ArrowSchema> = (0..s32.n_children as usize)
            .map(|i| {
                let child = unsafe { children_array.add(i) };
                let child = unsafe { *child };
                // let child = to64(base, child);
                let child = unsafe {&mut*(child as *mut FFI32_ArrowSchema)};
                FFI64_ArrowSchema::from(base, child)
            }).collect();
            // .map(|i| {
            //     let child = unsafe { children_array.add(i) };
            //     let child = unsafe { *child };
            //     child as u64
            // }).collect();

        let children_ptr = child_data
            .into_iter()
            .map(Box::new)
            .map(Box::into_raw)
            .collect::<Box<_>>();
        root.children = children_ptr.as_ptr() as u32;
        mem::forget(children_ptr);
        
        if s64.dictionary != 0 {
            let dictionary = to32(base, s64.dictionary) as *mut FFI64_ArrowSchema;
            let dictionary = unsafe { &mut*dictionary };
            let dictionary = FFI32_ArrowSchema::from(base, dictionary);
            let dictionary = Box::from(dictionary);
            let dictionary = Box::into_raw(dictionary);
            root.dictionary = dictionary as u32;
        }

        root.release = Some(s64.release.unwrap() as u32);
        // root.private_data = 

        // Move old schema
        s64.release = None;

        root
    }

}

// impl From<&FFI64_ArrowSchema> for FFI32_ArrowSchema {
//     fn from(s64: &FFI64_ArrowSchema) -> Self {
//         let mut root = FFI32_ArrowSchema::empty();

//         let format = std::ffi::CString::new("i").unwrap().into_raw();
//         println!("64:{}", format as u64);
//         println!("32:{}", format as u32);
//         root.format = format as u32;

//         // TODO: problem is that all pointers include the base address
//         let format = s64.format as *mut i8;
//         println!("64:{}", format as u64);
//         println!("32:{}", format as u32);
//         root.format = format as u32;
        
//         // root.format = s64.format.try_into().unwrap();
//         root.n_children = 0;
//         root.release = Some(s64.release.unwrap() as u32);

//         // root.format = s64.format.try_into().unwrap();
//         // root.name = s64.name.try_into().unwrap();
//         // root.metadata = s64.metadata.try_into().unwrap();
//         // root.flags = s64.flags;
//         // root.n_children = s64.n_children;
//         // root.dictionary = s64.dictionary.try_into().unwrap();

//         // let children = root.children as *mut u32;
//         // root.release
//         // root.private_data

//         root
//     }
// }

// // impl From<&FFI32_ArrowSchema> for FFI64_ArrowSchema {
// //     fn from(s32: &FFI32_ArrowSchema) -> Self {
// //         let mut root = FFI64_ArrowSchema::empty();

// //         root.format = s32.format.try_into().unwrap();
// //         root.name = s32.name.try_into().unwrap();
// //         root.metadata = s32.metadata.try_into().unwrap();
// //         root.flags = s32.flags;
// //         root.n_children = s32.n_children;
// //         root.dictionary = s32.dictionary.try_into().unwrap();

// //         // let children = root.children as *mut u32;
// //         // root.release
// //         // root.private_data

// //         root
// //     }
// // }
