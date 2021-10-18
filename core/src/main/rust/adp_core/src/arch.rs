use std::{
    convert::TryInto,
    mem::{self, size_of},
    os::raw::{c_char, c_void},
    ptr::{self, NonNull},
};

use crate::core::CoreInstance;

/// ABI-compatible struct for `ArrowSchema` from C Data Interface
/// See <https://arrow.apache.org/docs/format/CDataInterface.html#structure-definitions>
/// This was created by bindgen
#[repr(C)]
#[derive(Debug)]
pub struct FFI64_ArrowSchema {
    format: *const c_char,
    name: *const c_char,
    metadata: *const c_char,
    flags: i64,
    n_children: i64,
    children: *mut *mut FFI64_ArrowSchema,
    dictionary: *mut FFI64_ArrowSchema,
    release: Option<unsafe extern "C" fn(arg1: *mut FFI64_ArrowSchema)>,
    private_data: *mut c_void,
}

#[repr(C)]
#[derive(Debug)]
// #[derive(Debug)]
pub struct FFI32_ArrowSchema {
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

#[repr(C)]
#[derive(Debug)]
pub(crate) struct FFI32_ArrowSchema_PrivateData {
    inner: FFI64_ArrowSchema,
    children_buffer_ptr: u32,
    children_buffer_size: u32,
}

/// Plugin allocates empty s32 in WASM memory (`in32`)
/// Core allocates empty s64 (`in64`)
/// Java fills in64 using WASM allocator
/// Core transmutes in64 into in32:
/// 1. Remove `base` from addresses
/// 2. Allocates any auxilary structures in WASM memory
/// 3. Overrides private data to hold `in64` and self allocations (from 2)
/// 4. Overrides release callback to a function that frees in64 and self allocations
/// 5. WITHIN WASM: Overrides release callback to imported function
impl FFI32_ArrowSchema {
    fn new(instance: &CoreInstance) -> *mut Self {
        let allocated_offset = instance.allocate_buffer(size_of::<Self>() as u32);
        let s32 = to64(instance.allocator_base(), allocated_offset) as *mut FFI32_ArrowSchema;
        s32 as *mut Self
    }

    pub fn from(&mut self, instance: &CoreInstance, s64: &mut FFI64_ArrowSchema) {
        let base = instance.allocator_base();

        self.private_data =
            instance.allocate_buffer(size_of::<FFI32_ArrowSchema_PrivateData>() as u32);
        let private_data = self.private_data as *mut FFI32_ArrowSchema_PrivateData;
        let private_data = unsafe { &mut *private_data };
        private_data.inner = *s64;

        self.format = to32(base, s64.format as u64);
        self.name = to32(base, s64.name as u64);
        self.metadata = to32(base, s64.metadata as u64);
        self.flags = s64.flags;
        self.n_children = s64.n_children;

        // Children
        if self.n_children > 0 {
            // Allocate WASM memory for children array
            private_data.children_buffer_size = (self.n_children * 4) as u32;
            private_data.children_buffer_ptr =
                instance.allocate_buffer(private_data.children_buffer_size);

            // Copy children from 64bit structures
            let s64_children_array = s64.children as *const u64;
            let s32_children_array = to64(base, private_data.children_buffer_ptr) as *mut u32;
            for i in 0..s64.n_children as usize {
                let s64_child_item = unsafe { s64_children_array.add(i) };
                let s32_child_item = unsafe { s32_children_array.add(i) };
                let s64_child = unsafe { &mut *(s64_child_item as *mut FFI64_ArrowSchema) };
                let s32_child = Self::from(instance, s64_child);
                unsafe {
                    *s32_child_item = to32(base, s32_child as u64);
                }
            }
            root.children = private_data.children_buffer_ptr;
        }

        // dictionary
        root.dictionary = 0;
        if !s64.dictionary.is_null() {}

        // The release callback is set in the WASM module
        root.release = None; // TODO

        // Move old schema
        s64.release = None;

        root as *mut Self
    }
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

// #[repr(C)]
// #[derive(Debug, Clone, Copy)]
// // #[derive(Debug)]
// pub(crate) struct FFI32_ArrowSchema {
//     format: u32,
//     name: u32,
//     metadata: u32,
//     flags: i64,
//     n_children: i64,
//     children: u32,
//     dictionary: u32,
//     release: Option<u32>,
//     private_data: u32,
// }

// #[repr(C)]
// #[derive(Debug, Clone, Copy)]
// pub(crate) struct FFI64_ArrowSchema {
//     format: u64,
//     name: u64,
//     metadata: u64,
//     flags: i64,
//     n_children: i64,
//     children: *mut *mut FFI64_ArrowSchema,
//     dictionary: *mut FFI64_ArrowSchema,
//     release: Option<unsafe extern "C" fn(arg1: *mut FFI64_ArrowSchema)>,
//     private_data: *mut c_void,
// }

// #[repr(C)]
// // #[derive(Debug, Clone, Copy)]
// #[derive(Debug)]
// pub(crate) struct FFI32_ArrowArray {
//     pub(crate) length: i64,
//     pub(crate) null_count: i64,
//     pub(crate) offset: i64,
//     pub(crate) n_buffers: i64,
//     pub(crate) n_children: i64,
//     pub(crate) buffers: u32,
//     children: u32,
//     dictionary: u32,
//     release: Option<u32>,
//     private_data: u32,
// }

// #[repr(C)]
// #[derive(Debug, Clone, Copy)]
// pub(crate) struct FFI64_ArrowArray {
//     pub(crate) length: i64,
//     pub(crate) null_count: i64,
//     pub(crate) offset: i64,
//     pub(crate) n_buffers: i64,
//     pub(crate) n_children: i64,
//     pub(crate) buffers: u64,
//     children: u64,
//     dictionary: u64,
//     release: Option<u64>,
//     private_data: u64,
// }

// impl Default for FFI64_ArrowSchema {
//     fn default() -> Self {
//         Self {
//             format: 0,
//             name: 0,
//             metadata: 0,
//             flags: 0,
//             n_children: 0,
//             children: ptr::null_mut(),
//             dictionary: std::ptr::null_mut(),
//             release: None,
//             private_data: std::ptr::null_mut(),
//         }
//     }
// }

// impl Default for FFI64_ArrowArray {
//     fn default() -> Self {
//         Self {
//             length: 0,
//             null_count: 0,
//             offset: 0,
//             n_buffers: 0,
//             n_children: 0,
//             buffers: 0,
//             children: 0,
//             dictionary: 0,
//             release: None,
//             private_data: 0,
//         }
//     }
// }

// #[repr(C)]
// #[derive(Debug)]
// pub(crate) struct FFI32_ArrowSchema_PrivateData {
//     pub(crate) inner: FFI64_ArrowSchema,
//     children_buffer_ptr: u32,
//     children_buffer_size: u32,
// }

// unsafe extern "C" fn release_FFI32_ArrowSchema(schema: *mut FFI32_ArrowSchema) {
//     if schema.is_null() {
//         return;
//     }

//     let schema = &mut *schema;
//     let private_data = schema.private_data as *mut FFI32_ArrowSchema_PrivateData;
//     let private_data = &mut *private_data;
//     private_data.inner.release;
//     match private_data.inner.release {
//         None => (),
//         Some(release) => release(&mut private_data.inner),
//     };
// }

// impl FFI32_ArrowSchema {
//     fn new(instance: &CoreInstance) -> *mut Self {
//         let allocated_offset = instance.allocate_buffer(size_of::<Self>() as u32);
//         let s32 = to64(instance.allocator_base(), allocated_offset) as *mut FFI32_ArrowSchema;
//         s32 as *mut Self
//     }

//     /// Plugin allocates empty s32 in WASM memory (`in32`)
//     /// Core allocates empty s64 (`in64`)
//     /// Java fills in64 using WASM allocator
//     /// Core transmutes in64 into in32:
//     /// 1. Remove `base` from addresses
//     /// 2. Allocates any auxilary structures in WASM memory
//     /// 3. Overrides private data to hold `in64` and self allocations (from 2)
//     /// 4. Overrides release callback to a function that frees in64 and self allocations
//     /// 5. WITHIN WASM: Overrides release callback to imported function
//     pub fn from(instance: &CoreInstance, s64: &mut FFI64_ArrowSchema) -> *mut Self {
//         let root = Self::new(instance);
//         let mut root = unsafe { &mut *root };

//         let base = instance.allocator_base();

//         root.private_data =
//             instance.allocate_buffer(size_of::<FFI32_ArrowSchema_PrivateData>() as u32);
//         let private_data = root.private_data as *mut FFI32_ArrowSchema_PrivateData;
//         let private_data = unsafe { &mut *private_data };
//         private_data.inner = *s64;

//         root.format = to32(base, s64.format);
//         root.name = to32(base, s64.name);
//         root.metadata = to32(base, s64.metadata);
//         root.flags = s64.flags;
//         root.n_children = s64.n_children;

//         // Children
//         if root.n_children > 0 {
//             // Allocate WASM memory for children array
//             private_data.children_buffer_size = (root.n_children * 4) as u32;
//             private_data.children_buffer_ptr =
//                 instance.allocate_buffer(private_data.children_buffer_size);

//             // Copy children from 64bit structures
//             let s64_children_array = s64.children as *const u64;
//             let s32_children_array = to64(base, private_data.children_buffer_ptr) as *mut u32;
//             for i in 0..s64.n_children as usize {
//                 let s64_child_item = unsafe { s64_children_array.add(i) };
//                 let s32_child_item = unsafe { s32_children_array.add(i) };
//                 let s64_child = unsafe { &mut *(s64_child_item as *mut FFI64_ArrowSchema) };
//                 let s32_child = Self::from(instance, s64_child);
//                 unsafe {
//                     *s32_child_item = to32(base, s32_child as u64);
//                 }
//             }
//             root.children = private_data.children_buffer_ptr;
//         }

//         // dictionary
//         root.dictionary = 0;
//         if !s64.dictionary.is_null() {}

//         // The release callback is set in the WASM module
//         root.release = None; // TODO

//         // Move old schema
//         s64.release = None;

//         root as *mut Self
//     }
// }

// #[repr(C)]
// #[derive(Debug)]
// pub(crate) struct FFI64_ArrowSchema_PrivateData {
//     pub(crate) inner: FFI32_ArrowSchema,
// }

// impl FFI64_ArrowSchema {
//     pub(crate) fn new() -> Self {
//         Default::default()
//     }

//     /// Core allocates empty s32 in WASM memory (`out32`)
//     /// Rust fills out32
//     /// Core allocates empty s64 (`out64`)
//     /// Core transmutes out32 into out64:
//     /// 1. Add `base` to addresses
//     /// 2. Allocates any auxilary structures
//     /// 3. Overrides private data to hold `out32` and self allocations (from 2)
//     /// 4. Overrides release callback to a function that frees out32* and self allocations
//     ///     * frees out32 using an exported function
//     pub fn from(instance: &CoreInstance, s32: &mut FFI32_ArrowSchema) -> Self {
//         let mut root = Self::new();

//         let base = instance.allocator_base();

//         root.format = to64(base, s32.format);
//         root.name = to64(base, s32.name);
//         root.metadata = to64(base, s32.metadata);
//         root.flags = s32.flags;
//         root.n_children = s32.n_children;

//         let private_data = FFI64_ArrowSchema_PrivateData{
//             inner: *s32,
//         };

//         // Children
//         if root.n_children > 0 {
//             let children_array_ptr = to64(base, s32.children) as *const u32;
//             let children: Vec<FFI64_ArrowSchema> = (0..s32.n_children as usize)
//                 .map(|i| {
//                     let child = unsafe { children_array_ptr.add(i) };
//                     let child = unsafe { *child };
//                     let child = to64(base, child);
//                     let child = unsafe { &mut *(child as *mut FFI32_ArrowSchema) };
//                     FFI64_ArrowSchema::from(instance, child)
//                 })
//                 .collect();

//             let mut children_ptr = children
//                 .into_iter()
//                 .map(Box::new)
//                 .map(Box::into_raw)
//                 .collect::<Box<_>>();
//             root.children = children_ptr.as_mut_ptr();
//             mem::forget(children_ptr);
//         }

//         // TODO
//         if !root.dictionary.is_null() {}

//         root.private_data = Box::into_raw(Box::new(private_data)) as *mut c_void;

//         // format: u64,
//         // name: u64,
//         // metadata: u64,
//         // flags: i64,
//         // n_children: i64,
//         // children: u64,
//         // dictionary: u64,
//         // release: Option<unsafe extern "C" fn(schema: *mut FFI64_ArrowSchema)>,
//         // private_data: u64,

//         root
//     }
// }

// impl FFI32_ArrowArray {}

// impl FFI64_ArrowArray {
//     pub(crate) fn new() -> Self {
//         Default::default()
//     }

//     pub fn from(instance: &CoreInstance, s32: &mut FFI32_ArrowArray) -> Self {
//         let mut root = Self::new();
//         root
//     }
// }
