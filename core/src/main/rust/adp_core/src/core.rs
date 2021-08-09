use std::io;
use wasmer::NativeFunc;
use wasmer::{imports, Cranelift, Instance, Module, Store, Universal};

#[repr(C)]
pub struct CoreInstance {
    instance: Instance,
    // size -> buffer ptr
    pub allocate_buffer_func: NativeFunc<u32, u32>,
    // (buffer ptr, size) -> void
    pub deallocate_buffer_func: NativeFunc<(u32, u32), ()>,
    // void -> context ptr
    pub prepare_transform_func: NativeFunc<(), u32>,
    // context ptr -> void
    pub transform_func: NativeFunc<u32, ()>,
    // context ptr -> void
    pub finalize_tansform_func: NativeFunc<u32, ()>,
}

#[repr(C)]
pub struct FFI_TransformContext {
    pub in_schema: u32,
    pub in_array: u32,
    pub out_schema: u32,
    pub out_array: u32,
}

impl CoreInstance {
    /// create a new [`Ffi_ArrowSchema`]. This fails if the fields' [`DataType`] is not supported.
    pub fn try_new(module_bytes: &[u8]) -> Result<Self, io::Error> {
        let store = Store::new(&Universal::new(Cranelift::default()).engine());
        let module = Module::new(&store, module_bytes).unwrap();
        let import_object = imports! {
            "env" => {}
        };
        let instance = Instance::new(&module, &import_object).unwrap();
        
        let allocate_buffer_func = instance
            .exports
            .get_native_function::<u32, u32>("allocate_buffer")
            .unwrap();

        let deallocate_buffer_func = instance
            .exports
            .get_native_function::<(u32, u32), ()>("deallocate_buffer")
            .unwrap();

        let prepare_transform_func = instance
            .exports
            .get_native_function::<(), u32>("prepare_transform")
            .unwrap();

        let transform_func = instance
            .exports
            .get_native_function::<u32, ()>("transform")
            .unwrap();

        let finalize_tansform_func = instance
            .exports
            .get_native_function::<u32, ()>("finalize_tansform")
            .unwrap();

        Ok(Self {
            instance,
            allocate_buffer_func,
            deallocate_buffer_func,
            prepare_transform_func,
            transform_func,
            finalize_tansform_func
        })
    }

    pub fn context(&self, context: u64) -> &FFI_TransformContext {
        let base = self.allocator_base();
        let ctx = (base + context) as *const FFI_TransformContext;
        let ctx = unsafe { &*ctx };
        ctx    
    }

    pub fn allocator_base(&self) -> u64 {
        let memory = self.instance.exports.get_memory("memory").unwrap();
        let mem_ptr = memory.data_ptr();
        mem_ptr as u64
    }
    
    pub fn allocate_buffer(&self, size: u32) -> u32 {
        self.allocate_buffer_func.call(size).unwrap()
    }

    pub fn deallocate_buffer(&self, buffer_ptr: u32, size: u32) {
        self.deallocate_buffer_func.call(buffer_ptr, size).unwrap();
    }

    pub fn prepare_transform(&self) -> u32 {
        self.prepare_transform_func.call().unwrap()
    }

    pub fn transform(&self, context: u32) {
        self.transform_func.call(context).unwrap();
    }

    pub fn finalize_tansform(&self, context: u32) {
        self.finalize_tansform_func.call(context).unwrap();
    }
}
