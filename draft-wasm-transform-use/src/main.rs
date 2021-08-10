use std::mem;
use std::sync::Arc;
use wasmer::imports;
use std::convert::TryFrom;
use arrow::record_batch::RecordBatch;
use wasmer::{Instance, Module, Store};
use wasmer_engine_universal::Universal;
use wasmer_compiler_cranelift::Cranelift;
use arrow::datatypes::{Field, DataType};
use arrow::ffi::{ArrowArray, ArrowArrayRef, FFI_ArrowSchema, FFI_ArrowArray};
use arrow::array::{Int16Array, Array, ArrayRef, StructArray, make_array};

use std::ops::Deref;
#[derive(Debug)]
pub struct Pointer<Kind> {
    value: Box<Kind>,
}
impl<Kind> Pointer<Kind> {
    pub fn new(value: Kind) -> Self {
        Pointer {
            value: Box::new(value),
        }
    }
    pub fn borrow<'a>(self) -> &'a mut Kind {
        Box::leak(self.value)
    }
}
impl<Kind> From<Pointer<Kind>> for i64 {
    fn from(pointer: Pointer<Kind>) -> Self {
        Box::into_raw(pointer.value) as _
    }
}
impl<Kind> From<i64> for Pointer<Kind> {
    fn from(pointer: i64) -> Self {
        Self {
            value: unsafe { Box::from_raw(pointer as *mut Kind) },
        }
    }
}
impl<Kind> Deref for Pointer<Kind> {
    type Target = Kind;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct Tuple (pub i64, pub i64 );

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let wasm_bytes = std::fs::read("../wasm-modules/transform/target/wasm32-unknown-unknown/release/wasm_transform.wasm")?;
    let store = Store::new(&Universal::new(Cranelift::default()).engine());
    println!("Compiling module...");
    // Let's compile the Wasm module.
    let module = Module::new(&store, wasm_bytes)?;
    let import_object = imports! {
      "env" => {
      }
    };
    let instance = Instance::new(&module, &import_object)?;
    println!("instanced");
    let create_ffi_arrow_wasm = instance.exports.get_function("create_ffi_arrow")?.native::<(),i64>()?;
    let transform_wasm = instance.exports.get_function("transform")?.native::<i64,i64>()?;
    let check_transform_wasm = instance.exports.get_function("check_transform")?.native::<(i64, i32),()>()?;
    let drop_wasm = instance.exports.get_function("drop_record_batch")?.native::<i64,()>()?;

    let ffi_ptr = create_ffi_arrow_wasm.call()?;
    check_transform_wasm.call(ffi_ptr, 0)?;
    println!("pre transformation check passed");
    let ffi_transformed_ptr = transform_wasm.call(ffi_ptr)?;
    check_transform_wasm.call(ffi_transformed_ptr, 1)?;
    println!("post transformation check passed");

    // drop_wasm.call(ffi_ptr)?;
    // println!("after drop");
    // check_transform_wasm.call(ffi_transformed_ptr, 0)?;
    // println!("post transformation check passed");
    
    
    // let f = Field::new("name", DataType::Int16, true);
    // let int_array = Int16Array::from(vec![1]);
    // let int_array_data = make_array(int_array.data().clone());
    // let int_array2 = Int16Array::from(vec![2]);
    // let int_array_data2 = make_array(int_array2.data().clone());
    // let array = StructArray::try_from(vec![(f.clone(), int_array_data), (f.clone(), int_array_data2)]).unwrap();
    // // println!("arr = {:?}", array);
    // // print_type_of(&array);
    // // println!("array = {:?}", array);
    // let batch = RecordBatch::from(&array);
    // // println!("record batch {:?}", array);
    // let (FFIAA, FFIAS) = array.to_raw().unwrap();
    // println!("batch1");

    // let ffi_tuple = Tuple(FFIAA as i64, FFIAS as i64);
    // let ffi_tuple_ptr: i64 = Pointer::new(ffi_tuple).into();

    // let ffi_tuple_ptr = create_ffi_arrow();

    // check_transform(ffi_tuple_ptr, false);
    // let ffi_transformed = transform(ffi_tuple_ptr);
    // println!("record batch** {:?}", ffi_transformed);
    // check_transform(ffi_transformed, true);

    Ok(())
}


pub fn create_ffi_arrow() -> i64 {
    // let empty = unsafe { ArrowArray::empty() };
    // let empty_ptr = ArrowArray::into_raw(empty);
    

    let field1 = Field::new("a", DataType::Int16, true);
    let field2 = Field::new("b", DataType::Int16, true);
    let int_array1 = Int16Array::from(vec![1]);
    let int_array_data1 = make_array(int_array1.data().clone());
    let int_array2 = Int16Array::from(vec![2]);
    let int_array_data2 = make_array(int_array2.data().clone());
    let array = StructArray::try_from(vec![(field1, int_array_data1), (field2, int_array_data2)]).unwrap();
    println!("struct array = {:?}", array);

    let (ffiaa, ffias) = array.to_raw().unwrap();
    let ffi_tuple = Tuple(ffiaa as i64, ffias as i64);
    let ffi_tuple_ptr: i64 = Pointer::new(ffi_tuple).into();
    ffi_tuple_ptr
}

pub fn transform(ffi_ptr: i64) -> i64 {
    let ffi_tuple = Into::<Pointer<Tuple>>::into(ffi_ptr).borrow();
    let pffiaa = ((*ffi_tuple).0) as *const FFI_ArrowArray;
    let pffias = ((*ffi_tuple).1) as *const FFI_ArrowSchema;
    let arrow_array = unsafe{ArrowArray::try_from_raw(pffiaa, pffias).unwrap()};
    let array_data = arrow_array.to_data().unwrap();
    // let struct_array: StructArray = StructArray::from(array_data);
    let struct_array : StructArray = array_data.into();
    let record = RecordBatch::from(&struct_array);
    // Transformation
    let transformed_record = transform_record_batch(record);
    // Transformation
    let struct_array2: StructArray = transformed_record.into();
    let (ffiaa, ffias) = struct_array2.to_raw().unwrap();
    let ret_tuple = Tuple(ffiaa as i64, ffias as i64);
    let ret_tuple_ptr: i64 = Pointer::new(ret_tuple).into();
    mem::forget(arrow_array);
    ret_tuple_ptr
}

#[no_mangle]
pub fn transform_record_batch(x: RecordBatch) -> RecordBatch {
    let zero_array = Int16Array::from(vec![0]);
    let _const_array = Arc::new(zero_array);
    let _columns: &[ArrayRef] = x.columns();
    let _arr1 = _columns[0..1].to_vec();
    let _arr3 = [_arr1, vec![_const_array]].concat();
    let batch = RecordBatch::try_new(
        x.schema(),
        _arr3
    ).unwrap();
    println!("record transform ***** = {:?}", batch);
    return batch;
}

pub fn check_transform(ffi_ptr: i64, after_transform: bool) {
    let ffi_tuple = Into::<Pointer<Tuple>>::into(ffi_ptr).borrow();
    let pffiaa = ((*ffi_tuple).0) as *const FFI_ArrowArray;
    let pffias = ((*ffi_tuple).1) as *const FFI_ArrowSchema;
    let arrow_array = unsafe{ArrowArray::try_from_raw(pffiaa, pffias).unwrap()};
    let array_data = arrow_array.to_data().unwrap();
    // let array_data: ArrayData = ArrayData::try_from(arrow_array).unwrap();
    let struct_array: StructArray = StructArray::from(array_data.clone());
    println!("arrow_array = {:?}", struct_array);
    let record = RecordBatch::from(&struct_array);

    assert_eq!(&DataType::Int16, record.schema().field(0).data_type());
    if after_transform {
        assert_eq!(&0, record.column(1).data().buffers().first().unwrap().get(0).unwrap());
    } else {
        assert_eq!(&2, record.column(1).data().buffers().first().unwrap().get(0).unwrap());
    }

    mem::forget(arrow_array);

}


#[test]
fn test_imported_memory_env() -> Result<(), Box<dyn std::error::Error>> {
    main()
}