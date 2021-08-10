
use std::os::raw::c_void;
extern crate wee_alloc;
use std::{fs::{self, File}, io::{Write, Cursor, BufWriter, Read, Seek, SeekFrom}};

// Use `wee_alloc` as the global allocator.
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

use std::mem;
use std::sync::Arc;
use std::ops::Deref;
use std::convert::TryFrom;
use arrow::array::Int16Array;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Field, DataType, Schema};
use arrow::ffi::{ArrowArray, ArrowArrayRef, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::array::{Array, ArrayRef, StructArray, ArrayDataBuilder, make_array};

use arrow::{buffer::Buffer, array::{Float32Array, Int32Array, Int64Array}, ipc::{self, reader::{FileReader, StreamReader}, writer::{DictionaryTracker, FileWriter, IpcDataGenerator, IpcWriteOptions}}};
// use wasm_bindgen::prelude::*;

// #[wasm_bindgen]
// extern "C" {
//     type Buffer;
// }

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


#[no_mangle]
pub fn alloc(len: i64) -> *mut c_void {
    // create a new mutable buffer with capacity `len`
    let mut buf = Vec::with_capacity(len as usize);
    // take a mutable pointer to the buffer
    let ptr = buf.as_mut_ptr();
    // take ownership of the memory block and
    // ensure that its destructor is not
    // called when the object goes out of scope
    // at the end of the function
    std::mem::forget(buf);
    // return the pointer so the runtime
    // can write data at this offset
    ptr as *mut c_void
}    

#[no_mangle]
pub unsafe fn dealloc(ptr: i64, size: i64) {
    let data = Vec::from_raw_parts(ptr as *mut u8, size as usize, size as usize);
    std::mem::drop(data);
}    



pub fn transform_record_batch(record_in: RecordBatch) -> RecordBatch {
    let num_cols = record_in.num_columns();
    let num_rows = record_in.num_rows();
    let data_type = record_in.schema().field(0).data_type().clone();
    // // println!("transform record batch data type = {:?},, rows = {:?}", data_type, num_rows);
    // let mut array_builder = ArrayDataBuilder::new(data_type);
    // let buffer = Buffer::from(vec![0; num_rows]);
    // array_builder = array_builder.buffers(vec![buffer]);
    // // Get null buffer
    // let null_buffer = record_in.column(num_cols-1).data().null_buffer().unwrap();
    // array_builder = array_builder.null_bit_buffer(null_buffer.clone());

    // // Get number of elements
    // array_builder = array_builder.len(num_rows);
    
    // let array_data = array_builder.build();
    let struct_array = Int64Array::from(vec![0; num_rows]);
    let new_column = Arc::new(struct_array);

    // Create an array with one element of 0
    // let zero_array = Int32Array::from(vec![0, 0, 0, 0, 0]);
    // let const_array = Arc::new(zero_array);
    // Get the columns except the last column
    let columns: &[ArrayRef] = record_in.columns();
    let num_cols = record_in.num_columns();
    let first_columns = columns[0..num_cols-1].to_vec();
    // Create a new array with the same columns expect the last where it will be zero column
    // let new_array = [first_columns, vec![new_column]].concat();
    let new_array = &columns[0..num_cols];
    // Create a transformed record batch with the same schema and the new array
    let transformed_record = RecordBatch::try_new(
        record_in.schema(),
        new_array.to_vec()
    ).unwrap();
    transformed_record
}

#[no_mangle]
pub fn transform(ffi_ptr: i64, new_buff: i64, new_child: i64) -> i64 {
    // Get the pointers of FFI_ArrowArray and FFI_ArrowSchema
    let ffi_tuple = Into::<Pointer<Tuple>>::into(ffi_ptr).borrow();
    let pffiaa = ((*ffi_tuple).0) as *const FFI_ArrowArray;
    let pffias = ((*ffi_tuple).1) as *const FFI_ArrowSchema;
    // let tmp = unsafe{&(*pffiaa)};
    // Build a record batch from the FFI pointers
    let arrow_array = unsafe{ArrowArray::try_from_raw(pffiaa as *const FFI_ArrowArray, pffias as *const FFI_ArrowSchema).unwrap()};
    // let array_data = arrow_array.to_data().unwrap();
    // let struct_array : StructArray = array_data.into();
    // let record = RecordBatch::from(&struct_array);
    
    // // Transformation
    // // let transformed_record = transform_record_batch(record);
    // let transformed_record = record;
    // // Transformation

    // // Create a pointer of a tuple of pointers of FFI_ArrowArray and FFI_ArrowSchema for the transformed record batch
    // let struct_array: StructArray = transformed_record.into();
    // let (ffiaa, ffias) = struct_array.to_raw().unwrap();
    // let ret_tuple = Tuple(ffiaa as i64, ffias as i64);
    // let ret_tuple_ptr: i64 = Pointer::new(ret_tuple).into();
    // mem::forget(arrow_array);
    // ret_tuple_ptr
// let addr = (pffiaa as i64) + 8*5;
// let tmp = unsafe{*(ffi_ptr as *const i64)};

// let addr = ffi_ptr; 
// let ffiaa_read = unsafe{std::ptr::read(addr as *const i64)};
// ffiaa_read
// tmp.len() as i64
// tmp
// let addr = (pffiaa as i64) + 8*5;
// unsafe{std::ptr::write(addr as *mut i64, new_buff)};
// let addr = (pffiaa as i64) + 8*6;
// unsafe{std::ptr::write(addr as *mut i64, new_child)};

ffi_ptr
}

#[no_mangle]
pub fn transform2(pffiaa: i64, pffias: i64) -> i64 {
    // Get the pointers of FFI_ArrowArray and FFI_ArrowSchema
    // let ffi_tuple = Into::<Pointer<Tuple>>::into(ffi_ptr).borrow();
    // let pffiaa = ((*ffi_tuple).0) as *const FFI_ArrowArray;
    // let pffias = ((*ffi_tuple).1) as *const FFI_ArrowSchema;
    // Build a record batch from the FFI pointers
    // let arrow_array = unsafe{ArrowArray::try_from_raw(pffiaa as *const FFI_ArrowArray, pffias as *const FFI_ArrowSchema).unwrap()};
    // let array_data = arrow_array.to_data().unwrap();
    // let struct_array : StructArray = array_data.into();
    // let record = RecordBatch::from(&struct_array);
    
    // // Transformation
    // // let transformed_record = transform_record_batch(record);
    // let transformed_record = record;
    // // Transformation

    // // Create a pointer of a tuple of pointers of FFI_ArrowArray and FFI_ArrowSchema for the transformed record batch
    // let struct_array: StructArray = transformed_record.into();
    // let (ffiaa, ffias) = struct_array.to_raw().unwrap();
    // let ret_tuple = Tuple(ffiaa as i64, ffias as i64);
    // let ret_tuple_ptr: i64 = Pointer::new(ret_tuple).into();
    // mem::forget(arrow_array);
    // ret_tuple_ptr

    let tmp = unsafe{&(*(pffias as *const FFI_ArrowSchema))};
    // tmp.format().chars().next().unwrap() as i64
pffiaa
}


// Creates an example FFI_ArrowArray and FFI_ArrowSchema and returns a pointer to them.
#[no_mangle]
pub fn create_empty_ffi_arrow() -> i64 {
    let arrow_array= unsafe{ArrowArray::empty()};
    let (ffiaa, ffias) = ArrowArray::into_raw(arrow_array);
    let ffi_tuple = Tuple(ffiaa as i64, ffias as i64);
    let ffi_tuple_ptr: i64 = Pointer::new(ffi_tuple).into();
    ffi_tuple_ptr
}

// Creates an example FFI_ArrowArray and FFI_ArrowSchema and returns a pointer to them.
#[no_mangle]
pub fn create_ffi_arrow() -> i64 {
    let field1 = Field::new("a", DataType::Int16, true);
    let field2 = Field::new("b", DataType::Int16, true);
    let int_array1 = Int16Array::from(vec![1]);
    let int_array_data1 = make_array(int_array1.data().clone());
    let int_array2 = Int16Array::from(vec![2]);
    let int_array_data2 = make_array(int_array2.data().clone());
    // The array has two columns, each column has one int16 value
    let array = StructArray::try_from(vec![(field1, int_array_data1), (field2, int_array_data2)]).unwrap();

    let (ffiaa, ffias) = array.to_raw().unwrap();
    let ffi_tuple = Tuple(ffiaa as i64, ffias as i64);
    let ffi_tuple_ptr: i64 = Pointer::new(ffi_tuple).into();
    ffi_tuple_ptr
}

#[no_mangle]
pub fn check_transform(ffi_ptr: i64, after_transform: i32) {
    // Get the pointers of FFI_ArrowArray and FFI_ArrowSchema and create a record batch
    let ffi_tuple = Into::<Pointer<Tuple>>::into(ffi_ptr).borrow();
    let pffiaa = ((*ffi_tuple).0) as *const FFI_ArrowArray;
    let pffias = ((*ffi_tuple).1) as *const FFI_ArrowSchema;
    let arrow_array = unsafe{ArrowArray::try_from_raw(pffiaa, pffias).unwrap()};
    let array_data = arrow_array.to_data().unwrap();
    let struct_array: StructArray = StructArray::from(array_data.clone());
    let record = RecordBatch::from(&struct_array);

    // Check that the data type is correct
    assert_eq!(&DataType::Int16, record.schema().field(0).data_type());
    if after_transform == 1 {
        // Check if the first element of the second column is 0 after the transformation
        assert_eq!(&0, record.column(1).data().buffers().first().unwrap().get(0).unwrap());
    } else {
        // Check if the first element of the second column is 2 before the transformation
        assert_eq!(&2, record.column(1).data().buffers().first().unwrap().get(0).unwrap());
    }

    mem::forget(arrow_array);

}

#[no_mangle]
pub fn drop_record_batch(ffi_ptr: i64) {
    // Get the pointers of FFI_ArrowArray and FFI_ArrowSchema and create a record batch
    let ffi_tuple = Into::<Pointer<Tuple>>::into(ffi_ptr).borrow();
    let pffiaa = ((*ffi_tuple).0) as *const FFI_ArrowArray;
    let pffias = ((*ffi_tuple).1) as *const FFI_ArrowSchema;
    let arrow_array = unsafe{ArrowArray::try_from_raw(pffiaa, pffias).unwrap()};
    let array_data = arrow_array.to_data().unwrap();
    let struct_array: StructArray = StructArray::from(array_data.clone());
    let _record = RecordBatch::from(&struct_array);
    // mem::drop(arrow_array);
}

#[no_mangle]
pub fn create_tuple_ptr(pffiaa: i64, pffias: i64) -> i64 {
    let ret_tuple = Tuple(pffiaa as i64, pffias as i64);
    let ret_tuple_ptr = Pointer::new(ret_tuple).into();
    ret_tuple_ptr
}

#[no_mangle]
pub fn get_ffiaa(ffi_ptr: i64) -> i64 {
    let ffi_tuple = Into::<Pointer<Tuple>>::into(ffi_ptr).borrow();
    // let pffiaa = ((*ffi_tuple).0) as *const FFI_ArrowArray;
    // pffiaa as i64
    (*ffi_tuple).0
}

#[no_mangle]
pub fn get_ffias(ffi_ptr: i64) -> i64 {
    let ffi_tuple = Into::<Pointer<Tuple>>::into(ffi_ptr).borrow();
    // let pffias = ((*ffi_tuple).1) as *const FFI_ArrowSchema;
    // // let ret_ffias_ptr: i64 = Pointer::new(pffias).into();
    // pffias as i64
    (*ffi_tuple).1
}

#[no_mangle]
pub fn write_field(addr: i64, val: i64) {
    // let ffiaa_read = unsafe{std::ptr::read(val)};
    // unsafe{std::ptr::write(addr as *mut FFI_ArrowArray, ffiaa_read)};

    unsafe{std::ptr::write(addr as *mut i64, val)};
}

#[no_mangle]
pub fn write_field_u8(addr: i64, val: u8) {
    // let ffiaa_read = unsafe{std::ptr::read(val)};
    // unsafe{std::ptr::write(addr as *mut FFI_ArrowArray, ffiaa_read)};

    unsafe{std::ptr::write(addr as *mut u8, val)};
}
 //////////IPC related functions//////////
#[no_mangle]
pub fn write_batch_example() -> i64 {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Float32, false),
        Field::new("b", DataType::Float32, false),
        Field::new("c", DataType::Int32, false),
        Field::new("d", DataType::Int32, false),
    ]);
    let arrays = vec![
        Arc::new(Float32Array::from(vec![1.23])) as ArrayRef,
        Arc::new(Float32Array::from(vec![-6.50])) as ArrayRef,
        Arc::new(Int32Array::from(vec![2])) as ArrayRef,
        Arc::new(Int32Array::from(vec![1])) as ArrayRef,
    ];
    let batch = RecordBatch::try_new(Arc::new(schema.clone()), arrays).unwrap();
    // create stream writer
    // let mut cursor = Cursor::new(Vec::new());
    // let mut writer = FileWriter::try_new(cursor, &schema).unwrap();
    let mut vec = Vec::new();
    let mut writer = crate::ipc::writer::StreamWriter::try_new(vec, &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();

    // let my_file_writer: MyFileWriter<Cursor<Vec<u8>>> = unsafe {
    //     std::mem::transmute(writer)
    // };

    // let mut cursor = my_file_writer.writer.into_inner().unwrap();
    
    // cursor.seek(SeekFrom::Start(0)).unwrap();
    // let mut bytes_array = Vec::new();
    // cursor.read_to_end(&mut bytes_array).unwrap();
    // println!("bytes = {:?}", bytes_array);
    let mut bytes_array = writer.into_inner().unwrap();
    let bytes_ptr = bytes_array.as_mut_ptr();
    let bytes_len = bytes_array.len();
    mem::forget(bytes_array);
    create_tuple_ptr(bytes_ptr as i64, bytes_len as i64)
}

#[no_mangle]
pub fn read_from_bytes(bytes_ptr: i64, bytes_len: i64) -> i64 {
    // let bytes_ptr_len = Into::<Pointer<Tuple>>::into(bytes_tuple).borrow();
    // let bytes_ptr = (*bytes_ptr_len).0;
    // let bytes_len = (*bytes_ptr_len).1;

    let bytes_array: Vec<u8> = unsafe{ Vec::from_raw_parts(bytes_ptr as *mut _, bytes_len as usize, bytes_len as usize) };
    // let mut bytes_array = Vec::new();
    // for i in 0..bytes_len {
    //     let src_addr = bytes_ptr + i;
    //     bytes_array.push(unsafe{std::ptr::read(src_addr as *mut u8)});
    // }
    let cursor = Cursor::new(bytes_array);
    let reader = StreamReader::try_new(cursor).unwrap();
    let mut ret_ptr = 0;
    reader.for_each(|batch| {
        let batch = batch.unwrap();
        println!("reader = {:?}", batch);
        let transformed = transform_record_batch(batch);
        // let transformed = batch;
        // assert!(
        //     transformed
        //         .column(0)
        //         .as_any()
        //         .downcast_ref::<Int32Array>()
        //         .unwrap()
        //         .value(0)
        //         == 1
        // );
        // assert!(
        //     transformed
        //         .column(3)
        //         .as_any()
        //         .downcast_ref::<Int32Array>()
        //         .unwrap()
        //         .value(1)
        //         == 0
        // );


        // write the transformed record batch uing IPC
        let schema = transformed.schema();
        let mut vec = Vec::new();
        let mut writer = crate::ipc::writer::StreamWriter::try_new(vec, &schema).unwrap();
        writer.write(&transformed).unwrap();
        writer.finish().unwrap();
        mem::forget(transformed);
        let mut bytes_array = writer.into_inner().unwrap();
        let bytes_ptr = bytes_array.as_mut_ptr();
        let bytes_len = bytes_array.len();
        mem::forget(bytes_array);
        ret_ptr =  create_tuple_ptr(bytes_ptr as i64, bytes_len as i64);
        // ret_ptr =  bytes_ptr as i64;
    });
    // let mut bytes_array = Vec::new();
    // for i in 0..bytes_len {
    //     let src_addr = bytes_ptr + i;
    //     bytes_array.push(unsafe{std::ptr::read(src_addr as *mut u8)});
    // }
    ret_ptr
}

#[no_mangle]
pub fn get_first_of_tuple(tuple_ptr: i64) -> i64 {
    let tuple = Into::<Pointer<Tuple>>::into(tuple_ptr).borrow();
    (*tuple).0
}

#[no_mangle]
pub fn get_second_of_tuple(tuple_ptr: i64) -> i64 {
    let tuple = Into::<Pointer<Tuple>>::into(tuple_ptr).borrow();
    (*tuple).1
}
