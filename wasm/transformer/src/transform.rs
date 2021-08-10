use crate::common::TransformContext;
use arrow::{array::StructArray, record_batch::RecordBatch, util::pretty::print_batches};

#[no_mangle]
pub extern "C" fn transform(ctx: u32) {
    let ctx = ctx as *mut TransformContext;
    let ctx = unsafe{ &*ctx };
    // let schema = ctx.input_schema();
    // println!("{}", schema.unwrap());
    let array = ctx.input().unwrap();
    let as_structarray = array.as_any().downcast_ref::<StructArray>().unwrap();
    let result = RecordBatch::from(as_structarray);
    let _ = print_batches(&[result]);

}
