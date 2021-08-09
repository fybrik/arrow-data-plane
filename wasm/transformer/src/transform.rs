use crate::common::TransformContext;

#[no_mangle]
pub extern "C" fn transform(ctx: u32) {
    let ctx = ctx as *mut TransformContext;
    let ctx = unsafe{ &*ctx };
    let _ = ctx.input();
}
