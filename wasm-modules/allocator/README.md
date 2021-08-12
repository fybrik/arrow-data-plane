# Wasm module for allocator

Run the following command to get a .wasm file:

`cargo build --target wasm32-wasi --release`

Optimize with:

`wasm-opt -O2 target/wasm32-wasi/release/alloc.wasm -o target/wasm32-wasi/release/alloc.wasm`
