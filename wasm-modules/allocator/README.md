# Wasm module for allocator

Run the following command to get a .wasm file:

```
cargo build --target wasm32-wasi --release
```

To optimize the module install `wasm-opt`, e.g.:

```bash
sudo apt-get install binaryen
```

Then run:

```bash
wasm-opt -O2 target/wasm32-wasi/release/alloc.wasm -o target/wasm32-wasi/release/alloc.wasm
```
