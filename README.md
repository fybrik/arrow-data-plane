# arrow-data-plane

Approaches for data plane powered by Apache Arrow 

Currently this repository includes:
1. Arrow WASM SDK
2. Arrow Flight chains

## Arrow WASM SDK

The Arrow WASM SDK allows creating WASM modules that operate on Apache Arrow record batches. It enables integration of these modules into other components such as an Apache Arrow Flight server as plugins.

Initial support targets:
1. Implementing transformations in Rust compiled to WASM
2. Implementing a basic interface for transforming record batches and exposing it over JNI. Internally this layer uses a WASM runtime for executing transformation WASM modules
3. Implementing and demonstrating integration in an Apache Arrow Flight server implemented in Java

Performance is one of the main targets so the implementation targets as few memory copies (if at all) and microbenchmarking.

## Arrow Flight chains

Arrow Flight chains allows chaning multiple Arrow Flight servers together to perform data functions over Apache Arrow record batches (e.g., transformations) as part of the chain. This is an alternative approach to Arrow WASM SDK.

Initial support targets:
1. Demonstration of a chain with Arrow Flight servers implemeted in Java
2. Microbenchmarking in different deployment models in Kubernetes (same node, same pod, etc.)
3. Template for a chain node; allows implementing just the data function business logic and wrapping it in a Flight server that can be chained.



