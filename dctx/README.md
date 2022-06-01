# C DistributedContext

## Architecture

We want a background thread that executes distributed ccommunications, with
off-thread controls that can be bound into a separate thread with arbitrary
threading requirements.

## Dependencies

We will require:

- libuv, for easy, performant, and cross-platform event handling.
- pthread, for running uv loop on background thread

## Building

```bash
mkdir build
cd build
cmake -GNinja -DCMAKE_BUILD_TYPE=Debug ..
```
