# NStore

## Introduction

This is the implementation of the paper, "NStore: A High-Performance NUMA-Aware Key-Value Store for Hybrid Memory" (IEEE TC 2024).

## Compile & Run

### Dependencies
  - intel-mkl
  - libpmem
  - libpmemobj

### Configuration

Please set the PM pool path before testing.

Suppose the NVDIMM is mounted at `/mnt/pmem0` and `/mnt/pmem1`, we use `numactl` to bind the process.

### Build

```bash
git clone https://github.com/PDS-Lab/NStore
cd NStore
./build.sh
```
### Test
  
```bash
./test/run_example.sh
```

See `tests/run_example.sh` for more test details.

We generate some random data in our example test, you can also use your own dataset.

## Datasets

- Longlat
- YCSB
- Longitudes
- Lognormal
