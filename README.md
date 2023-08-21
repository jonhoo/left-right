[![Codecov](https://codecov.io/github/jonhoo/left-right/coverage.svg?branch=main)](https://codecov.io/gh/jonhoo/left-right)
[![Crates.io](https://img.shields.io/crates/v/left-right.svg)](https://crates.io/crates/left-right)
[![Documentation](https://docs.rs/left-right/badge.svg)](https://docs.rs/left-right/)

Left-right is a concurrency primitive for high concurrency reads over a
single-writer data structure. The primitive keeps two copies of the
backing data structure, one that is accessed by readers, and one that is
accessed by the (single) writer. This enables all reads to proceed in
parallel with minimal coordination, and shifts the coordination overhead
to the writer. In the absence of writes, reads scale linearly with the
number of cores.
