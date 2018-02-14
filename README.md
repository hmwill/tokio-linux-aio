# tokio-linux-aio

This package provides an integration of Linux kernel-level asynchronous I/O to the [Tokio platform](https://tokio.rs/). 

Linux kernel-level asynchronous I/O is different from the [Posix AIO library](http://man7.org/linux/man-pages/man7/aio.7.html). Posix AIO is implemented using a pool of userland threads, which invoke regular, blocking system calls to perform file I/O. [Linux kernel-level AIO](http://lse.sourceforge.net/io/aio.html), on the other hand, provides kernel-level asynchronous scheduling of I/O operations to the underlying block device. 

__Note__: This is an initial check-in to reserve the crate name within crates.io. A real implementation will be available soon.