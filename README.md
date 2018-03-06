# tokio-linux-aio

[![Join the chat at https://gitter.im/tokio-linux-aio/Lobby](https://badges.gitter.im/tokio-linux-aio/Lobby.svg)](https://gitter.im/tokio-linux-aio/Lobby?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

[![Version](https://img.shields.io/crates/v/tokio-linux-aio.svg)](https://crates.io/crates/tokio-linux-aio)
[![License](https://img.shields.io/crates/l/tokio-linux-aio.svg)](https://github.com/hmwill/tokio-linux-aio/blob/master/LICENSE)
[![Build Status](https://travis-ci.org/hmwill/tokio-linux-aio.svg?branch=master)](https://travis-ci.org/hmwill/tokio-linux-aio)

This package provides an integration of Linux kernel-level asynchronous I/O to the [Tokio platform](https://tokio.rs/).

Linux kernel-level asynchronous I/O is different from the [Posix AIO library](http://man7.org/linux/man-pages/man7/aio.7.html). Posix AIO is implemented using a pool of userland threads, which invoke regular, blocking system calls to perform file I/O. [Linux kernel-level AIO](http://lse.sourceforge.net/io/aio.html), on the other hand, provides kernel-level asynchronous scheduling of I/O operations to the underlying block device.

__Note__: Implementation and test development is still in progress.

## Usage

Add this to your `Cargo.toml`:

    [dependencies]
    tokio-linux-aio = "0.1"

Next, add this to the root module of your crate:

    extern crate tokio_linux_aio;

## Examples

Once you have added the crate to your project you should be able to write something like this:

    // Let's use a standard thread pool
    let pool = futures_cpupool::CpuPool::new(5);

    // These are handle objects for memory regions
    let buffer = MemoryHandle::new();

    {
        // Here we go: create an execution context, which uses the pool for background work
        let context = AioContext::new(&pool, 10).unwrap();

        // Create a future to read from a given file (fd) at the given offset into our buffer
        let read_future = context
            .read(fd, 0, buffer)
            .map(move |result_buffer| {
                // do something upon successfully reading the data
                assert!(validate_block(&result_buffer));
            })
            .map_err(|err| {
                // do something else when things go wrong
                panic!("{:?}", err);
            });

        // Execute the future and wait for its completion
        let cpu_future = pool.spawn(read_future);
        let result = cpu_future.wait();

        // Should be OK
        assert!(result.is_ok());
    }

## License

This code is licensed under the [MIT license](https://github.com/hmwill/tokio-linux-aio/blob/master/LICENSE).