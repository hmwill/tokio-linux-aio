# tokio-linux-aio

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

## License

This code is licensed under the [MIT license](https://github.com/hmwill/tokio-linux-aio/blob/master/LICENSE).