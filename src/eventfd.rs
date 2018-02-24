// ===============================================================================================
// Copyright (c) 2018 Hans-Martin Will
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
// ===============================================================================================

use std::io;
use std::mem;

use std::os::unix::io::RawFd;

use libc::{c_long, c_uint, close, eventfd, read, write, EAGAIN, O_CLOEXEC};

use futures;
use futures::Future;

use mio;

use tokio::executor;
use tokio::reactor;

use aio_bindings::{EFD_NONBLOCK, EFD_SEMAPHORE};

// -----------------------------------------------------------------------------------------------
// EventFd Implementation
// -----------------------------------------------------------------------------------------------

pub struct EventFdInner {
    pub fd: RawFd,
}

impl Drop for EventFdInner {
    fn drop(&mut self) {
        if self.fd >= 0 {
            unsafe { close(self.fd) };
        }
    }
}

impl mio::Evented for EventFdInner {
    fn register(
        &self,
        poll: &mio::Poll,
        token: mio::Token,
        interest: mio::Ready,
        opts: mio::PollOpt,
    ) -> io::Result<()> {
        mio::unix::EventedFd(&self.fd).register(poll, token, interest, opts)
    }

    fn reregister(
        &self,
        poll: &mio::Poll,
        token: mio::Token,
        interest: mio::Ready,
        opts: mio::PollOpt,
    ) -> io::Result<()> {
        mio::unix::EventedFd(&self.fd).reregister(poll, token, interest, opts)
    }

    fn deregister(&self, poll: &mio::Poll) -> io::Result<()> {
        mio::unix::EventedFd(&self.fd).deregister(poll)
    }
}

pub struct EventFd {
    pub evented: reactor::PollEvented<EventFdInner>,
}

impl EventFd {
    pub fn create(init: usize, semaphore: bool) -> Result<EventFd, io::Error> {
        let flags = if semaphore {
            O_CLOEXEC | EFD_NONBLOCK as i32 | EFD_SEMAPHORE as i32
        } else {
            O_CLOEXEC | EFD_NONBLOCK as i32
        };

        let fd = unsafe { eventfd(init as c_uint, flags) };

        if fd < 0 {
            Err(io::Error::last_os_error())
        } else {
            reactor::PollEvented::new(EventFdInner { fd }, &reactor::Handle::default())
                .map(|evented| EventFd { evented })
        }
    }

    pub fn read(&mut self) -> Result<futures::Async<u64>, io::Error> {
        match self.evented.poll_read() {
            futures::Async::NotReady => return Ok(futures::Async::NotReady),
            _ => (),
        };

        let fd = self.evented.get_ref().fd;
        let mut result: u64 = 0;

        let rc = unsafe { read(fd, mem::transmute(&mut result), mem::size_of::<u64>()) };

        if rc < 0 {
            let error = io::Error::last_os_error();

            if error.raw_os_error().unwrap() != EAGAIN {
                // this is a regular error
                return Err(io::Error::last_os_error());
            } else {
                if let Err(err) = self.evented.need_read() {
                    return Err(err);
                } else {
                    return Ok(futures::Async::NotReady);
                }
            }
        } else {
            if rc as usize != mem::size_of::<u64>() {
                panic!(
                    "Reading from an eventfd should transfer exactly {} bytes",
                    mem::size_of::<u64>()
                )
            }

            // eventfd should never return 0 value; it either blocks or fails with EAGAIN
            assert!(result != 0);
            Ok(futures::Async::Ready(result as u64))
        }
    }

    pub fn add(&mut self, increment: u64) -> Result<(), io::Error> {
        let fd = { self.evented.get_ref().fd };

        let result = unsafe { write(fd, mem::transmute(&increment), mem::size_of::<u64>()) };

        if result == -1 {
            Err(io::Error::last_os_error())
        } else {
            if result as usize != mem::size_of_val(&increment) {
                panic!(
                    "Writing to an eventfd should consume exactly {} bytes",
                    mem::size_of::<u64>()
                )
            }

            Ok(())
        }
    }
}

impl futures::Future for EventFd {
    type Item = u64;
    type Error = io::Error;

    fn poll(&mut self) -> Result<futures::Async<Self::Item>, Self::Error> {
        self.read()
    }
}

#[cfg(test)]
mod tests {
    use tokio::executor::current_thread;
    use futures::future::lazy;
    use super::*;

    #[test]
    fn read_eventfd_standard() {
        let efd = EventFd::create(2, false).unwrap();
        let result = efd.wait();

        assert!(result.is_ok());
        assert!(result.unwrap() == 2);
    }

    #[test]
    fn read_eventfd_semaphore() {
        let efd = EventFd::create(2, true).unwrap();
        let result = efd.wait();

        assert!(result.is_ok());
        assert!(result.unwrap() == 1);
    }

    #[test]
    fn read_add_eventfd() {
        current_thread::run(|_| {
            let efd = EventFd::create(0, false).unwrap();
            let fd = efd.evented.get_ref().fd;

            // The execution context is setup, futures may be executed.
            current_thread::spawn(efd.map(|res| {
                assert!(res == 1);
            }).map_err(|_| {
                panic!("Error!!!");
            }));

            current_thread::spawn(lazy(move || {
                let increment: u64 = 1;

                let result =
                    unsafe { write(fd, mem::transmute(&increment), mem::size_of::<u64>()) };
                assert!(result as usize == mem::size_of::<u64>());
                Ok(())
            }));
        });
    }
}
