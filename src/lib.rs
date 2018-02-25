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

extern crate aio_bindings;
extern crate futures;
extern crate libc;
extern crate mio;
extern crate rand;
extern crate tokio;

extern crate memmap;
extern crate futures_cpupool;

use std::cell;
use std::io;
use std::mem;
use std::ops;
use std::ptr;
use std::sync;

use std::os::unix::io::RawFd;

use libc::{c_int, c_long};

use futures::Future;

// Relevant symbols from the native bindings exposed via aio-bindings
use aio_bindings::{aio_context_t, io_event, iocb, syscall, timespec, __NR_io_destroy,
                   __NR_io_getevents, __NR_io_setup, __NR_io_submit, IOCB_CMD_PREAD,
                   IOCB_CMD_PWRITE, IOCB_FLAG_RESFD};

mod eventfd;

// -----------------------------------------------------------------------------------------------
// Inline functions that wrap the kernel calls for the entry points corresponding to Liux
// AIO functions
// -----------------------------------------------------------------------------------------------

// Initialize an AIO context for a given submission queue size within the kernel.
//
// See [io_setup(7)](http://man7.org/linux/man-pages/man2/io_setup.2.html) for details.
#[inline(always)]
unsafe fn io_setup(nr: c_long, ctxp: *mut aio_context_t) -> c_long {
    syscall(__NR_io_setup as c_long, nr, ctxp)
}

// Destroy an AIO context.
//
// See [io_destroy(7)](http://man7.org/linux/man-pages/man2/io_destroy.2.html) for details.
#[inline(always)]
unsafe fn io_destroy(ctx: aio_context_t) -> c_long {
    syscall(__NR_io_destroy as c_long, ctx)
}

// Submit a batch of IO operations.
//
// See [io_sumit(7)](http://man7.org/linux/man-pages/man2/io_submit.2.html) for details.
#[inline(always)]
unsafe fn io_submit(ctx: aio_context_t, nr: c_long, iocbpp: *mut *mut iocb) -> c_long {
    syscall(__NR_io_submit as c_long, ctx, nr, iocbpp)
}

// Retrieve completion events for previously submitted IO requests.
//
// See [io_getevents(7)](http://man7.org/linux/man-pages/man2/io_getevents.2.html) for details.
#[inline(always)]
unsafe fn io_getevents(
    ctx: aio_context_t,
    min_nr: c_long,
    max_nr: c_long,
    events: *mut io_event,
    timeout: *mut timespec,
) -> c_long {
    syscall(
        __NR_io_getevents as c_long,
        ctx,
        min_nr,
        max_nr,
        events,
        timeout,
    )
}

// -----------------------------------------------------------------------------------------------
// Bindings for Linux AIO start here
// -----------------------------------------------------------------------------------------------

// Common interface in order to initialize an embedded iocb control block within a future.
trait IocbSetup {
    fn setup(&mut self);
}

// Common data structures for futures returned by `AioContext`.
struct AioBaseFuture {
    // reference to the `AioContext` that controls the submission queue for asynchronous I/O
    context: sync::Arc<AioContextInner>,

    // the iocb control block that is used for queue submissions
    request: iocb,

    // state variable tracking if the I/O request associated with this instance has been submitted
    // to the kernel.
    submitted: bool,

    // place to capture the result of the I/O operation
    result: Option<Result<(), io::Error>>,
}

impl AioBaseFuture {
    fn poll(&mut self) -> Result<futures::Async<()>, io::Error> {
        if let Some(result) = self.result.take() {
            // procesing has completed
            return result.map(|_| futures::Async::Ready(()));
        }

        if !self.submitted {
            // See if we can secure a submission slot
            {
                let mut guard = self.context.capacity.write();

                match guard {
                    Ok(ref mut guard) => {
                        match guard.read() {
                            Err(err) => return Err(err),
                            Ok(futures::Async::NotReady) => return Ok(futures::Async::NotReady),
                            Ok(futures::Async::Ready(_)) => (),
                        }
                    },
                    Err(_) => panic!("TODO: Figure out how to handle this kind of error"),
                }

            }

            // submit the request
            let mut request_ptr_array: [*mut iocb; 1] = [&mut self.request as *mut iocb; 1];

            let result = unsafe {
                io_submit(
                    self.context.context,
                    1,
                    &mut request_ptr_array[0] as *mut *mut iocb,
                )
            };
            self.submitted = true;

            // if we have submission error, capture it as future result
            if result != 1 {
                return Err(io::Error::last_os_error());
            }
        }

        match self.context.completed.write() {
            Ok(ref mut guard) =>  {
                // See if we should look up completion events
                let available = match guard.event.read() {
                    Err(err) => return Err(err),
                    Ok(futures::Async::NotReady) => return Ok(futures::Async::NotReady),
                    Ok(futures::Async::Ready(n)) => n,
                };

                // get completion events; we retrieve exactly the number indiceted in the 
                // eventfd read-out.
                guard.completion_events.clear();
                assert!(available as usize <= guard.completion_events.capacity());

                unsafe {
                    let result = io_getevents(
                        self.context.context,
                        available as c_long,
                        available as c_long,
                        guard.completion_events.as_mut_ptr(),
                        ptr::null_mut::<timespec>(),
                    );

                    // adjust the vector size to the actual number of items returned
                    if result >= 0 {
                        guard.completion_events.set_len(result as usize);
                    } else {
                        return Err(io::Error::last_os_error());
                    }
                };

                for ref event in guard.completion_events.iter() {
                    let future: &mut AioBaseFuture = unsafe { mem::transmute(event.data) };
                    let result = event.res;

                    future.result = if result < 0 {
                        Some(Err(io::Error::from_raw_os_error(result as i32)))
                    } else {
                        Some(Ok(()))
                    };
                }

                // Release the kernel queue slots we just processed
                let length = guard.completion_events.len() as u64;
                if let Err(err) = guard.event.add(length) {
                    return Err(err);
                }
            },
            Err(_) => panic!("TODO: Figure out how to handle this error"),
        }

        if let Some(result) = self.result.take() {
            // procesing has completed
            result.map(|_| futures::Async::Ready(()))
        } else {
            // otherwise, register this future on the completion fd and return not ready
            let mut guard = self.context.completed.write().unwrap();

            guard.event.evented
                .need_read()
                .map(|_| futures::Async::NotReady)
        }
    }
}

/// Future returned as result of submitting a read request via `AioContext::read`.
pub struct AioReadResultFuture<ReadWriteHandle>
where
    ReadWriteHandle: ops::DerefMut<Target = [u8]>,
{
    // common AIO future state
    base: AioBaseFuture,

    // memory handle where data read from the underlying block device is being written to.
    buffer: ReadWriteHandle,
}

impl<ReadWriteHandle> IocbSetup for AioReadResultFuture<ReadWriteHandle>
where
    ReadWriteHandle: ops::DerefMut<Target = [u8]>,
{
    fn setup(&mut self) {
        unsafe {
            if self.base.request.aio_data == 0 {
                self.base.request.aio_data = mem::transmute(&mut self.base);
                self.base.request.aio_buf = mem::transmute(self.buffer.as_ptr());
            } else if self.base.request.aio_data != mem::transmute(&mut self.base) {
                panic!("Future was moved during I/O operation");
            }
        }
    }
}

impl<ReadWriteHandle> futures::Future for AioReadResultFuture<ReadWriteHandle>
where
    ReadWriteHandle: ops::DerefMut<Target = [u8]>,
{
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Result<futures::Async<Self::Item>, Self::Error> {
        self.setup();
        self.base.poll()
    }
}

/// Future returned as result of submitting a write request via `AioContext::write`.
pub struct AioWriteResultFuture<ReadOnlyHandle>
where
    ReadOnlyHandle: ops::Deref<Target = [u8]>,
{
    // common AIO future state
    base: AioBaseFuture,

    // memory handle where data written to the underlying block device is being read from.
    buffer: ReadOnlyHandle,
}

impl<ReadOnlyHandle> IocbSetup for AioWriteResultFuture<ReadOnlyHandle>
where
    ReadOnlyHandle: ops::Deref<Target = [u8]>,
{
    fn setup(&mut self) {
        unsafe {
            if self.base.request.aio_data == 0 {
                self.base.request.aio_data = mem::transmute(&mut self.base);
                self.base.request.aio_buf = mem::transmute(self.buffer.as_ptr());
            } else if self.base.request.aio_data != mem::transmute(&mut self.base) {
                panic!("Future was moved during I/O operation");
            }
        }
    }
}

impl<ReadOnlyHandle> futures::Future for AioWriteResultFuture<ReadOnlyHandle>
where
    ReadOnlyHandle: ops::Deref<Target = [u8]>,
{
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Result<futures::Async<Self::Item>, Self::Error> {
        self.setup();
        self.base.poll()
    }
}

struct CompletionState {
    // event fd indicating that I/O requests have been completed
    event: eventfd::EventFd,

    // vector of IO completion events; retrieved via io_getevents
    completion_events: Vec<io_event>,
}

impl CompletionState {
    fn new(nr: usize) -> Result<CompletionState, io::Error> {
        Ok(CompletionState {
            event: eventfd::EventFd::create(0, false)?,
            completion_events: Vec::with_capacity(nr),

        })
    }
}

struct AioContextInner {
    // the context handle for submitting AIO requests to the kernel
    context: aio_context_t,

    // event fd to signal that we can accept more I/O requests
    capacity: sync::RwLock<eventfd::EventFd>,

    // state to process completed I/O events
    completed: sync::RwLock<CompletionState>
}

impl AioContextInner {
    fn new(nr: usize) -> Result<AioContextInner, io::Error> {
        let mut context: aio_context_t = 0;

        unsafe {
            if io_setup(nr as c_long, &mut context) != 0 {
                return Err(io::Error::last_os_error());
            }
        };

        Ok(AioContextInner {
            context,
            capacity: sync::RwLock::new(eventfd::EventFd::create(nr, true)?),
            completed: sync::RwLock::new(CompletionState::new(nr)?),
        })
    }
}

impl Drop for AioContextInner {
    fn drop(&mut self) {
        let result = unsafe { io_destroy(self.context) };
        assert!(result == 0);
    }
}

pub struct AioContext {
    inner: sync::Arc<AioContextInner>,
}

/// AioContext provides a submission queue for asycnronous I/O operations to
/// block devices within the Linux kernel.
impl AioContext {
    /// Create a new AioContext that is driven by the provided event loop.
    ///
    /// # Params
    /// - nr: Number of submission slots fro IO requests
    pub fn new(nr: usize) -> Result<AioContext, io::Error> {
        Ok(AioContext {
            inner: sync::Arc::new(AioContextInner::new(nr)?),
        })
    }

    /// Initiate an asynchronous read operation on the given file descriptor for reading
    /// data from the provided absolute file offset into the buffer. The buffer also determines
    /// the number of bytes to be read, which should be a multiple of the underlying device block
    /// size.
    pub fn read<ReadWriteHandle>(
        &self,
        fd: RawFd,
        offset: u64,
        buffer: ReadWriteHandle,
    ) -> AioReadResultFuture<ReadWriteHandle>
    where
        ReadWriteHandle: ops::DerefMut<Target = [u8]>,
    {
        let len = buffer.len() as u64;

        // nothing really happens here until someone calls poll
        AioReadResultFuture {
            base: AioBaseFuture {
                context: self.inner.clone(),
                request: self.init_iocb(IOCB_CMD_PREAD, fd, offset, len),
                submitted: false,
                result: None,
            },
            buffer,
        }
    }

    /// Initiate an asynchronous write operation on the given file descriptor for writing
    /// data to the provided absolute file offset from the buffer. The buffer also determines
    /// the number of bytes to be written, which should be a multiple of the underlying device block
    /// size.
    pub fn write<ReadOnlyHandle>(
        &self,
        fd: RawFd,
        offset: u64,
        buffer: ReadOnlyHandle,
    ) -> AioWriteResultFuture<ReadOnlyHandle>
    where
        ReadOnlyHandle: ops::Deref<Target = [u8]>,
    {
        let len = buffer.len() as u64;

        // nothing really happens here until someone calls poll
        AioWriteResultFuture {
            base: AioBaseFuture {
                context: self.inner.clone(),
                request: self.init_iocb(IOCB_CMD_PWRITE, fd, offset, len),
                submitted: false,
                result: None,
            },
            buffer,
        }
    }

    fn init_iocb(&self, opcode: u32, fd: RawFd, offset: u64, len: u64) -> iocb {
        let mut result: iocb = unsafe { mem::zeroed() };

        result.aio_fildes = fd as u32;
        result.aio_offset = offset as i64;
        result.aio_nbytes = len;
        result.aio_lio_opcode = opcode as u16;
        result.aio_flags = IOCB_FLAG_RESFD;

        let guard = self.inner.completed.read().unwrap();

        result.aio_resfd = guard.event.evented.get_ref().fd as u32;

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::env;
    use std::fs;
    use std::io::Write;
    use std::os::unix::ffi::OsStrExt;
    use std::path;
    use std::sync;

    use rand::Rng;

    use tokio::executor::current_thread;

    use memmap;
    use futures_cpupool;

    use libc::{close, open, O_DIRECT, O_RDWR};

    // Create a temporary file name within the temporary directory configured in the environment.
    fn temp_file_name() -> path::PathBuf {
        let mut rng = rand::thread_rng();
        let mut result = env::temp_dir();
        let filename = format!("test-aio-{}.dat", rng.gen::<u64>());
        result.push(filename);
        result
    }

    // Create a temporary file with some content
    fn create_temp_file(path: &path::Path) {
        let mut file = fs::File::create(path).unwrap();
        let mut data: [u8; 16384] = [0; 16384];

        for index in 0..data.len() {
            data[index] = index as u8;
        }

        let result = file.write(&data);
        assert!(result.is_ok());
    }

    // Delete the temporary file
    fn remove_file(path: &path::Path) {
        let _ = fs::remove_file(path);
    }

    #[test]
    fn create_and_drop() {
        let _ = AioContext::new(10);
        // drop
    }

    struct MemoryBlock {
        bytes: sync::RwLock<memmap::MmapMut>,
    }

    impl MemoryBlock {
        fn new() -> MemoryBlock {
            MemoryBlock {
                // for real uses, we'll have a buffer pool with locks associated with individual pages
                // simplifying the logic here for test case development
                bytes: sync::RwLock::new(memmap::MmapMut::map_anon(8192).unwrap()),
            }
        }
    }

    struct MemoryHandle {
        block: sync::Arc<MemoryBlock>,
    }

    impl MemoryHandle {
        fn new() -> MemoryHandle {
            MemoryHandle {
                block: sync::Arc::new(MemoryBlock::new()),
            }
        }
    }

    impl Clone for MemoryHandle {
        fn clone(&self) -> MemoryHandle {
            MemoryHandle {
                block: self.block.clone(),
            }
        }
    }

    impl ops::Deref for MemoryHandle {
        type Target = [u8];

        fn deref(&self) -> &Self::Target {
            unsafe { mem::transmute(&(*self.block.bytes.read().unwrap())[..]) }
        }
    }

    impl ops::DerefMut for MemoryHandle {
        fn deref_mut(&mut self) -> &mut Self::Target {
            unsafe { mem::transmute(&mut (*self.block.bytes.write().unwrap())[..]) }
        }
    }

    #[test]
    fn read_block() {
        let file_name = temp_file_name();
        create_temp_file(&file_name);

        {
            let owned_fd = OwnedFd::new_from_raw_fd(unsafe {
                open(
                    mem::transmute(file_name.as_os_str().as_bytes().as_ptr()),
                    O_DIRECT | O_RDWR,
                )
            });
            let fd = owned_fd.fd;

            current_thread::run(move |_| {
                let context = AioContext::new(10).unwrap();
                let buffer = MemoryHandle::new();
                let result_buffer = buffer.clone();
                let read_future = context
                    .read(fd, 0, buffer)
                    .map(move |_| assert!(validate_block(&result_buffer)))
                    .map_err(|err| {
                        panic!("{:?}", err);
                    });

                current_thread::spawn(read_future);
            });
        }

        remove_file(&file_name);
    }

    #[test]
    fn read_block_mt() {
        let file_name = temp_file_name();
        create_temp_file(&file_name);

        {
            let owned_fd = OwnedFd::new_from_raw_fd(unsafe {
                open(
                    mem::transmute(file_name.as_os_str().as_bytes().as_ptr()),
                    O_DIRECT | O_RDWR,
                )
            });
            let fd = owned_fd.fd;

            let pool = futures_cpupool::CpuPool::new(4);

            {
                let context = AioContext::new(10).unwrap();
                let buffer = MemoryHandle::new();
                let result_buffer = buffer.clone();
                let read_future = context
                    .read(fd, 0, buffer)
                    .map(move |_| assert!(validate_block(&result_buffer)))
                    .map_err(|err| {
                        panic!("{:?}", err);
                    });

                let cpu_future = pool.spawn(read_future);
                let result = cpu_future.wait();

                assert!(result.is_ok());
            }
        }

        remove_file(&file_name);
    }

    fn validate_block(data: &[u8]) -> bool {
        for index in 0..data.len() {
            if data[index] != index as u8 {
                return false;
            }
        }

        true
    }

    struct OwnedFd {
        fd: RawFd,
    }

    impl OwnedFd {
        fn new_from_raw_fd(fd: RawFd) -> OwnedFd {
            OwnedFd { fd }
        }
    }

    impl Drop for OwnedFd {
        fn drop(&mut self) {
            let result = unsafe { close(self.fd) };
            assert!(result == 0);
        }
    }
}
