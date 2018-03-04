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

extern crate futures_cpupool;
extern crate memmap;

use std::io;
use std::mem;
use std::ops;
use std::ptr;
use std::sync;

use std::os::unix::io::RawFd;

use libc::{c_long, c_void, mlock};

use futures::Future;
use ops::Deref;

// Relevant symbols from the native bindings exposed via aio-bindings
use aio_bindings::{aio_context_t, io_event, iocb, syscall, timespec, __NR_io_destroy,
                   __NR_io_getevents, __NR_io_setup, __NR_io_submit, IOCB_CMD_PREAD,
                   IOCB_CMD_PWRITE, IOCB_FLAG_RESFD};

mod eventfd;

// -----------------------------------------------------------------------------------------------
// Inline functions that wrap the kernel calls for the entry points corresponding to Linux
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

// Common data structures for futures returned by `AioContext`.
struct AioBaseFuture {
    // reference to the `AioContext` that controls the submission queue for asynchronous I/O
    context: sync::Arc<AioContextInner>,

    // field values that we need to transfer into the IOCB

    // the I/O opcode
    opcode: u32,

    // file fd identifying the file to operate on
    fd: RawFd,

    // an absolute file offset, if applicable for the command
    offset: u64,

    // the base address of the transfer buffer, if applicable
    buf: u64,

    // the number of bytes to be transferred, if applicable
    len: u64,

    // state variable tracking if the I/O request associated with this instance has been submitted
    // to the kernel.
    submitted: sync::atomic::AtomicBool,

    // the associated eventfd
    state: Option<Box<RequestState>>,
}

// Common future base type for all asynchronous operations supperted by this API
impl AioBaseFuture {
    fn poll(&mut self) -> Result<futures::Async<()>, io::Error> {
        let invalid = -13579;

        if !self.submitted.load(sync::atomic::Ordering::Acquire) {
            // See if we can secure a submission slot
            if self.state.is_none() {
                let mut guard = self.context.capacity.write();

                match guard {
                    Ok(ref mut guard) => {
                        match guard.available.read() {
                            Err(err) => return Err(err),
                            Ok(futures::Async::NotReady) => return Ok(futures::Async::NotReady),
                            Ok(futures::Async::Ready(n)) => {
                                assert!(n == 1);

                                // retrieve an eventfd from the set of available ones and move it into the future
                                self.state = guard.state.pop();
                            }
                        }
                    }
                    Err(_) => panic!("TODO: Figure out how to handle this kind of error"),
                }
            }

            // Fill in the iocb data structure to be submitted to the kernel
            {
                assert!(self.state.is_some());
                let state = self.state.as_mut().unwrap();
                let state_addr = state.deref().deref() as *const RequestState;
                state.request.aio_data = unsafe { mem::transmute(state_addr) };
                state.request.aio_resfd = self.context.completed_fd as u32;
                state.request.aio_flags = IOCB_FLAG_RESFD;
                state.request.aio_fildes = self.fd as u32;
                state.request.aio_offset = self.offset as i64;
                state.request.aio_buf = self.buf;
                state.request.aio_nbytes = self.len;
                state.request.aio_lio_opcode = self.opcode as u16;
                state.result = invalid;
            }

            // submit the request
            let mut request_ptr_array: [*mut iocb; 1] =
                [&mut self.state.as_mut().unwrap().request as *mut iocb; 1];

            let result = unsafe {
                io_submit(
                    self.context.context,
                    1,
                    &mut request_ptr_array[0] as *mut *mut iocb,
                )
            };

            // mark that we performed the submission
            self.submitted.store(true, sync::atomic::Ordering::Release);

            // if we have submission error, capture it as future result
            if result != 1 {
                return Err(io::Error::last_os_error());
            } else {
                // register the current task to be notified upon I/O completion
                self.state.as_mut().unwrap().completed.register();

                // wait to be polled again
                return Ok(futures::Async::NotReady);
            }
        } else {
            let result_code = self.state.as_ref().unwrap().result;

            // triggered in error?
            if result_code == invalid {
                return Ok(futures::Async::NotReady);
            }

            // Release the kernel queue slot and the state variable that we just processed
            match self.context.capacity.write() {
                Ok(ref mut guard) => {
                    guard.state.push(self.state.take().unwrap());
                    guard.available.add(1)?;
                }
                Err(_) => panic!("TODO: Figure out how to handle this kind of error"),
            }

            if result_code < 0 {
                Err(io::Error::from_raw_os_error(result_code as i32))
            } else {
                Ok(futures::Async::Ready(()))
            }
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
    // Holding on to this value is important in the case where it implements Drop.
    buffer: ReadWriteHandle,
}

impl<ReadWriteHandle> futures::Future for AioReadResultFuture<ReadWriteHandle>
where
    ReadWriteHandle: ops::DerefMut<Target = [u8]>,
{
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Result<futures::Async<Self::Item>, Self::Error> {
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
    // Holding on to this value is important in the case where it implements Drop.
    buffer: ReadOnlyHandle,
}

impl<ReadOnlyHandle> futures::Future for AioWriteResultFuture<ReadOnlyHandle>
where
    ReadOnlyHandle: ops::Deref<Target = [u8]>,
{
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Result<futures::Async<Self::Item>, Self::Error> {
        self.base.poll()
    }
}

// State information that is associated with an I/O request that is currently in flight.
struct RequestState {
    // Linux kernal I/O control block which can be submitted to io_submit
    request: iocb,

    // Concurrency primitive to notify completion to the associated future
    completed: futures::task::AtomicTask,

    // the result value of the I/O request
    result: c_long,
}

// Shared state within AioContext that is backing I/O requests as represented by the individual futures.
struct Capacity {
    // event fd to signal that we can accept more I/O requests
    available: eventfd::EventFd,

    // pre-allocated eventfds and iocbs that are associated with scheduled I/O requests
    state: Vec<Box<RequestState>>,
}

impl Capacity {
    fn new(nr: usize) -> Result<Capacity, io::Error> {
        let available = eventfd::EventFd::create(nr, true)?;

        let mut state = Vec::with_capacity(nr);

        // using a for loop to properly handle the error case
        // range map collect would only allow for using unwrap(), thereby turning an error into a panic
        for _ in 0..nr {
            state.push(Box::new(RequestState {
                request: unsafe { mem::zeroed() },
                completed: futures::task::AtomicTask::new(),
                result: 0,
            }));
        }

        Ok(Capacity { available, state })
    }
}

// The inner state, which is shared between the AioContext object returned to clients and
// used internally by futues in flight.
struct AioContextInner {
    // the context handle for submitting AIO requests to the kernel
    context: aio_context_t,

    // the fd embedded in the completed eventfd, which can be passed to kernel functions;
    // the handle is managed by the Eventfd object that is owned by the AioPollFuture
    // that we spawn when creating an AioContext.
    completed_fd: RawFd,

    // pre-allocated eventfds and a capacity semaphore
    capacity: sync::RwLock<Capacity>,
}

impl AioContextInner {
    fn new(fd: RawFd, nr: usize) -> Result<AioContextInner, io::Error> {
        let mut context: aio_context_t = 0;

        unsafe {
            if io_setup(nr as c_long, &mut context) != 0 {
                return Err(io::Error::last_os_error());
            }
        };

        Ok(AioContextInner {
            context,
            capacity: sync::RwLock::new(Capacity::new(nr)?),
            completed_fd: fd,
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
    poll_task_handle: futures::sync::oneshot::SpawnHandle<(), io::Error>,
}

/// AioContext provides a submission queue for asycnronous I/O operations to
/// block devices within the Linux kernel.
impl AioContext {
    /// Create a new AioContext that is driven by the provided event loop.
    ///
    /// # Params
    /// - executor: The executor used to spawn the background polling task
    /// - nr: Number of submission slots fro IO requests
    pub fn new<E>(executor: &E, nr: usize) -> Result<AioContext, io::Error>
    where
        E: futures::future::Executor<futures::sync::oneshot::Execute<AioPollFuture>>,
    {
        let eventfd = eventfd::EventFd::create(0, false)?;
        let fd = eventfd.evented.get_ref().fd;

        let inner = AioContextInner::new(fd, nr)?;
        let context = inner.context;

        let poll_future = AioPollFuture {
            context,
            eventfd,
            events: Vec::with_capacity(nr),
        };

        Ok(AioContext {
            inner: sync::Arc::new(inner),
            poll_task_handle: futures::sync::oneshot::spawn(poll_future, executor),
        })
    }

    /// Initiate an asynchronous read operation on the given file descriptor for reading
    /// data from the provided absolute file offset into the buffer. The buffer also determines
    /// the number of bytes to be read, which should be a multiple of the underlying device block
    /// size.
    ///
    /// # Params:
    /// - fd: The file descriptor of the file from which to read
    /// - offset: The file offset where we want to read from
    /// - buffer: A buffer to receive the read results
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
                opcode: IOCB_CMD_PREAD,
                fd,
                offset,
                len,
                buf: unsafe { mem::transmute(buffer.as_ptr()) },
                submitted: sync::atomic::AtomicBool::new(false),
                state: None,
            },
            buffer,
        }
    }

    /// Initiate an asynchronous write operation on the given file descriptor for writing
    /// data to the provided absolute file offset from the buffer. The buffer also determines
    /// the number of bytes to be written, which should be a multiple of the underlying device block
    /// size.
    ///
    /// # Params:
    /// - fd: The file descriptor of the file to which to write
    /// - offset: The file offset where we want to write to
    /// - buffer: A buffer holding the data to be written
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
                opcode: IOCB_CMD_PWRITE,
                fd,
                offset,
                len,
                buf: unsafe { mem::transmute(buffer.as_ptr()) },
                submitted: sync::atomic::AtomicBool::new(false),
                state: None,
            },
            buffer,
        }
    }
}

// A future spawned as background task to retrieve I/O completion events from the kernel
// and distributing the results to the current futures in flight.
struct AioPollFuture {
    // the context handle for retrieving AIO completions from the kernel
    context: aio_context_t,

    // the eventfd on which the kernel will notify I/O completions
    eventfd: eventfd::EventFd,

    // a buffer to retrieve completion status from the kernel
    events: Vec<io_event>,
}

impl futures::Future for AioPollFuture {
    type Item = ();
    type Error = io::Error;

    // This poll function will never return completion
    fn poll(&mut self) -> Result<futures::Async<Self::Item>, Self::Error> {
        loop {
            let available = match self.eventfd.read() {
                Err(err) => return Err(err),
                Ok(futures::Async::NotReady) => return Ok(futures::Async::NotReady),
                Ok(futures::Async::Ready(value)) => value as usize,
            };

            assert!(available > 0);
            self.events.clear();

            unsafe {
                let result = io_getevents(
                    self.context,
                    available as c_long,
                    available as c_long,
                    self.events.as_mut_ptr(),
                    ptr::null_mut::<timespec>(),
                );

                // adjust the vector size to the actual number of items returned
                if result < 0 {
                    return Err(io::Error::last_os_error());
                }

                assert!(result as usize == available);
                self.events.set_len(available);
            };

            for ref event in &self.events {
                let request_state: &mut RequestState = unsafe { mem::transmute(event.data) };

                request_state.result = event.res;
                request_state.completed.notify();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Test code starts here
// ---------------------------------------------------------------------------

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

        let result = file.write(&data).and_then(|_| file.sync_all());
        assert!(result.is_ok());
    }

    // Delete the temporary file
    fn remove_file(path: &path::Path) {
        let _ = fs::remove_file(path);
    }

    #[test]
    fn create_and_drop() {
        let pool = futures_cpupool::CpuPool::new(3);
        let _context = AioContext::new(&pool, 10).unwrap();
    }

    struct MemoryBlock {
        bytes: sync::RwLock<memmap::MmapMut>,
    }

    impl MemoryBlock {
        fn new() -> MemoryBlock {
            let map = memmap::MmapMut::map_anon(8192).unwrap();
            unsafe { mlock(map.as_ref().as_ptr() as *const c_void, map.len()) };

            MemoryBlock {
                // for real uses, we'll have a buffer pool with locks associated with individual pages
                // simplifying the logic here for test case development
                bytes: sync::RwLock::new(map),
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

    /*
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
                let context =
                    AioContext::new(|f| current_thread::spawn(f.map_err(|_| ())), 10).unwrap();
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
*/

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

            let pool = futures_cpupool::CpuPool::new(5);
            let buffer = MemoryHandle::new();
            let result_buffer = buffer.clone();

            {
                let context = AioContext::new(&pool, 10).unwrap();
                let read_future = context
                    .read(fd, 0, buffer)
                    .map(move |_| {
                        assert!(validate_block(&result_buffer));
                    })
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

    #[test]
    fn read_invalid_fd() {
        let fd = 2431;

        let pool = futures_cpupool::CpuPool::new(5);
        let buffer = MemoryHandle::new();
        let result_buffer = buffer.clone();

        {
            let context = AioContext::new(&pool, 10).unwrap();
            let read_future = context
                .read(fd, 0, buffer)
                .map(move |_| {
                    assert!(false);
                })
                .map_err(|err| {
                    assert!(err.kind() == io::ErrorKind::Other);
                    err
                });

            let cpu_future = pool.spawn(read_future);
            let result = cpu_future.wait();

            assert!(result.is_err());
        }
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
