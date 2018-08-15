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

//! Tokio Bindings for Linux Kernel AIO
//!
//! This package provides an integration of Linux kernel-level asynchronous I/O to the
//! [Tokio platform](https://tokio.rs/).
//!
//! Linux kernel-level asynchronous I/O is different from the [Posix AIO library](http://man7.org/linux/man-pages/man7/aio.7.html).
//! Posix AIO is implemented using a pool of userland threads, which invoke regular, blocking system
//! calls to perform file I/O. [Linux kernel-level AIO](http://lse.sourceforge.net/io/aio.html), on the
//! other hand, provides kernel-level asynchronous scheduling of I/O operations to the underlying block device.
//!
//! The core abstraction exposed by this library is the `AioContext`, which essentially wraps
//! a kernel-level I/O submission queue with limited capacity. The capacity of the underlying queue
//! is a constructor argument when creating an instance of `AioContext`. Once created, the context
//! can be used to issue read and write requests. Each such invocations will create a suitable instance
//! of `futures::Future`, which can be executed within the context of Tokio.
//!
//! There's a few gotchas to be aware of when using this library:
//!
//! 1. Linux AIO requires the underlying file to be opened in direct mode (`O_DIRECT`), bypassing
//! any other buffering at the OS level. If you attempt to use this library on files opened regularly,
//! likely it won't work.
//!
//! 2. Because Linux AIO operates on files in direct mode, by corrollary the memory buffers associated
//! with read/write requests need to be suitable for direct DMA transfers. This means that those buffers
//! should be aligned to hardware page boundaries, and the memory needs to be mapped to pysical RAM.
//! The best way to accomplish this is to have a mmapped region that is locked in physical memory.
//!
//! 3. Due to the asynchronous nature of this library, memory buffers are represented using generic
//! handle types. For the purpose of the inner workings of this library, the important aspect is that
//! those handle types can be dereferenced into a `&[u8]` or, respectively, a `&mut [u8]` type. Because
//! we hand off those buffers to the kernel (and ultimately hardware DMA) it is mandatory that those
//! bytes slices have a fixed address in main memory during I/O processing.
//!
//! 4. The general idea is that those generic handle types for memory access can implement smart
//! pointer semantics. For example, a conceivable implementation of a memory handle type is a smart
//! pointer that acquires a write-lock on a page while a data transfer is in progress, and releases
//! such a lock once the operation has completed.

extern crate aio_bindings;
extern crate futures;
extern crate futures_cpupool;
extern crate libc;
extern crate memmap;
extern crate mio;
extern crate rand;
extern crate tokio;

use std::convert;
use std::error;
use std::fmt;
use std::io;
use std::mem;
use std::ops;
use std::ptr;

use std::os::unix::io::RawFd;

use libc::{c_long, c_void, mlock};

use futures::Future;
use ops::Deref;

// local modules
mod aio;
mod eventfd;
mod sync;

// -----------------------------------------------------------------------------------------------
// Bindings for Linux AIO start here
// -----------------------------------------------------------------------------------------------

// field values that we need to transfer into a kernel IOCB
struct IocbInfo {
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

    // flags to provide additional parameters 
    flags: u32,
}

// State information that is associated with an I/O request that is currently in flight.
struct RequestState {
    // Linux kernal I/O control block which can be submitted to io_submit
    request: aio::iocb,

    // Concurrency primitive to notify completion to the associated future
    completed_receiver: futures::sync::oneshot::Receiver<c_long>,

    // We have both sides of a oneshot channel here
    completed_sender: Option<futures::sync::oneshot::Sender<c_long>>,
}

// Common data structures for futures returned by `AioContext`.
struct AioBaseFuture {
    // reference to the `AioContext` that controls the submission queue for asynchronous I/O
    context: std::sync::Arc<AioContextInner>,

    // request information captured for the kernel request
    iocb_info: IocbInfo,

    // the associated request state
    state: Option<Box<RequestState>>,

    // acquire future
    acquire_state: Option<sync::SemaphoreHandle>,
}

impl AioBaseFuture {
    // Attempt to submit the I/O request; this may need to wait until a submission slot is
    // available.
    fn submit_request(&mut self) -> Result<futures::Async<()>, io::Error> {
        if self.state.is_none() {
            // See if we can secure a submission slot
            if self.acquire_state.is_none() {
                self.acquire_state = Some(self.context.have_capacity.acquire());
            }

            match self.acquire_state.as_mut().unwrap().poll() {
                Err(err) => return Err(err),
                Ok(futures::Async::NotReady) => return Ok(futures::Async::NotReady),
                Ok(futures::Async::Ready(_)) => {
                    // retrieve a state container from the set of available ones and move it into the future
                    let mut guard = self.context.capacity.write();
                    match guard {
                        Ok(ref mut guard) => {
                            self.state = guard.state.pop();
                        }
                        Err(_) => panic!("TODO: Figure out how to handle this kind of error"),
                    }
                }
            }

            assert!(self.state.is_some());
            let state = self.state.as_mut().unwrap();
            let state_addr = state.deref().deref() as *const RequestState;

            // Fill in the iocb data structure to be submitted to the kernel
            state.request.aio_data = unsafe { mem::transmute(state_addr) };
            state.request.aio_resfd = self.context.completed_fd as u32;
            state.request.aio_flags = aio::IOCB_FLAG_RESFD | self.iocb_info.flags;
            state.request.aio_fildes = self.iocb_info.fd as u32;
            state.request.aio_offset = self.iocb_info.offset as i64;
            state.request.aio_buf = self.iocb_info.buf;
            state.request.aio_nbytes = self.iocb_info.len;
            state.request.aio_lio_opcode = self.iocb_info.opcode as u16;

            // attach synchronization primitives that are used to indicate completion of this request
            let (sender, receiver) = futures::sync::oneshot::channel();
            state.completed_receiver = receiver;
            state.completed_sender = Some(sender);

            // submit the request
            let mut request_ptr_array: [*mut aio::iocb; 1] =
                [&mut state.request as *mut aio::iocb; 1];

            let result = unsafe {
                aio::io_submit(
                    self.context.context,
                    1,
                    &mut request_ptr_array[0] as *mut *mut aio::iocb,
                )
            };

            // if we have submission error, capture it as future result
            if result != 1 {
                return Err(io::Error::last_os_error());
            }
        }

        Ok(futures::Async::Ready(()))
    }

    // Attempt to retrieve the result of a previously submitted I/O request; this may need to
    // wait until the I/O operation has been completed
    fn retrieve_result(&mut self) -> Result<futures::Async<()>, io::Error> {
        // Check if we have received a notification indicating completion of the I/O request
        let result_code = match self.state.as_mut().unwrap().completed_receiver.poll() {
            Err(err) => return Err(io::Error::new(io::ErrorKind::Other, err)),
            Ok(futures::Async::NotReady) => return Ok(futures::Async::NotReady),
            Ok(futures::Async::Ready(n)) => n,
        };

        // Release the kernel queue slot and the state variable that we just processed
        match self.context.capacity.write() {
            Ok(ref mut guard) => {
                guard.state.push(self.state.take().unwrap());
            }
            Err(_) => panic!("TODO: Figure out how to handle this kind of error"),
        }

        // notify others that we release a state slot
        self.context.have_capacity.release();

        if result_code < 0 {
            Err(io::Error::from_raw_os_error(result_code as i32))
        } else {
            Ok(futures::Async::Ready(()))
        }
    }
}

// Common future base type for all asynchronous operations supperted by this API
impl futures::Future for AioBaseFuture {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Result<futures::Async<()>, io::Error> {
        let result = self.submit_request();

        match result {
            Ok(futures::Async::Ready(())) => self.retrieve_result(),
            Ok(futures::Async::NotReady) => Ok(futures::Async::NotReady),
            Err(err) => Err(err),
        }
    }
}

/// An error type for I/O operations that allows us to return the memory handle in failure cases.
pub struct AioError<Handle> {
    // The buffer handle that we want to return to the caller
    pub buffer: Handle,

    // The error value
    pub error: io::Error,
}

impl<Handle> fmt::Debug for AioError<Handle> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        self.error.fmt(f)
    }
}

impl<Handle> fmt::Display for AioError<Handle> {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        self.error.fmt(f)
    }
}

impl<Handle> error::Error for AioError<Handle> {
    fn description(&self) -> &str {
        self.error.description()
    }

    fn cause(&self) -> Option<&error::Error> {
        self.error.cause()
    }
}

/// Future returned as result of submitting a read request via `AioContext::read`.
pub struct AioReadResultFuture<ReadWriteHandle>
where
    ReadWriteHandle: convert::AsMut<[u8]>,
{
    // common AIO future state
    base: AioBaseFuture,

    // memory handle where data read from the underlying block device is being written to.
    // Holding on to this value is important in the case where it implements Drop.
    buffer: Option<ReadWriteHandle>,
}

impl<ReadWriteHandle> futures::Future for AioReadResultFuture<ReadWriteHandle>
where
    ReadWriteHandle: convert::AsMut<[u8]>,
{
    type Item = ReadWriteHandle;
    type Error = AioError<ReadWriteHandle>;

    fn poll(&mut self) -> Result<futures::Async<Self::Item>, Self::Error> {
        self.base
            .poll()
            .map(|val| val.map(|_| self.buffer.take().unwrap()))
            .map_err(|err| AioError {
                buffer: self.buffer.take().unwrap(),
                error: err,
            })
    }
}

/// Future returned as result of submitting a write request via `AioContext::write`.
pub struct AioWriteResultFuture<ReadOnlyHandle>
where
    ReadOnlyHandle: convert::AsRef<[u8]>,
{
    // common AIO future state
    base: AioBaseFuture,

    // memory handle where data written to the underlying block device is being read from.
    // Holding on to this value is important in the case where it implements Drop.
    buffer: Option<ReadOnlyHandle>,
}

impl<ReadOnlyHandle> futures::Future for AioWriteResultFuture<ReadOnlyHandle>
where
    ReadOnlyHandle: convert::AsRef<[u8]>,
{
    type Item = ReadOnlyHandle;
    type Error = AioError<ReadOnlyHandle>;

    fn poll(&mut self) -> Result<futures::Async<Self::Item>, Self::Error> {
        self.base
            .poll()
            .map(|val| val.map(|_| self.buffer.take().unwrap()))
            .map_err(|err| AioError {
                buffer: self.buffer.take().unwrap(),
                error: err,
            })
    }
}

/// Future returned as result of submitting a write request via `AioContext::sync` or
/// `AioContext::data_sync`.
pub struct AioSyncResultFuture
{
    // common AIO future state
    base: AioBaseFuture,
}

impl futures::Future for AioSyncResultFuture
{
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Result<futures::Async<Self::Item>, Self::Error> {
        self.base.poll()
    }
}

// A future spawned as background task to retrieve I/O completion events from the kernel
// and distributing the results to the current futures in flight.
pub struct AioPollFuture {
    // the context handle for retrieving AIO completions from the kernel
    context: aio::aio_context_t,

    // the eventfd on which the kernel will notify I/O completions
    eventfd: eventfd::EventFd,

    // a buffer to retrieve completion status from the kernel
    events: Vec<aio::io_event>,
}

impl futures::Future for AioPollFuture {
    type Item = ();
    type Error = io::Error;

    // This poll function will never return completion
    fn poll(&mut self) -> Result<futures::Async<Self::Item>, Self::Error> {
        loop {
            // check the eventfd for completed I/O operations
            let available = match self.eventfd.read() {
                Err(err) => return Err(err),
                Ok(futures::Async::NotReady) => return Ok(futures::Async::NotReady),
                Ok(futures::Async::Ready(value)) => value as usize,
            };

            assert!(available > 0);
            self.events.clear();

            unsafe {
                let result = aio::io_getevents(
                    self.context,
                    available as c_long,
                    available as c_long,
                    self.events.as_mut_ptr(),
                    ptr::null_mut::<aio::timespec>(),
                );

                // adjust the vector size to the actual number of items returned
                if result < 0 {
                    return Err(io::Error::last_os_error());
                }

                assert!(result as usize == available);
                self.events.set_len(available);
            };

            // dispatch the retrieved events to the associated futures
            for ref event in &self.events {
                let request_state: &mut RequestState = unsafe { mem::transmute(event.data) };
                request_state
                    .completed_sender
                    .take()
                    .unwrap()
                    .send(event.res)
                    .unwrap();
            }
        }
    }
}

// Shared state within AioContext that is backing I/O requests as represented by the individual futures.
struct Capacity {
    // pre-allocated eventfds and iocbs that are associated with scheduled I/O requests
    state: Vec<Box<RequestState>>,
}

impl Capacity {
    fn new(nr: usize) -> Result<Capacity, io::Error> {
        let mut state = Vec::with_capacity(nr);

        // using a for loop to properly handle the error case
        // range map collect would only allow for using unwrap(), thereby turning an error into a panic
        for _ in 0..nr {
            let (_, receiver) = futures::sync::oneshot::channel();

            state.push(Box::new(RequestState {
                request: unsafe { mem::zeroed() },
                completed_receiver: receiver,
                completed_sender: None,
            }));
        }

        Ok(Capacity { state })
    }
}

// The inner state, which is shared between the AioContext object returned to clients and
// used internally by futures in flight.
struct AioContextInner {
    // the context handle for submitting AIO requests to the kernel
    context: aio::aio_context_t,

    // the fd embedded in the completed eventfd, which can be passed to kernel functions;
    // the handle is managed by the Eventfd object that is owned by the AioPollFuture
    // that we spawn when creating an AioContext.
    completed_fd: RawFd,

    // do we have capacity?
    have_capacity: sync::Semaphore,

    // pre-allocated eventfds and a capacity semaphore
    capacity: std::sync::RwLock<Capacity>,

    // handle for the spawned background task; dropping it will cancel the task
    // we are using an Option value with delayed initialization to keep the generic
    // executor type parameter out of AioContextInner
    poll_task_handle: Option<futures::sync::oneshot::SpawnHandle<(), io::Error>>,
}

impl AioContextInner {
    fn new(fd: RawFd, nr: usize) -> Result<AioContextInner, io::Error> {
        let mut context: aio::aio_context_t = 0;

        unsafe {
            if aio::io_setup(nr as c_long, &mut context) != 0 {
                return Err(io::Error::last_os_error());
            }
        };

        Ok(AioContextInner {
            context,
            capacity: std::sync::RwLock::new(Capacity::new(nr)?),
            have_capacity: sync::Semaphore::new(nr),
            completed_fd: fd,
            poll_task_handle: None,
        })
    }
}

impl Drop for AioContextInner {
    fn drop(&mut self) {
        let result = unsafe { aio::io_destroy(self.context) };
        assert!(result == 0);
    }
}

/// AioContext provides a submission queue for asycnronous I/O operations to
/// block devices within the Linux kernel.
#[derive(Clone)]
pub struct AioContext {
    inner: std::sync::Arc<AioContextInner>,
}

/// Synchronization levels associated with I/O operations
#[derive(Copy, Clone, Debug)]
pub enum SyncLevel {
    /// No synchronization requirement
    None = 0,

    /// Data is written to device, but not necessarily meta data
    Data = aio::RWF_DSYNC as isize,

    /// Data and associated meta data is written to device
    Full = aio::RWF_SYNC as isize,
}

impl AioContext {
    /// Create a new AioContext that is driven by the provided event loop.
    ///
    /// # Params
    /// - executor: The executor used to spawn the background polling task
    /// - nr: Number of submission slots for IO requests
    pub fn new<E>(executor: &E, nr: usize) -> Result<AioContext, io::Error>
    where
        E: futures::future::Executor<futures::sync::oneshot::Execute<AioPollFuture>>,
    {
        // An eventfd that we use for I/O completion notifications from the kernel
        let eventfd = eventfd::EventFd::create(0, false)?;
        let fd = eventfd.evented.get_ref().fd;

        let mut inner = AioContextInner::new(fd, nr)?;
        let context = inner.context;

        let poll_future = AioPollFuture {
            context,
            eventfd,
            events: Vec::with_capacity(nr),
        };

        inner.poll_task_handle = Some(futures::sync::oneshot::spawn(poll_future, executor));

        Ok(AioContext {
            inner: std::sync::Arc::new(inner),
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
        mut buffer_obj: ReadWriteHandle,
    ) -> AioReadResultFuture<ReadWriteHandle>
    where
        ReadWriteHandle: convert::AsMut<[u8]>,
    {
        let (ptr, len) = {
            let buffer = buffer_obj.as_mut();
            let len = buffer.len() as u64;
            let ptr = unsafe { mem::transmute(buffer.as_ptr()) };
            (ptr, len)
        };

        // nothing really happens here until someone calls poll
        AioReadResultFuture {
            base: AioBaseFuture {
                context: self.inner.clone(),
                iocb_info: IocbInfo {
                    opcode: aio::IOCB_CMD_PREAD,
                    fd,
                    offset,
                    len,
                    buf: ptr,
                    flags: 0,
                },
                state: None,
                acquire_state: None,
            },
            buffer: Some(buffer_obj),
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
        ReadOnlyHandle: convert::AsRef<[u8]>,
    {
        self.write_sync(fd, offset, buffer, SyncLevel::None)
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
    /// - sync_level: A synchronization level to apply for this write operation
    pub fn write_sync<ReadOnlyHandle>(
        &self,
        fd: RawFd,
        offset: u64,
        buffer_obj: ReadOnlyHandle,
        sync_level: SyncLevel
    ) -> AioWriteResultFuture<ReadOnlyHandle>
    where
        ReadOnlyHandle: convert::AsRef<[u8]>,
    {
        let (ptr, len) = {
            let buffer = buffer_obj.as_ref();
            let len = buffer.len() as u64;
            let ptr = unsafe { mem::transmute(buffer.as_ptr()) };
            (ptr, len)
        };

        // nothing really happens here until someone calls poll
        AioWriteResultFuture {
            base: AioBaseFuture {
                context: self.inner.clone(),
                iocb_info: IocbInfo {
                    opcode: aio::IOCB_CMD_PWRITE,
                    fd,
                    offset,
                    len,
                    buf: ptr,
                    flags: sync_level as u32,
                },
                state: None,
                acquire_state: None,
            },
            buffer: Some(buffer_obj),
        }
    }

    /// Initiate an asynchronous sync operation on the given file descriptor.
    /// 
    /// __Caveat:__ While this operation is defined in the ABI, this command is known to
    /// fail with an invalid argument error (`EINVAL`) in many, if not all, cases. You are kind of
    /// on your own.
    ///
    /// # Params:
    /// - fd: The file descriptor of the file to which to write
    pub fn sync(
        &self,
        fd: RawFd,
    ) -> AioSyncResultFuture
    {
        // nothing really happens here until someone calls poll
        AioSyncResultFuture {
            base: AioBaseFuture {
                context: self.inner.clone(),
                iocb_info: IocbInfo {
                    opcode: aio::IOCB_CMD_FSYNC,
                    fd,
                    buf: 0,
                    len: 0,
                    offset: 0,
                    flags: 0,
                },
                state: None,
                acquire_state: None,
            },
        }
    }


    /// Initiate an asynchronous data sync operation on the given file descriptor.
    ///
    /// __Caveat:__ While this operation is defined in the ABI, this command is known to
    /// fail with an invalid argument error (`EINVAL`) in many, if not all, cases. You are kind of
    /// on your own.
    ///
    /// # Params:
    /// - fd: The file descriptor of the file to which to write
    pub fn data_sync(
        &self,
        fd: RawFd,
    ) -> AioSyncResultFuture
    {
        // nothing really happens here until someone calls poll
        AioSyncResultFuture {
            base: AioBaseFuture {
                context: self.inner.clone(),
                iocb_info: IocbInfo {
                    opcode: aio::IOCB_CMD_FDSYNC,
                    fd,
                    buf: 0,
                    len: 0,
                    offset: 0,
                    flags: 0,
                },
                state: None,
                acquire_state: None,
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Test code starts here
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::borrow::{Borrow, BorrowMut};
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

    const FILE_SIZE: u64 = 1024 * 512;

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
        let mut data: [u8; FILE_SIZE as usize] = [0; FILE_SIZE as usize];

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

    impl convert::AsRef<[u8]> for MemoryHandle {
        fn as_ref(&self) -> &[u8] {
            unsafe { mem::transmute(&(*self.block.bytes.read().unwrap())[..]) }
        }
    }

    impl convert::AsMut<[u8]> for MemoryHandle {
        fn as_mut(&mut self) -> &mut [u8] {
            unsafe { mem::transmute(&mut (*self.block.bytes.write().unwrap())[..]) }
        }
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

            let pool = futures_cpupool::CpuPool::new(5);
            let buffer = MemoryHandle::new();

            {
                let context = AioContext::new(&pool, 10).unwrap();
                let read_future = context
                    .read(fd, 0, buffer)
                    .map(move |result_buffer| {
                        assert!(validate_block(result_buffer.as_ref()));
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
    fn write_block_mt() {
        use io::{Read, Seek};

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
            let mut buffer = MemoryHandle::new();
            fill_pattern(65u8, buffer.as_mut());

            {
                let context = AioContext::new(&pool, 2).unwrap();
                let write_future = context.write(fd, 16384, buffer).map_err(|err| {
                    panic!("{:?}", err);
                });

                let cpu_future = pool.spawn(write_future);
                let result = cpu_future.wait();

                assert!(result.is_ok());
            }
        }

        let mut file = fs::File::open(&file_name).unwrap();
        file.seek(io::SeekFrom::Start(16384)).unwrap();

        let mut read_buffer: [u8; 8192] = [0u8; 8192];
        file.read(&mut read_buffer).unwrap();

        assert!(validate_pattern(65u8, &read_buffer));
    }

    #[test]
    fn write_block_sync_mt() {
        // At this point, this test merely verifies that data ends up being written to
        // a file in the presence of synchronization flags. What the test does not verify
        // as that the specific synchronization guarantees are being fulfilled.
        use io::{Read, Seek};

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
            let context = AioContext::new(&pool, 2).unwrap();

            {
                let mut buffer = MemoryHandle::new();
                fill_pattern(65u8, buffer.as_mut());
                let write_future = context.write(fd, 16384, buffer).map_err(|err| {
                    panic!("{:?}", err);
                });

                let cpu_future = pool.spawn(write_future);
                let result = cpu_future.wait();

                assert!(result.is_ok());
            }

            {
                let mut buffer = MemoryHandle::new();
                fill_pattern(66u8, buffer.as_mut());
                let write_future = context.write(fd, 32768, buffer).map_err(|err| {
                    panic!("{:?}", err);
                });

                let cpu_future = pool.spawn(write_future);
                let result = cpu_future.wait();

                assert!(result.is_ok());
            }

            {
                let mut buffer = MemoryHandle::new();
                fill_pattern(67u8, buffer.as_mut());
                let write_future = context.write(fd, 49152, buffer).map_err(|err| {
                    panic!("{:?}", err);
                });

                let cpu_future = pool.spawn(write_future);
                let result = cpu_future.wait();

                assert!(result.is_ok());
            }
        }

        let mut file = fs::File::open(&file_name).unwrap();
        let mut read_buffer: [u8; 8192] = [0u8; 8192];

        file.seek(io::SeekFrom::Start(16384)).unwrap();
        file.read(&mut read_buffer).unwrap();
        assert!(validate_pattern(65u8, &read_buffer));

        file.seek(io::SeekFrom::Start(32768)).unwrap();
        file.read(&mut read_buffer).unwrap();
        assert!(validate_pattern(66u8, &read_buffer));

        file.seek(io::SeekFrom::Start(49152)).unwrap();
        file.read(&mut read_buffer).unwrap();
        assert!(validate_pattern(67u8, &read_buffer));
    }

    #[test]
    fn read_invalid_fd() {
        let fd = 2431;

        let pool = futures_cpupool::CpuPool::new(5);
        let buffer = MemoryHandle::new();

        {
            let context = AioContext::new(&pool, 10).unwrap();
            let read_future = context
                .read(fd, 0, buffer)
                .map(move |_| {
                    assert!(false);
                })
                .map_err(|err| {
                    assert!(err.error.kind() == io::ErrorKind::Other);
                    err
                });

            let cpu_future = pool.spawn(read_future);
            let result = cpu_future.wait();

            assert!(result.is_err());
        }
    }

    /*
    For some reason, this test does not pass on Travis. Need to research why the out-of-range
    file offset does not trip an invalid argument error.

    #[test]
    fn invalid_offset() {
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

            let context = AioContext::new(&pool, 10).unwrap();
            let read_future = context
                .read(fd, 1000000, buffer)
                .map(move |_| {
                    assert!(false);
                })
                .map_err(|err| {
                    assert!(err.error.kind() == io::ErrorKind::Other);
                    err
                });

            let cpu_future = pool.spawn(read_future);
            let result = cpu_future.wait();

            assert!(result.is_err());
        }

        remove_file(&file_name);
    }
    */

    #[test]
    fn read_many_blocks_mt() {
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

            {
                let num_slots = 7;
                let context = AioContext::new(&pool, num_slots).unwrap();

                // 50 waves of requests just going above the lmit

                // Waves start here
                for _wave in 0..50 {
                    let mut futures = Vec::new();

                    // Each wave makes 100 I/O requests
                    for index in 0..100 {
                        let buffer = MemoryHandle::new();
                        let read_future = context
                            .read(fd, (index * 8192) % FILE_SIZE, buffer)
                            .map(move |result_buffer| {
                                assert!(validate_block(result_buffer.as_ref()));
                            })
                            .map_err(|err| {
                                panic!("{:?}", err);
                            });

                        futures.push(pool.spawn(read_future));
                    }

                    // wait for all 100 requests to complete
                    let result = futures::future::join_all(futures).wait();

                    assert!(result.is_ok());

                    // all slots have been returned
                    assert!(context.inner.have_capacity.current_capacity() == num_slots);
                }
            }
        }

        remove_file(&file_name);
    }

    // A test with a mixed read/write workload
    #[test]
    fn mixed_read_write() {
        let file_name = temp_file_name();
        create_temp_file(&file_name);

        let owned_fd = OwnedFd::new_from_raw_fd(unsafe {
            open(
                mem::transmute(file_name.as_os_str().as_bytes().as_ptr()),
                O_DIRECT | O_RDWR,
            )
        });
        let fd = owned_fd.fd;

        let mut futures = Vec::new();

        let pool = futures_cpupool::CpuPool::new(5);
        let context = AioContext::new(&pool, 7).unwrap();

        // First access sequence
        let buffer1 = MemoryHandle::new();

        let sequence1 = {
            let context1 = context.clone();
            let context2 = context.clone();
            let context3 = context.clone();
            let context4 = context.clone();
            let context5 = context.clone();
            let context6 = context.clone();

            context1
                .read(fd, 8192, buffer1)
                .map(|mut buffer| -> MemoryHandle {
                    assert!(validate_block(buffer.as_ref()));
                    fill_pattern(0u8, buffer.as_mut());
                    buffer
                })
                .and_then(move |buffer| context2.write(fd, 8192, buffer))
                .and_then(move |buffer| context3.read(fd, 0, buffer))
                .map(|mut buffer| -> MemoryHandle {
                    assert!(validate_block(buffer.as_ref()));
                    fill_pattern(1u8, buffer.as_mut());
                    buffer
                })
                .and_then(move |buffer| context4.write(fd, 0, buffer))
                .and_then(move |buffer| context5.read(fd, 8192, buffer))
                .map(|buffer| -> MemoryHandle {
                    assert!(validate_pattern(0u8, buffer.as_ref()));
                    buffer
                })
                .and_then(move |buffer| context6.read(fd, 0, buffer))
                .map(|buffer| -> MemoryHandle {
                    assert!(validate_pattern(1u8, buffer.as_ref()));
                    buffer
                })
                .map_err(|err| {
                    panic!("{:?}", err);
                })
        };

        // Second access sequence

        let buffer2 = MemoryHandle::new();

        let sequence2 = {
            let context1 = context.clone();
            let context2 = context.clone();
            let context3 = context.clone();
            let context4 = context.clone();
            let context5 = context.clone();
            let context6 = context.clone();

            context1
                .read(fd, 16384, buffer2)
                .map(|mut buffer| -> MemoryHandle {
                    assert!(validate_block(buffer.as_ref()));
                    fill_pattern(2u8, buffer.as_mut());
                    buffer
                })
                .and_then(move |buffer| context2.write(fd, 16384, buffer))
                .and_then(move |buffer| context3.read(fd, 24576, buffer))
                .map(|mut buffer| -> MemoryHandle {
                    assert!(validate_block(buffer.as_ref()));
                    fill_pattern(3u8, buffer.as_mut());
                    buffer
                })
                .and_then(move |buffer| context4.write(fd, 24576, buffer))
                .and_then(move |buffer| context5.read(fd, 16384, buffer))
                .map(|buffer| -> MemoryHandle {
                    assert!(validate_pattern(2u8, buffer.as_ref()));
                    buffer
                })
                .and_then(move |buffer| context6.read(fd, 24576, buffer))
                .map(|buffer| -> MemoryHandle {
                    assert!(validate_pattern(3u8, buffer.as_ref()));
                    buffer
                })
                .map_err(|err| {
                    panic!("{:?}", err);
                })
        };

        // Third access sequence

        let buffer3 = MemoryHandle::new();

        let sequence3 = {
            let context1 = context.clone();
            let context2 = context.clone();
            let context3 = context.clone();
            let context4 = context.clone();
            let context5 = context.clone();
            let context6 = context.clone();

            context1
                .read(fd, 40960, buffer3)
                .map(|mut buffer| -> MemoryHandle {
                    assert!(validate_block(buffer.as_ref()));
                    fill_pattern(5u8, buffer.as_mut());
                    buffer
                })
                .and_then(move |buffer| context2.write(fd, 40960, buffer))
                .and_then(move |buffer| context3.read(fd, 32768, buffer))
                .map(|mut buffer| -> MemoryHandle {
                    assert!(validate_block(buffer.as_ref()));
                    fill_pattern(6u8, buffer.as_mut());
                    buffer
                })
                .and_then(move |buffer| context4.write(fd, 32768, buffer))
                .and_then(move |buffer| context5.read(fd, 40960, buffer))
                .map(|buffer| -> MemoryHandle {
                    assert!(validate_pattern(5u8, buffer.as_ref()));
                    buffer
                })
                .and_then(move |buffer| context6.read(fd, 32768, buffer))
                .map(|buffer| -> MemoryHandle {
                    assert!(validate_pattern(6u8, buffer.as_ref()));
                    buffer
                })
                .map_err(|err| {
                    panic!("{:?}", err);
                })
        };

        // Launch the three futures
        futures.push(pool.spawn(sequence1));
        futures.push(pool.spawn(sequence2));
        futures.push(pool.spawn(sequence3));

        // Wair for completion
        let result = futures::future::join_all(futures).wait();

        assert!(result.is_ok());
    }

    // Fille the buffer with a pattern that has a dependency on the provided key.
    fn fill_pattern(key: u8, buffer: &mut [u8]) {
        // The pattern we generate is an alternation of the key value and an index value
        // For this we ensure that the buffer has an even number of elements
        assert!(buffer.len() % 2 == 0);

        for index in 0..buffer.len() / 2 {
            buffer[index * 2] = key;
            buffer[index * 2 + 1] = index as u8;
        }
    }

    // Validate that the buffer is filled with a pattern as generated by the provided key.
    fn validate_pattern(key: u8, buffer: &[u8]) -> bool {
        // The pattern we generate is an alternation of the key value and an index value
        // For this we ensure that the buffer has an even number of elements
        assert!(buffer.len() % 2 == 0);

        for index in 0..buffer.len() / 2 {
            if (buffer[index * 2] != key) || (buffer[index * 2 + 1] != (index as u8)) {
                return false;
            }
        }

        return true;
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
