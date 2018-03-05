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
extern crate futures_cpupool;
extern crate libc;
extern crate memmap;
extern crate mio;
extern crate rand;
extern crate tokio;

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
            state.request.aio_flags = aio::IOCB_FLAG_RESFD;
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
    ReadWriteHandle: ops::DerefMut<Target = [u8]>,
{
    // common AIO future state
    base: AioBaseFuture,

    // memory handle where data read from the underlying block device is being written to.
    // Holding on to this value is important in the case where it implements Drop.
    buffer: Option<ReadWriteHandle>,
}

impl<ReadWriteHandle> futures::Future for AioReadResultFuture<ReadWriteHandle>
where
    ReadWriteHandle: ops::DerefMut<Target = [u8]>,
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
    ReadOnlyHandle: ops::Deref<Target = [u8]>,
{
    // common AIO future state
    base: AioBaseFuture,

    // memory handle where data written to the underlying block device is being read from.
    // Holding on to this value is important in the case where it implements Drop.
    buffer: Option<ReadOnlyHandle>,
}

impl<ReadOnlyHandle> futures::Future for AioWriteResultFuture<ReadOnlyHandle>
where
    ReadOnlyHandle: ops::Deref<Target = [u8]>,
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
pub struct AioContext {
    inner: std::sync::Arc<AioContextInner>,

    // handle for the spawned background task; dropping it will cancel the task
    _poll_task_handle: futures::sync::oneshot::SpawnHandle<(), io::Error>,
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

        let inner = AioContextInner::new(fd, nr)?;
        let context = inner.context;

        let poll_future = AioPollFuture {
            context,
            eventfd,
            events: Vec::with_capacity(nr),
        };

        Ok(AioContext {
            inner: std::sync::Arc::new(inner),
            _poll_task_handle: futures::sync::oneshot::spawn(poll_future, executor),
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
                iocb_info: IocbInfo {
                    opcode: aio::IOCB_CMD_PREAD,
                    fd,
                    offset,
                    len,
                    buf: unsafe { mem::transmute(buffer.as_ptr()) },
                },
                state: None,
                acquire_state: None,
            },
            buffer: Some(buffer),
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
                iocb_info: IocbInfo {
                    opcode: aio::IOCB_CMD_PWRITE,
                    fd,
                    offset,
                    len,
                    buf: unsafe { mem::transmute(buffer.as_ptr()) },
                },
                state: None,
                acquire_state: None,
            },
            buffer: Some(buffer),
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
                                assert!(validate_block(&result_buffer));
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
