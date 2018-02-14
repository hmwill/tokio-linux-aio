use libc::{c_uint, c_int, c_long};
use aio_bindings::{aio_context_t, iocb, io_event, syscall, __NR_io_setup, __NR_io_destroy, __NR_io_submit, __NR_io_getevents, 
    IOCB_CMD_PREAD, IOCB_CMD_PWRITE, IOCB_FLAG_RESFD, timespec};

#[inline(always)]
unsafe fn io_setup(nr: c_long, ctxp: *mut aio_context_t) -> c_long {
    syscall(__NR_io_setup as c_long, nr, ctxp)
}

#[inline(always)]
unsafe fn io_destroy(ctx: aio_context_t) -> c_long {
    syscall(__NR_io_destroy as c_long, ctx)
}

#[inline(always)]
unsafe fn io_submit(ctx: aio_context_t, nr: c_long, iocbpp: *mut *mut iocb) -> c_long {
    syscall(__NR_io_submit as c_long, ctx, nr, iocbpp)
}

#[inline(always)]
unsafe fn io_getevents(ctx: aio_context_t, min_nr: c_long, max_nr: c_long, events: *mut io_event, 
    timeout: *mut timespec) -> c_long {
    syscall(__NR_io_getevents as c_long, ctx, min_nr, max_nr, events, timeout)
}
