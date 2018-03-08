#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

/*
 * extracted from https://elixir.bootlin.com/linux/latest/source/include/uapi/linux/fs.h#L372
 * Flags for preadv2/pwritev2:
 */

/* per-IO O_DSYNC */
//#define RWF_DSYNC	((__force __kernel_rwf_t)0x00000002)
pub const RWF_DSYNC: u32 = 2;

/* per-IO O_SYNC */
//#define RWF_SYNC	((__force __kernel_rwf_t)0x00000004)
pub const RWF_SYNC: u32 = 4;

/* per-IO, return -EAGAIN if operation would block */
//#define RWF_NOWAIT	((__force __kernel_rwf_t)0x00000008)
pub const RWF_NOWAIT: u32 = 8;
