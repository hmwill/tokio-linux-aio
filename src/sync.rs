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

use std::sync;
use std::io;
use std::collections;

use futures;

// -----------------------------------------------------------------------------------------------
// Semaphore that's workable with Futures
//
// Currently this is rather barebones; may consider expanding it into something library-grade
// and then exposing it by itself.
// -----------------------------------------------------------------------------------------------

#[derive(Debug)]
struct SemaphoreInner {
    capacity: usize,
    waiters: collections::VecDeque<futures::task::Task>,
}

#[derive(Debug)]
pub struct Semaphore {
    inner: parking_lot::RwLock<SemaphoreInner>,
}

impl Semaphore {
    pub fn new(initial: usize) -> Semaphore {
        Semaphore {
            inner: parking_lot::RwLock::new(SemaphoreInner {
                capacity: initial,
                waiters: collections::VecDeque::new(),
            }),
        }
    }

    pub fn acquire(&self) -> SemaphoreHandle {
        let mut guard = self.inner.write();
        if guard.capacity > 0 {
            guard.capacity -= 1;
            SemaphoreHandle::Completed(futures::future::result(Ok(())))
        } else {
            guard.waiters.push_back(futures::task::current());
            SemaphoreHandle::Waiting
        }
    }

    pub fn release(&self) {
        let mut guard = self.inner.write();
        if !guard.waiters.is_empty() {
            guard.waiters.pop_front().unwrap().notify();
        } else {
            guard.capacity += 1;
        }
    }

    // For testing code
    pub fn current_capacity(&self) -> usize {
        self.inner.read().capacity
    }
}

pub enum SemaphoreHandle {
    Waiting,
    Completed(futures::future::FutureResult<(), io::Error>),
}

impl futures::Future for SemaphoreHandle {
    type Item = ();
    type Error = io::Error;

    fn poll(&mut self) -> Result<futures::Async<()>, io::Error> {
        match self {
            &mut SemaphoreHandle::Completed(_) => Ok(futures::Async::Ready(())),
            &mut SemaphoreHandle::Waiting => {
                *self = SemaphoreHandle::Completed(futures::future::result(Ok(())));
                Ok(futures::Async::NotReady)
            }
        }
    }
}
