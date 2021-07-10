use futures::stream::Stream;
use futures::task::{Context, Poll};
use std::pin::Pin;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::channel;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

pub struct Results<T> {
    stream: ReceiverStream<T>,
    _join_handle: JoinHandle<()>,
}

pub trait BackgroundIterator<T>: Iterator<Item = T> + Send + Sized + 'static
where
    T: Send + 'static,
{
    // TODO: allow using different backends, e.g. dedicated OS thread, rayon thread pool, etc.
    fn run_in_background(self, runtime: &Runtime, queue_bound: usize) -> Results<T> {
        let (sender, recv) = channel(queue_bound);

        let _join_handle = runtime.spawn_blocking(move || {
            for item in self {
                if let Err(e) = sender.blocking_send(item) {
                    println!("error during iteration: {}", e);
                    break;
                }
            }
        });

        let stream = ReceiverStream::new(recv);
        Results {
            stream,
            _join_handle,
        }
    }
}

impl<I, T> BackgroundIterator<T> for I
where
    I: Iterator<Item = T> + Send + Sized + 'static,
    T: Send + 'static,
{
}

// TODO: implement a fully-blocking version (impl Iterator).
// Compute-heavy tasks shouldn't run async on the Tokio executor anyway.
impl<T> Stream for Results<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use futures::stream::StreamExt;
    use std::thread::sleep;
    use std::time::{Duration, Instant};

    struct SleepyIter<I> {
        iter: I,
        delay_millis: u64,
    }

    impl<I> Iterator for SleepyIter<I>
    where
        I: Iterator,
    {
        type Item = I::Item;

        fn next(&mut self) -> Option<I::Item> {
            self.iter.next().map(|item| {
                // Simulate processing/load time for iterator items.
                sleep(Duration::from_millis(self.delay_millis));
                item
            })
        }
    }

    #[test]
    fn parallel() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();

        let t = Instant::now();

        let load_delay = 5;
        let process_delay = 5;

        let sleepy = SleepyIter {
            iter: (0..10u64),
            delay_millis: load_delay,
        }
        .map(|item| {
            println!("processed item {} in the background", item);
            item
        });

        let sleepy_stream = sleepy.run_in_background(&rt, 1);

        rt.block_on(async move {
            sleepy_stream
                .for_each(|item| async move {
                    // Simulate heavy processing.
                    tokio::time::sleep(Duration::from_millis(process_delay)).await;
                    println!("finished processing item {}", item);
                })
                .await;
        });

        println!("total time to run: {}ms", t.elapsed().as_millis());
    }
}
