use std::sync::mpsc::{sync_channel, Receiver};

pub struct Results<T> {
    recv: Receiver<T>,
}

pub trait BackgroundIterator<T>: Iterator<Item = T> + Send + Sized + 'static
where
    T: Send + 'static,
{
    fn run_in_background(self, queue_bound: usize) -> Results<T> {
        let (sender, recv) = sync_channel(queue_bound);

        rayon::spawn(move || {
            for item in self {
                if sender.send(item).is_err() {
                    break;
                }
            }
        });

        Results { recv }
    }
}

impl<I, T> BackgroundIterator<T> for I
where
    I: Iterator<Item = T> + Send + Sized + 'static,
    T: Send + 'static,
{
}

impl<T> Iterator for Results<T>
where
    T: Send + 'static,
{
    type Item = T;

    fn next(&mut self) -> Option<T> {
        self.recv.recv().ok()
    }
}

#[cfg(test)]
mod test {
    use super::*;
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
        let t = Instant::now();

        let load_delay = 7;
        let process_delay = 10;

        SleepyIter {
            iter: (0..10u64),
            delay_millis: load_delay,
        }
        .map(|item| {
            println!("processed item {} in the background", item);
            item
        })
        .run_in_background(5)
        .for_each(|item| {
            // Simulate heavy processing.
            sleep(Duration::from_millis(process_delay));
            println!("finished processing item {}", item);
        });

        println!("total time to run: {}ms", t.elapsed().as_millis());
    }
}
