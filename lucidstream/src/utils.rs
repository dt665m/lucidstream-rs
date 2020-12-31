use crate::traits::Aggregate;
use futures::future::TryFuture;

pub fn create_stream_id<T: Aggregate>(id: &str) -> String {
    [T::kind(), "_", id].concat()
}

pub async fn retry_future<Fut, Factory, F, R>(
    f: Factory,
    should_retry: F,
    count: usize,
) -> Fut::Output
where
    Fut: TryFuture<Output = R>,
    Factory: Fn() -> Fut,
    F: Fn(&R) -> bool,
{
    let mut attempts = 0;
    loop {
        let result = f().await;
        if should_retry(&result) && attempts < count {
            // probably should incur some kind of wait here to prevent thundering herd
            attempts += 1;
            continue;
        }
        return result;
    }
}
