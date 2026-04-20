use std::io;
use std::time::Duration;

const LOOPBACK_BIND_MAX_ATTEMPTS: usize = 120;
const LOOPBACK_BIND_RETRY_DELAY: Duration = Duration::from_millis(50);

pub(crate) async fn bind_loopback_listener() -> io::Result<tokio::net::TcpListener> {
    let mut last_retryable_error = None;

    for attempt in 1..=LOOPBACK_BIND_MAX_ATTEMPTS {
        match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => return Ok(listener),
            Err(err)
                if attempt < LOOPBACK_BIND_MAX_ATTEMPTS
                    && is_retryable_loopback_bind_error(&err) =>
            {
                last_retryable_error = Some(err);
                tokio::time::sleep(LOOPBACK_BIND_RETRY_DELAY).await;
            }
            Err(err) => return Err(err),
        }
    }

    let err =
        last_retryable_error.expect("loopback bind retries should record the last retryable error");
    Err(io::Error::new(
        err.kind(),
        format!("loopback bind failed after {LOOPBACK_BIND_MAX_ATTEMPTS} attempts: {err}"),
    ))
}

fn is_retryable_loopback_bind_error(err: &io::Error) -> bool {
    matches!(
        err.kind(),
        io::ErrorKind::PermissionDenied
            | io::ErrorKind::AddrInUse
            | io::ErrorKind::AddrNotAvailable
    )
}
