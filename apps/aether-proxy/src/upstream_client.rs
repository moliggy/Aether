use std::collections::HashMap;
use std::convert::Infallible;
use std::future::Future;
use std::io;
use std::net::IpAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::{Context, Poll};
use std::time::Duration;

use aether_contracts::{
    ResolvedTransportProfile, TRANSPORT_BACKEND_HYPER_RUSTLS, TRANSPORT_BACKEND_REQWEST_RUSTLS,
    TRANSPORT_HTTP_MODE_HTTP1_ONLY,
};
use bytes::Bytes;
use futures_util::Stream;
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::Frame;
use hyper::rt;
use hyper::Response;
use hyper::Uri;
pub use hyper_util::client::legacy::connect::capture_connection;
use hyper_util::client::legacy::connect::dns::Name;
use hyper_util::client::legacy::connect::{Connected, Connection, HttpConnector};
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use rustls::pki_types::ServerName;
use rustls::ClientConfig;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tower_service::Service;

use crate::config::Config;
use crate::target_filter::{self, DnsCache};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

type PlainStream = TokioIo<TcpStream>;
type TlsStream = TokioIo<tokio_rustls::client::TlsStream<TcpStream>>;

pub type UpstreamRequestBody = UnsyncBoxBody<Bytes, io::Error>;
pub type UpstreamClient = Client<InstrumentedConnector, UpstreamRequestBody>;

const DEFAULT_PROFILE_ID: &str = "default";
const DEFAULT_BACKEND: &str = TRANSPORT_BACKEND_HYPER_RUSTLS;
const DEFAULT_HTTP_MODE: &str = "auto";

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct UpstreamClientPoolKey {
    pub provider_id: String,
    pub endpoint_id: String,
    pub key_id: String,
    pub profile_id: String,
    pub backend: String,
    pub http_mode: String,
}

#[derive(Clone)]
pub struct UpstreamClientPool {
    config: Arc<Config>,
    dns_cache: Arc<DnsCache>,
    clients: Arc<Mutex<HashMap<UpstreamClientPoolKey, UpstreamClient>>>,
}

impl UpstreamClientPool {
    pub fn new(config: Arc<Config>, dns_cache: Arc<DnsCache>) -> Self {
        Self {
            config,
            dns_cache,
            clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn get_or_build(&self, key: UpstreamClientPoolKey) -> Result<UpstreamClient, String> {
        if let Some(client) = self
            .clients
            .lock()
            .expect("client pool lock")
            .get(&key)
            .cloned()
        {
            return Ok(client);
        }

        validate_proxy_transport_backend(&key.backend)?;
        let http1_only = key
            .http_mode
            .eq_ignore_ascii_case(TRANSPORT_HTTP_MODE_HTTP1_ONLY);
        let client = build_upstream_client_with_protocol(
            &self.config,
            Arc::clone(&self.dns_cache),
            http1_only,
        );
        self.clients
            .lock()
            .expect("client pool lock")
            .insert(key, client.clone());
        Ok(client)
    }
}

pub fn upstream_client_pool_key(
    provider_id: Option<&str>,
    endpoint_id: Option<&str>,
    key_id: Option<&str>,
    profile: Option<&ResolvedTransportProfile>,
    http1_only: bool,
) -> UpstreamClientPoolKey {
    let profile_http_mode = profile
        .map(|profile| profile.http_mode.trim())
        .filter(|value| !value.is_empty())
        .unwrap_or(DEFAULT_HTTP_MODE);
    let http_mode = if http1_only {
        TRANSPORT_HTTP_MODE_HTTP1_ONLY
    } else {
        profile_http_mode
    };
    UpstreamClientPoolKey {
        provider_id: normalized_pool_key_part(provider_id),
        endpoint_id: normalized_pool_key_part(endpoint_id),
        key_id: normalized_pool_key_part(key_id),
        profile_id: profile
            .map(|profile| profile.profile_id.trim())
            .filter(|value| !value.is_empty())
            .unwrap_or(DEFAULT_PROFILE_ID)
            .to_string(),
        backend: profile
            .map(|profile| profile.backend.trim())
            .filter(|value| !value.is_empty())
            .unwrap_or(DEFAULT_BACKEND)
            .to_string(),
        http_mode: http_mode.to_string(),
    }
}

fn normalized_pool_key_part(value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("-")
        .to_string()
}

fn validate_proxy_transport_backend(backend: &str) -> Result<(), String> {
    if backend.eq_ignore_ascii_case(TRANSPORT_BACKEND_HYPER_RUSTLS)
        || backend.eq_ignore_ascii_case(TRANSPORT_BACKEND_REQWEST_RUSTLS)
    {
        return Ok(());
    }
    Err(format!("unsupported transport profile backend: {backend}"))
}

pub fn stream_request_body<S>(stream: S) -> UpstreamRequestBody
where
    S: Stream<Item = Result<Frame<Bytes>, io::Error>> + Send + 'static,
{
    StreamBody::new(stream).boxed_unsync()
}

pub fn full_request_body(body: Bytes) -> UpstreamRequestBody {
    Full::new(body)
        .map_err(|err: Infallible| match err {})
        .boxed_unsync()
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ConnectTiming {
    pub connect_ms: u64,
    pub tls_ms: u64,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RequestTiming {
    pub connection_acquire_ms: u64,
    pub connect_ms: u64,
    pub tls_ms: u64,
    pub response_wait_ms: u64,
    pub connection_reused: bool,
}

#[derive(Clone)]
pub struct ValidatedResolver {
    dns_cache: Arc<DnsCache>,
    allow_private: bool,
}

impl ValidatedResolver {
    pub fn new(dns_cache: Arc<DnsCache>, allow_private: bool) -> Self {
        Self {
            dns_cache,
            allow_private,
        }
    }
}

pub struct ValidatedAddrs {
    inner: std::vec::IntoIter<std::net::SocketAddr>,
}

impl Iterator for ValidatedAddrs {
    type Item = std::net::SocketAddr;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl Service<Name> for ValidatedResolver {
    type Response = ValidatedAddrs;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, name: Name) -> Self::Future {
        let dns_cache = Arc::clone(&self.dns_cache);
        let allow_private = self.allow_private;
        let host = name.as_str().to_string();
        Box::pin(async move {
            if let Some(addrs) = dns_cache.get_by_host(&host).await {
                return Ok(ValidatedAddrs {
                    inner: (*addrs).clone().into_iter(),
                });
            }

            let resolved =
                target_filter::resolve_public_addrs(&host, 0, allow_private, dns_cache.as_ref())
                    .await
                    .map_err(|err| io::Error::other(err.to_string()))?;
            Ok(ValidatedAddrs {
                inner: resolved.into_iter(),
            })
        })
    }
}

#[derive(Clone)]
pub struct InstrumentedConnector {
    http: HttpConnector<ValidatedResolver>,
    tls_config: Arc<ClientConfig>,
}

impl Service<Uri> for InstrumentedConnector {
    type Response = TimedConn;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.http.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, dst: Uri) -> Self::Future {
        let scheme = dst.scheme_str().map(|value| value.to_ascii_lowercase());
        let tls_config = Arc::clone(&self.tls_config);
        let connecting = self.http.call(dst.clone());
        let connect_start = std::time::Instant::now();

        Box::pin(async move {
            match scheme.as_deref() {
                Some("http") => {
                    let tcp = connecting.await.map_err(|err| Box::new(err) as BoxError)?;
                    let connect_ms = connect_start.elapsed().as_millis() as u64;
                    Ok(TimedConn::new(
                        MaybeHttpsStream::Http(tcp),
                        ConnectTiming {
                            connect_ms,
                            tls_ms: 0,
                        },
                    ))
                }
                Some("https") => {
                    let server_name = resolve_server_name(&dst)?;
                    let tcp = connecting.await.map_err(|err| Box::new(err) as BoxError)?;
                    let connect_ms = connect_start.elapsed().as_millis() as u64;

                    let tls_start = std::time::Instant::now();
                    let tls_stream = TlsConnector::from(tls_config)
                        .connect(server_name, tcp.into_inner())
                        .await
                        .map_err(io::Error::other)?;
                    let tls_ms = tls_start.elapsed().as_millis() as u64;

                    Ok(TimedConn::new(
                        MaybeHttpsStream::Https(TokioIo::new(tls_stream)),
                        ConnectTiming { connect_ms, tls_ms },
                    ))
                }
                Some(other) => Err(io::Error::other(format!("unsupported scheme {other}")).into()),
                None => Err(io::Error::other("missing scheme").into()),
            }
        })
    }
}

fn build_upstream_client_with_protocol(
    config: &Config,
    dns_cache: Arc<DnsCache>,
    http1_only: bool,
) -> UpstreamClient {
    let mut http = HttpConnector::new_with_resolver(ValidatedResolver::new(
        dns_cache,
        config.allow_private_targets,
    ));
    http.enforce_http(false);
    http.set_connect_timeout(Some(Duration::from_secs(
        config.upstream_connect_timeout_secs,
    )));
    http.set_nodelay(config.upstream_tcp_nodelay);
    if config.upstream_tcp_keepalive_secs > 0 {
        http.set_keepalive(Some(Duration::from_secs(
            config.upstream_tcp_keepalive_secs,
        )));
    } else {
        http.set_keepalive(None);
    }

    let connector = InstrumentedConnector {
        http,
        tls_config: build_tls_config(http1_only),
    };

    let mut builder = Client::builder(TokioExecutor::new());
    builder.pool_max_idle_per_host(config.upstream_pool_max_idle_per_host);
    builder.pool_idle_timeout(Duration::from_secs(config.upstream_pool_idle_timeout_secs));
    builder.pool_timer(TokioTimer::new());
    builder.build(connector)
}

pub fn resolve_request_timing<B>(
    response: &Response<B>,
    connection_acquire_ms: Option<u64>,
    ttfb_ms: u64,
) -> RequestTiming {
    let raw = response
        .extensions()
        .get::<ConnectTiming>()
        .copied()
        .unwrap_or_default();

    let raw_connection_ms = raw.connect_ms.saturating_add(raw.tls_ms);
    let measured_acquire_ms = connection_acquire_ms.unwrap_or(raw_connection_ms.min(ttfb_ms));
    let likely_reused = measured_acquire_ms <= 5 && raw_connection_ms > 0;
    let connector_matches_request = raw_connection_ms <= measured_acquire_ms.saturating_add(25);

    let (connect_ms, tls_ms) = if likely_reused || !connector_matches_request {
        (0, 0)
    } else {
        (raw.connect_ms, raw.tls_ms)
    };

    RequestTiming {
        connection_acquire_ms: measured_acquire_ms,
        connect_ms,
        tls_ms,
        response_wait_ms: ttfb_ms.saturating_sub(measured_acquire_ms),
        connection_reused: likely_reused,
    }
}

fn build_tls_config(http1_only: bool) -> Arc<ClientConfig> {
    let root_store =
        rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let mut config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    config.alpn_protocols = if http1_only {
        vec![b"http/1.1".to_vec()]
    } else {
        vec![b"h2".to_vec(), b"http/1.1".to_vec()]
    };
    Arc::new(config)
}

fn resolve_server_name(uri: &Uri) -> Result<ServerName<'static>, BoxError> {
    let host = uri.host().ok_or_else(|| io::Error::other("missing host"))?;
    let host = host.trim_start_matches('[').trim_end_matches(']');

    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(ServerName::from(ip));
    }

    Ok(ServerName::try_from(host.to_string())?)
}

pub struct TimedConn {
    inner: MaybeHttpsStream,
    timing: ConnectTiming,
}

impl TimedConn {
    fn new(inner: MaybeHttpsStream, timing: ConnectTiming) -> Self {
        Self { inner, timing }
    }
}

impl Connection for TimedConn {
    fn connected(&self) -> Connected {
        self.inner.connected().extra(self.timing)
    }
}

impl rt::Read for TimedConn {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: rt::ReadBufCursor<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl rt::Write for TimedConn {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        Pin::new(&mut self.inner).poll_write_vectored(cx, bufs)
    }
}

pub enum MaybeHttpsStream {
    Http(PlainStream),
    Https(TlsStream),
}

impl Connection for MaybeHttpsStream {
    fn connected(&self) -> Connected {
        match self {
            Self::Http(stream) => stream.connected(),
            Self::Https(stream) => {
                let (tcp, tls) = stream.inner().get_ref();
                if tls.alpn_protocol() == Some(b"h2") {
                    tcp.connected().negotiated_h2()
                } else {
                    tcp.connected()
                }
            }
        }
    }
}

impl rt::Read for MaybeHttpsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: rt::ReadBufCursor<'_>,
    ) -> Poll<Result<(), io::Error>> {
        match Pin::get_mut(self) {
            Self::Http(stream) => Pin::new(stream).poll_read(cx, buf),
            Self::Https(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

impl rt::Write for MaybeHttpsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, io::Error>> {
        match Pin::get_mut(self) {
            Self::Http(stream) => Pin::new(stream).poll_write(cx, buf),
            Self::Https(stream) => Pin::new(stream).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match Pin::get_mut(self) {
            Self::Http(stream) => Pin::new(stream).poll_flush(cx),
            Self::Https(stream) => Pin::new(stream).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        match Pin::get_mut(self) {
            Self::Http(stream) => Pin::new(stream).poll_shutdown(cx),
            Self::Https(stream) => Pin::new(stream).poll_shutdown(cx),
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::Http(stream) => stream.is_write_vectored(),
            Self::Https(stream) => stream.is_write_vectored(),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        match Pin::get_mut(self) {
            Self::Http(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
            Self::Https(stream) => Pin::new(stream).poll_write_vectored(cx, bufs),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aether_contracts::ResolvedTransportProfile;
    use hyper::Response;

    #[test]
    fn fresh_connection_uses_connector_breakdown() {
        let mut response = Response::new(());
        response.extensions_mut().insert(ConnectTiming {
            connect_ms: 80,
            tls_ms: 40,
        });

        let timing = resolve_request_timing(&response, Some(125), 600);

        assert_eq!(timing.connection_acquire_ms, 125);
        assert_eq!(timing.connect_ms, 80);
        assert_eq!(timing.tls_ms, 40);
        assert_eq!(timing.response_wait_ms, 475);
        assert!(!timing.connection_reused);
    }

    #[test]
    fn reused_connection_zeroes_stale_connect_timings() {
        let mut response = Response::new(());
        response.extensions_mut().insert(ConnectTiming {
            connect_ms: 70,
            tls_ms: 30,
        });

        let timing = resolve_request_timing(&response, Some(0), 310);

        assert_eq!(timing.connection_acquire_ms, 0);
        assert_eq!(timing.connect_ms, 0);
        assert_eq!(timing.tls_ms, 0);
        assert_eq!(timing.response_wait_ms, 310);
        assert!(timing.connection_reused);
    }

    #[test]
    fn falls_back_to_connector_timings_when_capture_missing() {
        let mut response = Response::new(());
        response.extensions_mut().insert(ConnectTiming {
            connect_ms: 55,
            tls_ms: 25,
        });

        let timing = resolve_request_timing(&response, None, 400);

        assert_eq!(timing.connection_acquire_ms, 80);
        assert_eq!(timing.connect_ms, 55);
        assert_eq!(timing.tls_ms, 25);
        assert_eq!(timing.response_wait_ms, 320);
        assert!(!timing.connection_reused);
    }

    #[test]
    fn upstream_client_pool_key_includes_profile_identity() {
        let profile = ResolvedTransportProfile {
            profile_id: "profile-a".to_string(),
            backend: TRANSPORT_BACKEND_REQWEST_RUSTLS.to_string(),
            http_mode: "auto".to_string(),
            pool_scope: "key".to_string(),
            header_fingerprint: None,
            extra: None,
        };
        let pool_key = upstream_client_pool_key(
            Some("provider-1"),
            Some("endpoint-1"),
            Some("key-1"),
            Some(&profile),
            false,
        );

        assert_eq!(pool_key.provider_id, "provider-1");
        assert_eq!(pool_key.endpoint_id, "endpoint-1");
        assert_eq!(pool_key.key_id, "key-1");
        assert_eq!(pool_key.profile_id, "profile-a");
        assert_eq!(pool_key.backend, TRANSPORT_BACKEND_REQWEST_RUSTLS);
        assert_eq!(pool_key.http_mode, "auto");
    }

    #[test]
    fn upstream_client_pool_rejects_unsupported_backend() {
        let error = validate_proxy_transport_backend("utls").unwrap_err();

        assert!(error.contains("unsupported transport profile backend"));
    }
}
