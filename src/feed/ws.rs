use anyhow::{Context, anyhow};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;
use tungstenite::WebSocket;
use tungstenite::client::IntoClientRequest;
use tungstenite::stream::MaybeTlsStream;

pub type Ws = WebSocket<MaybeTlsStream<TcpStream>>;

/// Blocking connect + TLS + WS handshake with timeouts; then the socket is switched to
/// non-blocking (spin mode) or a short read timeout (block mode) so the loop stays responsive.
pub fn connect(url: &str, timeout: Duration, nonblocking: bool) -> anyhow::Result<Ws> {
    let request = url.into_client_request()?;
    let host = request.uri().host().ok_or_else(|| anyhow!("no host in {url}"))?.to_owned();
    let port = request.uri().port_u16().unwrap_or(443);
    let address = (host.as_str(), port).to_socket_addrs()?.next().context("dns")?;
    let tcp = TcpStream::connect_timeout(&address, timeout)?;
    tcp.set_nodelay(true)?;
    tcp.set_read_timeout(Some(timeout))?;
    tcp.set_write_timeout(Some(timeout))?;
    // Same socket, separate handle: lets us change its mode after tungstenite takes ownership.
    let socket = tcp.try_clone()?;
    let (ws, _) = tungstenite::client_tls_with_config(request, tcp, None, None).map_err(|e| anyhow!("handshake: {e}"))?;
    if nonblocking {
        socket.set_nonblocking(true)?;
    } else {
        socket.set_read_timeout(Some(Duration::from_millis(100)))?;
    }
    Ok(ws)
}
