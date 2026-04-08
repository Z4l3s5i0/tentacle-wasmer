#[cfg(all(not(target_family = "wasm"), not(target_os = "wasix")))]
use super::proxy::{socks5, socks5_config::random_auth};
use multiaddr::{MultiAddr, Protocol};
pub use tokio::{
    net::{TcpListener, TcpStream},
    spawn,
    task::{JoinHandle, block_in_place, spawn_blocking, yield_now},
};

use crate::{
    service::config::{TcpSocket, TcpSocketConfig, TcpSocketTransformer, TransformerContext},
};
#[cfg(all(not(target_family = "wasm"), not(target_os = "wasix")))]
    use crate::utils::redact_auth_from_url;
    #[cfg(any(not(target_family = "wasm"), target_os = "wasix", all(target_family = "wasm", not(target_os = "unknown"))))]
    use socket2::{Domain, Protocol as SocketProtocol, Socket, Type, SockAddr};
    #[cfg(all(any(not(target_family = "wasm"), target_os = "wasix", all(target_family = "wasm", not(target_os = "unknown"))), any(unix, target_os = "wasix", target_os = "wasi")))]
    use std::os::unix::io::{FromRawFd, IntoRawFd};
    #[cfg(all(any(not(target_family = "wasm"), target_os = "wasix", all(target_family = "wasm", not(target_os = "unknown"))), all(not(unix), not(target_os = "wasix"), not(target_os = "wasi"), windows)))]
    use std::os::windows::io::{FromRawSocket, IntoRawSocket};
    use std::{io, net::SocketAddr};
    use tokio::net::TcpSocket as TokioTcp;

    # [cfg(feature = "tokio-timer")]
    pub use {
    time::{Interval, interval},
    tokio::time::{MissedTickBehavior, Sleep as Delay, Timeout, sleep as delay_for, timeout},
    };

    # [cfg(feature = "tokio-timer")]
    mod time {
        use futures::Stream;
        use std::{
            pin::Pin,
            task::{Context, Poll},
            time::Duration,
        };
        use tokio::time::{
            Instant, Interval as Inner, MissedTickBehavior, interval_at as inner_interval,
        };

        pub struct Interval(Inner);

        impl Interval {
            /// Same as tokio::time::interval
            pub fn new(period: Duration) -> Self {
            Self::new_at(Duration::ZERO, period)
            }

            /// Same as tokio::time::interval_at
            pub fn new_at(start_since_now: Duration, period: Duration) -> Self {
            Self (inner_interval(Instant::now() + start_since_now, period))
            }

            pub fn set_missed_tick_behavior( & mut self, behavior: MissedTickBehavior) {
                self.0.set_missed_tick_behavior(behavior);
            }
        }

        impl Stream for Interval {
            type Item = ();

            fn poll_next( mut self: Pin< & mut Self >, cx: & mut Context<'_ > ) -> Poll<Option<() > > {
            match self.0.poll_tick(cx) {
            Poll::Ready(_) => Poll::Ready(Some(())),
            Poll::Pending => Poll::Pending,
            }
            }

            fn size_hint( & self) -> (usize, Option<usize>) {
            (usize::MAX, None)
            }
        }

        pub fn interval(period: Duration) -> Interval {
            Interval::new(period)
        }
    }

    pub (crate) fn listen(addr: SocketAddr, tcp_config: TcpSocketConfig) -> io::Result<TcpListener> {
        println!("[tentacle::runtime] listen on addr: {:?}", addr);
        
        let std_listener = std::net::TcpListener::bind(addr)?;
        println!("[tentacle::runtime] std::net::TcpListener bound.");

        // On WASIX, we might not have unix or windows set, but target_os = "wasix" should be true.
        // However, let's use a more robust way to define the socket.
        #[cfg(any(unix, target_os = "wasix", target_os = "wasi"))]
        let socket = unsafe {
            println!("[tentacle::runtime] socket created from std_listener (unix/wasix/wasi).");
            Socket::from_raw_fd(std_listener.into_raw_fd())
        };
        #[cfg(all(not(unix), not(target_os = "wasix"), not(target_os = "wasi"), windows))]
        let socket = unsafe {
            use std::os::windows::io::IntoRawSocket;
            println!("[tentacle::runtime] socket created from std_listener (windows).");
            Socket::from_raw_socket(std_listener.into_raw_socket())
        };
        #[cfg(all(not(unix), not(target_os = "wasix"), not(target_os = "wasi"), not(windows)))]
        let socket: Socket = {
             println!("[tentacle::runtime] Unsupported platform in listen!");
             return Err(io::Error::new(io::ErrorKind::Other, "Unsupported platform"));
        };

        // reuse addr and reuse port's situation on each platform
        // https://stackoverflow.com/questions/14388706/how-do-so-reuseaddr-and-so-reuseport-differ

        // user can disable it on socket_transformer
        #[cfg(all(unix, not(target_os = "wasix")))]
        {
            println!("[tentacle::runtime] setting reuse_address...");
            socket.set_reuse_address(true) ?;
        }

        let transformer_context = TransformerContext::new_listen(addr);
        println!("[tentacle::runtime] calling socket_transformer...");
        let t = (tcp_config.socket_transformer)(TcpSocket {inner: socket}, transformer_context) ?;

        println!("[tentacle::runtime] setting non-blocking...");
        t.inner.set_nonblocking(true) ?;
        println!("[tentacle::runtime] socket set non-blocking.");

        // `bind` was already called via std::net::TcpListener::bind
        // On WASI/WASIX, std::net::TcpListener::bind already puts the socket in listening state.
        // Calling listen() again might return "Not supported".
        if !cfg!(any(target_os = "wasix", target_os = "wasi")) {
            println!("[tentacle::runtime] listening on socket (backlog=1024)...");
            t.inner.listen(1024)?;
            println!("[tentacle::runtime] socket listening.");
        } else {
            println!("[tentacle::runtime] skipping redundant listen() on wasix/wasi.");
        }
        
        // safety: fd convert by socket2
        unsafe {
            #[cfg(any(unix, target_os = "wasix", target_os = "wasi"))]
            {
                println!("[tentacle::runtime] converting to tokio TcpListener (wasix/wasi/unix)...");
                Ok(TcpListener::from_std(std::net::TcpListener::from_raw_fd(t.inner.into_raw_fd()))?)
            }
            #[cfg(all(not(unix), not(target_os = "wasix"), not(target_os = "wasi"), windows))]
            {
                println!("[tentacle::runtime] converting to tokio TcpListener (windows)...");
                Ok(TcpListener::from_std(std::net::TcpListener::from_raw_socket(t.inner.into_raw_socket()))?)
            }
            #[cfg(all(not(unix), not(target_os = "wasix"), not(target_os = "wasi"), not(windows)))]
            {
                let _ = t;
                let _ = addr;
                let _ = tcp_config;
                return Err(io::Error::new(io::ErrorKind::Other, "Unsupported platform"));
            }
        }
    }

async fn connect_direct(
    addr: SocketAddr,
    socket_transformer: TcpSocketTransformer,
) -> io::Result<TcpStream> {
    let socket: Socket = if cfg!(any(unix, target_os = "wasix", target_os = "wasi")) {
        let std_stream = std::net::TcpStream::connect(addr)?;
        #[cfg(any(unix, target_os = "wasix", target_os = "wasi"))]
        unsafe {
            Socket::from_raw_fd(std_stream.into_raw_fd())
        }
        #[cfg(all(not(unix), not(target_os = "wasix"), not(target_os = "wasi")))]
        {
            unreachable!("This code should only run on unix/wasix/wasi");
        }
    } else if cfg!(windows) {
        let domain = Domain::for_address(addr);
        Socket::new(domain, Type::STREAM, Some(SocketProtocol::TCP))?
    } else {
        return Err(io::Error::new(io::ErrorKind::Other, "Unsupported platform"));
    };

    let transformer_context = TransformerContext::new_dial(addr);
    let t = socket_transformer(TcpSocket { inner: socket }, transformer_context)?;
    t.inner.set_nonblocking(true)?;

    #[cfg(any(unix, target_os = "wasix", target_os = "wasi"))]
    unsafe {
        let tokio_socket = TokioTcp::from_raw_fd(t.inner.into_raw_fd());
        if cfg!(any(target_os = "wasix", target_os = "wasi")) {
            Ok(TcpStream::from_std(std::net::TcpStream::from_raw_fd(tokio_socket.into_raw_fd()))?)
        } else {
            tokio_socket.connect(addr).await
        }
    }
    #[cfg(all(not(unix), not(target_os = "wasix"), not(target_os = "wasi"), windows))]
    unsafe {
        let tokio_socket = TokioTcp::from_raw_socket(t.inner.into_raw_socket());
        tokio_socket.connect(addr).await
    }
    #[cfg(all(not(unix), not(target_os = "wasix"), not(target_os = "wasi"), not(windows)))]
    {
        let _ = t;
        let _ = addr;
        Err(io::Error::new(io::ErrorKind::Other, "Unsupported platform"))
    }
}

#[cfg(all(not(target_family = "wasm"), not(target_os = "wasix")))]
async fn connect_by_proxy(
    target_addr: String,
    target_port: u16,
    mut proxy_server_url: url::Url,
    proxy_random_auth: bool,
) -> io::Result<TcpStream> {
    if proxy_random_auth {
        // Generate random username and password for authentication
        if proxy_server_url.username().is_empty() {
            let (random_username, random_passwd) = random_auth();
            proxy_server_url
                .set_username(&random_username)
                .map_err(|_| io::Error::other("failed to set username"))?;
            proxy_server_url
                .set_password(Some(&random_passwd))
                .map_err(|_| io::Error::other("failed to set password"))?;
        } else {
            // if username is not empty, then use the original username and password
        }
    }

    socks5::connect(proxy_server_url.clone(), target_addr.clone(), target_port)
        .await
        .map_err(|err| {
            io::Error::other(
                format!(
                    "socks5_connect to target_addr: {}, target_port: {} by proxy_server: {} failed, err: {}",
                    target_addr, target_port, crate::utils::redact_auth_from_url(&proxy_server_url), err
                ),
            )
        })
}

#[cfg(any(target_family = "wasm", target_os = "wasix"))]
async fn connect_by_proxy(
    _target_addr: String,
    _target_port: u16,
    _proxy_server_url: url::Url,
    _proxy_random_auth: bool,
) -> io::Result<TcpStream> {
    Err(io::Error::other("proxy not supported on this platform"))
}

pub(crate) async fn connect(
    target_addr: SocketAddr,
    tcp_config: TcpSocketConfig,
) -> io::Result<TcpStream> {
    let TcpSocketConfig {
        socket_transformer,
        proxy_url,
        onion_url: _,
        proxy_random_auth,
    } = tcp_config;

    match proxy_url {
        Some(proxy_url) => connect_by_proxy(
            target_addr.ip().to_string(),
            target_addr.port(),
            proxy_url.clone(),
            proxy_random_auth,
        )
            .await
            .map_err(|err| {
                io::Error::other(format!("connect_by_proxy: {}, error: {}", proxy_url, err))
            }),
        None => connect_direct(target_addr, socket_transformer).await,
    }
}

pub(crate) async fn connect_onion(
    onion_addr: MultiAddr,
    tcp_config: TcpSocketConfig,
) -> io::Result<TcpStream> {
    let TcpSocketConfig {
        socket_transformer: _,
        proxy_url,
        onion_url,
        proxy_random_auth,
    } = tcp_config;
    let tor_server_url = onion_url.or(proxy_url).ok_or(io::Error::other(
        "need tor proxy server to connect to onion address",
    ))?;

    let onion_protocol = onion_addr
        .iter()
        .find_map(|protocol| {
            if let Protocol::Onion3(onion_address) = protocol {
                Some(onion_address)
            } else {
                None
            }
        })
        .ok_or(io::Error::other(format!(
            "No Onion3 address found. in {}",
            onion_addr
        )))?;

    let onion_str = onion_protocol.hash_string() + ".onion";
    let onion_port = onion_protocol.port();

    connect_by_proxy(onion_str, onion_port, tor_server_url, proxy_random_auth).await
}
