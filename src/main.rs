use anyhow::Result;
use std::{
    collections::HashMap,
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{tcp::OwnedReadHalf, tcp::OwnedWriteHalf, TcpListener, TcpStream, UdpSocket},
    signal,
    sync::Mutex,
    time,
};

const DISCOVER_PORT: u16 = 9001; // UDP discovery port
const CHAT_PORT: u16 = 9000; // default TCP chat port
const BROADCAST_INTERVAL_SECS: u64 = 3;
const DISCOVER_MSG_PREFIX: &str = "ADHOC_CHAT_DISCOVER";

#[derive(Clone, Debug)]
struct PeerInfo {
    name: String,
    addr: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<()> {
    // parse optional args: name and port
    let args: Vec<String> = env::args().collect();
    let name = args.get(1).cloned().unwrap_or_else(|| whoami::username());

    // Async-friendly TCP port assignment
    let tcp_port: u16 = if let Some(p) = args.get(2).and_then(|s| s.parse().ok()) {
        p
    } else {
        TcpListener::bind("0.0.0.0:0").await?.local_addr()?.port()
    };

    println!("Ad-hoc terminal chat");
    println!("Name: {}", name);
    println!("TCP port: {}", tcp_port);
    println!("Discovery UDP port: {}", DISCOVER_PORT);
    println!("Type messages and press Enter to send to all connected peers.");
    println!("Press Ctrl+C to quit.");
    println!("---");

    // Shared state of connected peers
    let peers = Arc::new(Mutex::new(HashMap::<String, OwnedWriteHalf>::new()));

    // TCP listener for incoming chat connections
    let listener_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), tcp_port);
    let tcp_listener = TcpListener::bind(listener_addr).await?;
    let peers_for_accept = peers.clone();
    let name_for_accept = name.clone();
    tokio::spawn(async move {
        loop {
            match tcp_listener.accept().await {
                Ok((stream, addr)) => {
                    eprintln!("Incoming TCP connection from {}", addr);
                    let peers_inner = peers_for_accept.clone();
                    let my_name = name_for_accept.clone();
                    tokio::spawn(async move {
                        if let Err(e) =
                            handle_incoming_tcp(stream, peers_inner, addr, my_name).await
                        {
                            eprintln!("Connection handler error: {:?}", e);
                        }
                    });
                }
                Err(e) => {
                    eprintln!("TCP accept error: {:?}", e);
                }
            }
        }
    });

    // UDP socket for discovery
    let udp_port: u16 = args
        .get(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(DISCOVER_PORT);
    let udp_bind = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), udp_port);
    let udp_socket = UdpSocket::bind(udp_bind).await?;
    udp_socket.set_broadcast(true)?;
    let udp_socket = Arc::new(udp_socket);
    let udp_socket_rcv = udp_socket.clone();
    let peers_for_udp = peers.clone();
    let name_for_udp = name.clone();

    // Discovery message listener
    tokio::spawn(async move {
        let mut buf = [0u8; 1024];
        loop {
            match udp_socket_rcv.recv_from(&mut buf).await {
                Ok((n, src)) => {
                    if n == 0 {
                        continue;
                    }
                    let s = String::from_utf8_lossy(&buf[..n]).to_string();
                    if let Some((peer_name, peer_port)) = parse_discover_msg(&s) {
                        let peer_addr = SocketAddr::new(src.ip(), peer_port);

                        // ✅ use updated is_self check
                        if is_self(&peer_name, &peer_addr, &name_for_udp, tcp_port) {
                            continue;
                        }

                        let peer_id = peer_id_str(&peer_name, &peer_addr);
                        let mut locked = peers_for_udp.lock().await;
                        if !locked.contains_key(&peer_id) {
                            drop(locked); // release lock before await
                            match TcpStream::connect(peer_addr).await {
                                Ok(stream) => {
                                    eprintln!(
                                        "Connected to discovered peer {} at {}",
                                        peer_name, peer_addr
                                    );
                                    let (read_half, mut write_half) = stream.into_split();
                                    let hn = format!("HN::{}\n", name_for_udp);
                                    if let Err(e) = write_half.write_all(hn.as_bytes()).await {
                                        eprintln!("Handshake send failed: {:?}", e);
                                        continue;
                                    }
                                    let mut locked = peers_for_udp.lock().await;
                                    locked.insert(peer_id.clone(), write_half);
                                    drop(locked);

                                    let peers_clone = peers_for_udp.clone();
                                    let peer_name_clone = peer_name.clone();
                                    tokio::spawn(async move {
                                        if let Err(e) =
                                            read_tcp_loop(read_half, peer_name_clone, peer_addr, peers_clone).await
                                        {
                                            eprintln!("Read loop ended: {:?}", e);
                                        }
                                    });
                                }
                                Err(e) => {
                                    eprintln!("Failed to connect to {}: {} -> {}", peer_name, peer_addr, e);
                                }
                            }
                        }
                    }
                }
                Err(e) => eprintln!("UDP recv error: {:?}", e),
            }
        }
    });

    // Periodic broadcast
    let udp_bcast = UdpSocket::bind("0.0.0.0:0").await?;
    udp_bcast.set_broadcast(true)?;
    let udp_bcast = Arc::new(udp_bcast);
    let broadcast_target = SocketAddr::new(IpAddr::V4(Ipv4Addr::BROADCAST), DISCOVER_PORT);
    let name_clone = name.clone();
    let udp_writer_peers = peers.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(BROADCAST_INTERVAL_SECS));
        loop {
            interval.tick().await;
            let msg = format!("{}:{}:{}", DISCOVER_MSG_PREFIX, name_clone, tcp_port);
            if let Err(e) = udp_bcast.send_to(msg.as_bytes(), broadcast_target).await {
                eprintln!("UDP broadcast send error: {:?}", e);
            }
        }
    });

    // Input loop
    let stdin = tokio::io::stdin();
    let reader = BufReader::new(stdin);
    let mut lines = reader.lines();

    // Ctrl+C shutdown
    let peers_for_shutdown = peers.clone();
    tokio::spawn(async move {
        signal::ctrl_c().await.unwrap();
        eprintln!("\nCtrl+C received. Shutting down...");
        let mut locked = peers_for_shutdown.lock().await;
        for (_, mut stream) in locked.drain() {
            let _ = stream.shutdown().await;
        }
        std::process::exit(0);
    });

    // Main loop: read stdin and send to peers
    while let Ok(Some(line)) = lines.next_line().await {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let msg = format!("MSG::{}::{}\n", name, trimmed);
        let mut locked = peers.lock().await;
        if locked.is_empty() {
            println!("(no peers) > {}", trimmed);
        } else {
            let mut to_remove = Vec::new();
            for (peer_id, writer) in locked.iter_mut() {
                if let Err(e) = writer.write_all(msg.as_bytes()).await {
                    eprintln!("Failed to send to {}: {:?}", peer_id, e);
                    to_remove.push(peer_id.clone());
                }
            }
            for k in to_remove {
                locked.remove(&k);
            }
            println!("me > {}", trimmed);
        }
    }

    Ok(())
}

/// Handle incoming TCP connection
async fn handle_incoming_tcp(
    stream: TcpStream,
    peers: Arc<Mutex<HashMap<String, OwnedWriteHalf>>>,
    addr: SocketAddr,
    my_name: String,
) -> Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    let mut s = String::new();
    let n = reader.read_line(&mut s).await?;
    if n == 0 {
        return Ok(());
    }
    let peer_name = if s.starts_with("HN::") {
        s.trim_start_matches("HN::").trim().to_string()
    } else {
        s.trim().to_string()
    };

    let hn = format!("HN::{}\n", my_name);
    let _ = write_half.write_all(hn.as_bytes()).await;

    let peer_id = peer_id_str(&peer_name, &addr);
    {
        let mut locked = peers.lock().await;
        locked.insert(peer_id.clone(), write_half);
    }

    read_tcp_loop_from_reader(reader, peer_name, addr, peers).await?;
    Ok(())
}

/// Read loop for incoming messages
async fn read_tcp_loop_from_reader(
    mut reader: BufReader<OwnedReadHalf>,
    peer_name: String,
    addr: SocketAddr,
    peers: Arc<Mutex<HashMap<String, OwnedWriteHalf>>>,
) -> Result<()> {
    let peer_id = peer_id_str(&peer_name, &addr);
    let mut buf = String::new();
    loop {
        buf.clear();
        let bytes_read = reader.read_line(&mut buf).await?;
        if bytes_read == 0 {
            eprintln!("Peer {} disconnected", peer_id);
            let mut locked = peers.lock().await;
            locked.remove(&peer_id);
            break;
        }
        if buf.starts_with("MSG::") {
            if let Some((_, rest)) = buf.split_once("MSG::") {
                if let Some((_from, payload)) = rest.split_once("::") {
                    let payload = payload.trim_end();
                    println!("{} > {}", peer_name, payload);
                    continue;
                }
            }
            println!("{} raw> {}", peer_name, buf.trim_end());
        } else if buf.starts_with("HN::") {
            // ignore handshake
        } else {
            let s = buf.trim_end();
            if !s.is_empty() {
                println!("{} > {}", peer_name, s);
            }
        }
    }
    Ok(())
}

/// Outgoing read loop
async fn read_tcp_loop(
    read_half: OwnedReadHalf,
    peer_name: String,
    addr: SocketAddr,
    peers: Arc<Mutex<HashMap<String, OwnedWriteHalf>>>,
) -> Result<()> {
    let reader = BufReader::new(read_half);
    read_tcp_loop_from_reader(reader, peer_name, addr, peers).await
}

/// Parse discovery message
fn parse_discover_msg(s: &str) -> Option<(String, u16)> {
    let s = s.trim();
    if !s.starts_with(DISCOVER_MSG_PREFIX) {
        return None;
    }
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() >= 3 {
        let name = parts[1].to_string();
        if let Ok(port) = parts[2].parse::<u16>() {
            return Some((name, port));
        }
    }
    None
}

/// Generate peer ID string
fn peer_id_str(name: &str, addr: &SocketAddr) -> String {
    format!("{}@{}", name, addr)
}

/// Check if peer is self
fn is_self(peer_name: &str, peer_addr: &SocketAddr, my_name: &str, my_port: u16) -> bool {
    let peer_ip = peer_addr.ip();
    let local_ip = local_ipaddress::get()
    .and_then(|s| s.parse::<IpAddr>().ok())
    .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED));

    // Consider self if:
    // 1. Same username
    // 2. Same TCP port
    // 3. Peer IP is either local_ip or loopback
    peer_name == my_name
        && peer_addr.port() == my_port
        && (peer_ip == local_ip || peer_ip.is_loopback())
}
