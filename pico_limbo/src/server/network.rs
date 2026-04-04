use crate::server::client_data::ClientData;
use crate::server::packet_handler::{PacketHandler, PacketHandlerError};
use crate::server::packet_registry::{
    PacketRegistry, PacketRegistryDecodeError, PacketRegistryEncodeError,
};
use crate::server::shutdown_signal::shutdown_signal;
use crate::server_state::ServerState;
use futures::StreamExt;
use minecraft_packets::login::login_disconnect_packet::LoginDisconnectPacket;
use minecraft_packets::play::client_bound_keep_alive_packet::ClientBoundKeepAlivePacket;
use minecraft_packets::play::disconnect_packet::DisconnectPacket;
use minecraft_protocol::prelude::State;
use net::packet_stream::PacketStreamError;
use net::raw_packet::RawPacket;
use std::num::TryFromIntError;
use std::sync::Arc;
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tracing::{debug, error, info, trace, warn};
use std::net::SocketAddr;
use minecraft_packets::play::transfer_packet::TransferPacket;

pub struct Server {
    state: Arc<RwLock<ServerState>>,
    listen_address: String,
}

impl Server {
    pub fn new(listen_address: &impl ToString, state: ServerState) -> Self {
        Self {
            state: Arc::new(RwLock::new(state)),
            listen_address: listen_address.to_string(),
        }
    }

    pub async fn run(self) {
        let listener = match TcpListener::bind(&self.listen_address).await {
            Ok(sock) => sock,
            Err(err) => {
                error!("Failed to bind to {}: {}", self.listen_address, err);
                std::process::exit(1);
            }
        };

        info!("Listening on: {}", self.listen_address);
        self.accept(&listener).await;
    }

    pub async fn accept(self, listener: &TcpListener) {
        // [Dynastia] Start upstream status poller if configured
        if let Ok(upstream_addr) = std::env::var("UPSTREAM_STATUS") {
            let handle = self.state.read().await.upstream_status_handle();

            // Load persisted cache from disk (survives VPS reboots)
            if let Ok(json) = std::fs::read_to_string("./upstream_status.json") {
                if let Ok(mut c) = handle.write() {
                    *c = Some(json);
                }
                self.state.write().await.set_reply_to_status_live(true);
                info!("Loaded persisted upstream status from disk");
            } else {
                // No cache — don't reply to status pings until first poll succeeds
                self.state.write().await.set_reply_to_status_live(false);
                info!("No persisted upstream status — server appears offline until first poll");
            }

            let state_for_poller = std::sync::Arc::clone(&self.state);
            std::thread::spawn(move || {
                poll_upstream_status_thread(upstream_addr, handle, state_for_poller);
            });
        }

        loop {
            tokio::select! {
                 accept_result = listener.accept() => {
                    match accept_result {
                        Ok((socket, addr)) => {
                            debug!("Accepted connection from {}", addr);
                        let state_clone = Arc::clone(&self.state);
                            tokio::spawn(async move {
                                handle_client(socket, addr, state_clone).await;
                            });
                        }
                        Err(e) => {
                            error!("Failed to accept a connection: {:?}", e);
                        }
                    }
                },

                 () = shutdown_signal() => {
                    info!("Shutdown signal received, shutting down gracefully.");
                    break;
                }
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum PacketProcessingError {
    #[error("Client disconnected")]
    Disconnected,

    #[error("Packet not found version={0} state={1} packet_id={2}")]
    DecodePacketError(i32, State, u8),

    #[error("{0}")]
    Custom(String),
}

impl From<PacketHandlerError> for PacketProcessingError {
    fn from(e: PacketHandlerError) -> Self {
        match e {
            PacketHandlerError::Custom(reason) => Self::Custom(reason),
            PacketHandlerError::InvalidState(reason, should_warn) => {
                if should_warn { warn!("{reason}"); } else { debug!("{reason}"); }
                Self::Disconnected
            }
        }
    }
}

impl From<PacketRegistryDecodeError> for PacketProcessingError {
    fn from(e: PacketRegistryDecodeError) -> Self {
        match e {
            PacketRegistryDecodeError::NoCorrespondingPacket(version, state, packet_id) => {
                Self::DecodePacketError(version, state, packet_id)
            }
            _ => Self::Custom(e.to_string()),
        }
    }
}

impl From<PacketRegistryEncodeError> for PacketProcessingError {
    fn from(e: PacketRegistryEncodeError) -> Self {
        Self::Custom(e.to_string())
    }
}

impl From<TryFromIntError> for PacketProcessingError {
    fn from(e: TryFromIntError) -> Self {
        Self::Custom(e.to_string())
    }
}

impl From<PacketStreamError> for PacketProcessingError {
    fn from(value: PacketStreamError) -> Self {
        match value {
            PacketStreamError::Io(ref e)
                if e.kind() == std::io::ErrorKind::UnexpectedEof
                    || e.kind() == std::io::ErrorKind::ConnectionReset =>
            {
                Self::Disconnected
            }
            _ => Self::Custom(value.to_string()),
        }
    }
}

async fn process_packet(addr: SocketAddr, 
    client_data: &ClientData,
    server_state: &Arc<RwLock<ServerState>>,
    raw_packet: RawPacket,
    was_in_play_state: &mut bool,
) -> Result<(), PacketProcessingError> {
    let mut client_state = client_data.client().await;
    let protocol_version = client_state.protocol_version();
    let state = client_state.state();
    let decoded_packet = PacketRegistry::decode_packet(protocol_version, state, raw_packet)?;

    let batch = {
        let server_state_guard = server_state.read().await;
        decoded_packet.handle(&mut client_state, &server_state_guard)?
    };

    let protocol_version = client_state.protocol_version();
    let state = client_state.state();

    if !*was_in_play_state && state == State::Play {
        *was_in_play_state = true;
        server_state.write().await.increment();
        let username = client_state.get_username();
        debug!(
            "{} joined using version {}",
            username,
            protocol_version.humanize()
        );
        info!("{} joined the game", username,);

        // --- Dynastia XDP patch: write IP to game_validated + auto-transfer ---
        if let std::net::IpAddr::V4(ipv4) = addr.ip() {
            let ip_bytes = ipv4.octets();
            let ip_u32 = u32::from_le_bytes(ip_bytes);
            // Write to BPF map via bpf() syscall
            match write_game_validated(ip_u32) {
                Ok(()) => info!("game_validated: added {}", ipv4),
                Err(e) => warn!("game_validated write failed: {}", e),
            }
        }

        // Auto-transfer to game server after a short delay
        let transfer_host = std::env::var("TRANSFER_HOST")
            .unwrap_or_else(|_| "play.dynastia.fr".to_string());
        let transfer_port: i32 = std::env::var("TRANSFER_PORT")
            .unwrap_or_else(|_| "25568".to_string())
            .parse()
            .unwrap_or(25568);
        if transfer_port > 0 {
            // Version gate: hard floor (766 = 1.20.5, Transfer packets) + configurable min.
            let effective_floor = {
                let ss = server_state.read().await;
                std::cmp::max(766, ss.version_gate_protocol())
            };

            if protocol_version < minecraft_protocol::prelude::ProtocolVersion::from(effective_floor) {
                let (kick_msg, version_name) = {
                    let ss = server_state.read().await;
                    (ss.version_gate_kick_message(), ss.version_gate_version_name())
                };
                let msg = kick_msg.replace("<version>", &version_name).replace("\n", "
");
                warn!("{} kicked: protocol {} < {} ({})", username, protocol_version.version_number(), effective_floor, version_name);
                drop(client_state);
                let _ = kick_client(client_data, msg).await;
                return Err(PacketProcessingError::Disconnected);
            }

            drop(client_state);
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            let mut cs = client_data.client().await;
            let pv = cs.protocol_version();
            drop(cs);
            let pkt = TransferPacket {
                host: transfer_host.clone(),
                port: transfer_port.into(),
            };
            if let Ok(raw) = PacketRegistry::Transfer(pkt).encode_packet(pv) {
                let _ = client_data.write_packet(raw).await;
                info!("Transferred {} to {}:{}", username, transfer_host, transfer_port);
            }
            // Return early — skip the rest of the batch processing for this packet
            return Ok(());
        }
    }

    let mut stream = batch.into_stream();
    while let Some(pending_packet) = stream.next().await {
        let enable_compression = matches!(pending_packet, PacketRegistry::SetCompression(..));
        let raw_packet = pending_packet.encode_packet(protocol_version)?;
        client_data.write_packet(raw_packet).await?;
        if enable_compression
            && let Some(compression_settings) = server_state.read().await.compression_settings()
        {
            let mut packet_stream = client_data.stream().await;
            packet_stream
                .set_compression(compression_settings.threshold, compression_settings.level);
        }
    }

    if let Some(reason) = client_state.should_kick() {
        drop(client_state);
        kick_client(client_data, reason.clone())
            .await
            .map_err(|_| PacketProcessingError::Disconnected)?;
        return Err(PacketProcessingError::Disconnected);
    }

    drop(client_state);
    client_data.enable_keep_alive_if_needed().await;

    Ok(())
}

async fn read(addr: SocketAddr, 
    client_data: &ClientData,
    server_state: &Arc<RwLock<ServerState>>,
    was_in_play_state: &mut bool,
) -> Result<(), PacketProcessingError> {
    tokio::select! {
        result = client_data.read_packet() => {
            let raw_packet = result?;
            process_packet(addr, client_data, server_state, raw_packet, was_in_play_state).await?;
        }
        () = client_data.keep_alive_tick() => {
            send_keep_alive(client_data).await?;
        }
    }
    Ok(())
}

async fn handle_client(socket: TcpStream, addr: SocketAddr, server_state: Arc<RwLock<ServerState>>) {
    let client_data = ClientData::new(socket);
    let mut was_in_play_state = false;

    loop {
        match read(addr, &client_data, &server_state, &mut was_in_play_state).await {
            Ok(()) => {}
            Err(PacketProcessingError::Disconnected) => {
                debug!("Client disconnected");
                break;
            }
            Err(PacketProcessingError::Custom(e)) => {
                debug!("Error processing packet: {}", e);
            }
            Err(PacketProcessingError::DecodePacketError(version, state, packet_id)) => {
                trace!(
                    "Unknown packet received: version={version} state={state} packet_id={packet_id}"
                );
            }
        }
    }

    let _ = client_data.shutdown().await;

    if was_in_play_state {
        server_state.write().await.decrement();
        let username = client_data.client().await.get_username();
        info!("{} left the game", username);
    }
}

async fn kick_client(
    client_data: &ClientData,
    reason: String,
) -> Result<(), PacketProcessingError> {
    let (protocol_version, state) = {
        let state = client_data.client().await;
        (state.protocol_version(), state.state())
    };
    let packet = match state {
        State::Login => {
            debug!("Login disconnect");
            PacketRegistry::LoginDisconnect(LoginDisconnectPacket::text(reason))
        }
        State::Configuration => {
            debug!("Configuration disconnect");
            PacketRegistry::ConfigurationDisconnect(DisconnectPacket::text(reason))
        }
        State::Play => {
            debug!("Play disconnect");
            PacketRegistry::PlayDisconnect(DisconnectPacket::text(reason))
        }
        _ => {
            debug!("A user was disconnected from a state where no packet can be sent");
            return Err(PacketProcessingError::Disconnected);
        }
    };
    if let Ok(raw_packet) = packet.encode_packet(protocol_version) {
        client_data.write_packet(raw_packet).await?;
        client_data.shutdown().await?;
    }

    Ok(())
}

async fn send_keep_alive(client_data: &ClientData) -> Result<(), PacketProcessingError> {
    let (protocol_version, state) = {
        let client = client_data.client().await;
        (client.protocol_version(), client.state())
    };

    if state == State::Play {
        let packet = PacketRegistry::ClientBoundKeepAlive(ClientBoundKeepAlivePacket::random()?);
        let raw_packet = packet.encode_packet(protocol_version)?;
        client_data.write_packet(raw_packet).await?;
    }

    Ok(())
}


/// Poll upstream game server status every 10 seconds and cache the JSON response.
fn poll_upstream_status_thread(
    addr: String,
    cache: std::sync::Arc<std::sync::RwLock<Option<String>>>,
    server_state: std::sync::Arc<tokio::sync::RwLock<ServerState>>,
) {
    let poll_secs: u64 = std::env::var("UPSTREAM_POLL_SECS")
        .unwrap_or_else(|_| "10".to_string())
        .parse()
        .unwrap_or(10);

    eprintln!("[poller] Starting for {} (every {}s)", addr, poll_secs);
    let mut backend_was_up = true;

    std::thread::sleep(std::time::Duration::from_secs(2));

    loop {
        match fetch_upstream_status_blocking(&addr) {
            Ok(json) => {
                // Backend is up — cache the full status and persist to disk
                let _ = std::fs::write("./upstream_status.json", &json);
                if let Ok(mut c) = cache.write() {
                    *c = Some(json);
                }
                // Enable status replies (AtomicBool + try_write ServerState)
                if let Ok(mut s) = server_state.try_write() {
                    if !s.reply_to_status() {
                        s.set_reply_to_status_live(true);
                        eprintln!("[poller] First successful poll — status replies enabled");
                    }
                }
                if !backend_was_up {
                    eprintln!("[poller] Backend is back up");
                    backend_was_up = true;
                }
            }
            Err(e) => {
                eprintln!("[poller] fetch failed: {}", e);
                if backend_was_up {
                    // Backend just went down — zero the player count in cache
                    if let Ok(mut c) = cache.write() {
                        if let Some(ref cached_json) = *c {
                            if let Ok(mut status) = serde_json::from_str::<serde_json::Value>(cached_json) {
                                if let Some(players) = status.get_mut("players") {
                                    players["online"] = serde_json::Value::Number(0.into());
                                }
                                *c = Some(serde_json::to_string(&status).unwrap_or_else(|_| cached_json.clone()));
                            }
                        }
                    }
                    eprintln!("[poller] Backend down — player count zeroed");
                    backend_was_up = false;
                }
            }
        }
        std::thread::sleep(std::time::Duration::from_secs(poll_secs));
    }
}

fn fetch_upstream_status_blocking(addr: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::time::Duration;

    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(3)))?;
    stream.set_write_timeout(Some(Duration::from_secs(3)))?;

    // MC status handshake
    let host = b"play.dynastia.fr";
    let mut hs = vec![0x00u8];
    hs.extend_from_slice(&[0x80, 0x06]);
    hs.push(host.len() as u8);
    hs.extend_from_slice(host);
    hs.extend_from_slice(&25565u16.to_be_bytes());
    hs.push(0x01);
    let mut pkt = vec![hs.len() as u8];
    pkt.extend_from_slice(&hs);
    stream.write_all(&pkt)?;
    stream.write_all(&[0x01, 0x00])?;

    let mut buf = vec![0u8; 65536];
    let mut total = 0;
    for _ in 0..20 {
        match stream.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => {
                total += n;
                if let Some(json) = extract_status_json(&buf[..total]) {
                    return Ok(json);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock
                || e.kind() == std::io::ErrorKind::TimedOut => break,
            Err(e) => return Err(e.into()),
        }
    }
    extract_status_json(&buf[..total]).ok_or_else(|| "Incomplete status response".into())
}

fn extract_status_json(data: &[u8]) -> Option<String> {
    let mut i = 0;
    // Skip packet length varint
    while i < data.len() && data[i] & 0x80 != 0 { i += 1; }
    if i >= data.len() { return None; }
    i += 1;
    // Skip packet id varint
    while i < data.len() && data[i] & 0x80 != 0 { i += 1; }
    if i >= data.len() { return None; }
    i += 1;
    // Read string length varint
    let mut str_len: usize = 0;
    let mut shift = 0;
    while i < data.len() && data[i] & 0x80 != 0 {
        str_len |= ((data[i] & 0x7f) as usize) << shift;
        shift += 7;
        i += 1;
    }
    if i >= data.len() { return None; }
    str_len |= ((data[i] & 0x7f) as usize) << shift;
    i += 1;
    if i + str_len > data.len() { return None; }
    String::from_utf8(data[i..i+str_len].to_vec()).ok()
}

/// Write a player IP to the game_validated BPF map (pinned at /sys/fs/bpf/game_validated).
/// Uses raw bpf() syscall to update the map without any BPF library dependency.
fn write_game_validated(ip: u32) -> std::io::Result<()> {
    use std::os::unix::io::RawFd;

    // Open the pinned map
    let path = std::ffi::CString::new("/sys/fs/bpf/game_validated").unwrap();

    #[repr(C)]
    struct BpfAttrObjGet {
        pathname: u64,
        bpf_fd: u32,
        file_flags: u32,
    }

    let mut attr = BpfAttrObjGet {
        pathname: path.as_ptr() as u64,
        bpf_fd: 0,
        file_flags: 0,
    };

    // BPF_OBJ_GET = 7
    let fd: RawFd = unsafe {
        libc::syscall(
            libc::SYS_bpf,
            7i32, // BPF_OBJ_GET
            &mut attr as *mut _ as *mut libc::c_void,
            std::mem::size_of::<BpfAttrObjGet>(),
        )
    } as RawFd;

    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }

    // Update the map: key = IP (u32), value = timestamp (u64, we use 1 as placeholder)
    let key = ip;
    let value: u64 = 1; // PicoLimbo doesnt have bpf_ktime, use 1 as "valid" marker

    #[repr(C)]
    struct BpfAttrMapElem {
        map_fd: u32,
        _pad: u32,
        key: u64,
        value_or_next: u64,
        flags: u64,
    }

    let mut update_attr = BpfAttrMapElem {
        map_fd: fd as u32,
        _pad: 0,
        key: &key as *const u32 as u64,
        value_or_next: &value as *const u64 as u64,
        flags: 0, // BPF_ANY
    };

    // BPF_MAP_UPDATE_ELEM = 2
    let ret = unsafe {
        libc::syscall(
            libc::SYS_bpf,
            2i32,
            &mut update_attr as *mut _ as *mut libc::c_void,
            std::mem::size_of::<BpfAttrMapElem>(),
        )
    };

    unsafe { libc::close(fd) };

    if ret < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}
