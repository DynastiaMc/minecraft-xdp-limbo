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
use minecraft_packets::play::transfer_packet::TransferPacket;
use minecraft_protocol::prelude::State;
use net::packet_stream::PacketStreamError;
use net::raw_packet::RawPacket;
use std::net::SocketAddr;
use std::num::TryFromIntError;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tracing::{debug, error, info, trace, warn};

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
        {
            let upstream_addr = self.state.read().await.upstream_status_addr().to_string();
            if !upstream_addr.is_empty() {
                let handle = self.state.read().await.upstream_status_handle();
                let alive = self.state.read().await.upstream_alive_handle();
                let poll_secs = self.state.read().await.upstream_poll_secs();

                // Load persisted cache from disk (survives VPS reboots).
                // If we have a cached JSON from a previous run, assume the
                // backend is still alive until the first poll proves otherwise.
                // This avoids holding the first few players on the limbo during
                // the 2s initial poller delay after a loader restart.
                if let Ok(json) = std::fs::read_to_string("./upstream_status.json") {
                    if let Ok(mut c) = handle.write() {
                        *c = Some(json);
                    }
                    self.state.write().await.set_reply_to_status_live(true);
                    alive.store(true, std::sync::atomic::Ordering::Release);
                    info!("Loaded persisted upstream status from disk");
                } else {
                    // No cache — don't reply to status pings until first poll succeeds,
                    // and hold any incoming players on the limbo until the poller
                    // confirms the backend is up.
                    self.state.write().await.set_reply_to_status_live(false);
                    alive.store(false, std::sync::atomic::Ordering::Release);
                    info!("No persisted upstream status — server appears offline until first poll");
                }

                let state_for_poller = std::sync::Arc::clone(&self.state);
                std::thread::spawn(move || {
                    poll_upstream_status_thread(
                        upstream_addr,
                        handle,
                        alive,
                        poll_secs,
                        state_for_poller,
                    );
                });
            }
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
    DecodePacketError(i32, State, i32),

    #[error("{0}")]
    Custom(String),

    /// [Dynastia] Peer opened with a pre-1.7 Legacy Server List Ping (0xFE).
    #[error("legacy server list ping")]
    LegacyPing,
}

impl From<PacketHandlerError> for PacketProcessingError {
    fn from(e: PacketHandlerError) -> Self {
        match e {
            PacketHandlerError::Custom(reason) => Self::Custom(reason),
            PacketHandlerError::InvalidState(reason, should_warn) => {
                if should_warn {
                    warn!("{reason}");
                } else {
                    debug!("{reason}");
                }
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
            // [Dynastia] Surface the legacy ping as its own variant so
            // handle_client can answer it instead of treating it as a decode
            // error and looping forever on a stream it will never parse.
            PacketStreamError::LegacyPing => Self::LegacyPing,
            _ => Self::Custom(value.to_string()),
        }
    }
}

async fn process_packet(
    addr: SocketAddr,
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

        // --- Dynastia XDP patch: auto-transfer to backend with upstream health check ---
        // If the backend is reachable, transfer immediately (happy path).
        // If the backend is down, hold the player on the limbo and spawn a
        // deferred transfer task that retries every `retry_secs` until the
        // upstream poller flips `upstream_alive` back to true, then sends the
        // Transfer packet. The BPF `game_validated` write is done at the
        // actual transfer point (not at login), so held players don't occupy a
        // whitelist slot while they're still on the limbo.
        let (transfer_host, transfer_port, retry_secs, slot_ms) = {
            let ss = server_state.read().await;
            (
                ss.upstream_transfer_host().to_string(),
                ss.upstream_transfer_port(),
                ss.upstream_retry_secs(),
                ss.upstream_transfer_rate_slot_ms(),
            )
        };
        if transfer_port > 0 {
            // Version gate is enforced in the handshake handler (before login).
            // By this point, the client already passed the version check.
            drop(client_state);
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            let pv = client_data.protocol_version().await;
            let ipv4_opt: Option<std::net::Ipv4Addr> = match addr.ip() {
                std::net::IpAddr::V4(v4) => Some(v4),
                _ => None,
            };

            // Read the live upstream flag (set by the poller thread).
            if server_state.read().await.upstream_alive() {
                // Happy path: backend reachable, transfer immediately.
                if let Some(ipv4) = ipv4_opt {
                    let ip_u32 = u32::from_le_bytes(ipv4.octets());
                    match write_game_validated(ip_u32) {
                        Ok(()) => info!("game_validated: added {}", ipv4),
                        Err(e) => warn!("game_validated write failed: {}", e),
                    }
                }
                // Generate a one-time transfer token for game port fallback.
                // Players whose IP changes between gate and game (e.g. UU Booster)
                // use this token to validate on the game port instead of IP.
                let token = generate_token();
                let token_host = format!("{:08x}.{}", token, transfer_host);
                match write_game_token(token) {
                    Ok(()) => info!("game_token: wrote {:08x}", token),
                    Err(e) => warn!("game_token write failed: {}", e),
                }
                let transfer_start = std::time::Instant::now();
                let pkt = TransferPacket {
                    host: token_host.clone(),
                    port: transfer_port.into(),
                };
                if let Ok(raw) = PacketRegistry::Transfer(pkt).encode_packet(pv) {
                    let _ = client_data.write_packet(raw).await;
                    info!(
                        "Transferred {} to {}:{} in {}ms",
                        username,
                        token_host,
                        transfer_port,
                        transfer_start.elapsed().as_millis()
                    );
                }
                // [Dynastia] After sending the Transfer packet the vanilla client
                // opens a NEW TCP connection to the target and abandons this one
                // without a clean FIN. Returning Ok would loop back into read()
                // waiting on a packet that never comes; the socket then leaks
                // until Linux TCP keepalive (2h default) or ss -K reaps it.
                // Under sustained transfer traffic, PicoLimbo accumulates
                // thousands of stale fds (observed 11k+ on pg3 2026-07-04),
                // slows accept(), and triggers TcpExtListenOverflows on new
                // players.
                // Returning Disconnected here breaks the handle_client loop
                // which then calls client_data.shutdown() → clean TCP close,
                // fd freed immediately.
                return Err(PacketProcessingError::Disconnected);
            }

            // Backend down — hold on limbo + spawn deferred transfer task.
            let retry = std::time::Duration::from_secs(retry_secs.max(1));
            info!(
                "{} holding on limbo: backend down, will retry every {}s",
                username,
                retry.as_secs()
            );
            let stream_weak = client_data.stream_weak();
            let state_clone = Arc::clone(server_state);
            let host_clone = transfer_host.clone();
            let username_clone = username.clone();
            let hold_start = std::time::Instant::now();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(retry).await;
                    // Disconnect detection: if handle_client already exited and
                    // dropped its ClientData, the packet_stream Arc is gone and
                    // stream_weak.upgrade() returns None. Stop retrying.
                    let Some(stream_arc) = stream_weak.upgrade() else {
                        debug!(
                            "deferred transfer: {} disconnected before recovery",
                            username_clone
                        );
                        return;
                    };
                    if !state_clone.read().await.upstream_alive() {
                        drop(stream_arc);
                        continue;
                    }
                    // Backend back up — claim a rate-limit slot to stagger
                    // transfers and avoid a thundering herd when N held
                    // players wake up in the same poll cycle. slot_ms is
                    // configurable via `UpstreamConfig::transfer_rate_slot_ms`
                    // (0 = no stagger). Default 200ms → 5 transfers/s.
                    let slot = state_clone.read().await.claim_transfer_slot(slot_ms);
                    let until_slot = slot.saturating_duration_since(std::time::Instant::now());
                    // Release the stream lock during any wait so keep-alive
                    // ticks can still fire on the limbo connection.
                    drop(stream_arc);
                    if !until_slot.is_zero() {
                        tokio::time::sleep(until_slot).await;
                    }
                    // Re-check disconnect after slot wait.
                    let Some(stream_arc) = stream_weak.upgrade() else {
                        debug!(
                            "deferred transfer: {} disconnected during slot wait",
                            username_clone
                        );
                        return;
                    };
                    // Re-check alive in case backend flapped back down.
                    if !state_clone.read().await.upstream_alive() {
                        drop(stream_arc);
                        continue;
                    }
                    if let Some(ipv4) = ipv4_opt {
                        let ip_u32 = u32::from_le_bytes(ipv4.octets());
                        match write_game_validated(ip_u32) {
                            Ok(()) => info!("game_validated: added {} (deferred)", ipv4),
                            Err(e) => warn!(
                                "game_validated write failed for {}: {}",
                                ipv4, e
                            ),
                        }
                    }
                    let pkt = TransferPacket {
                        host: host_clone.clone(),
                        port: transfer_port.into(),
                    };
                    if let Ok(raw) = PacketRegistry::Transfer(pkt).encode_packet(pv) {
                        let mut stream = stream_arc.lock().await;
                        if stream.write_packet(raw).await.is_ok() {
                            info!(
                                "Deferred transfer: {} → {}:{} after {}s hold (slot +{}ms)",
                                username_clone,
                                host_clone,
                                transfer_port,
                                hold_start.elapsed().as_secs(),
                                until_slot.as_millis()
                            );
                        }
                    }
                    return;
                }
            });
            // Fall through to normal limbo join flow so the player actually
            // spawns in the limbo world (Login Play + Configuration packets).
            // The deferred task will send the Transfer packet later, once the
            // upstream poller flips `upstream_alive` back to true.
            // Re-acquire client_state: it was dropped before the 500ms sleep,
            // but the `should_kick` check below still needs it.
            client_state = client_data.client().await;
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

/// [Dynastia] Idle timeout for client sockets that go silent without a FIN.
///
/// Upstream's read loop is a select! over read_packet() and keep_alive_tick()
/// with no deadline. Keep-alive is only armed once the client reaches Play
/// (`enable_keep_alive_if_needed`), so any connection that stalls *before*
/// that — status ping, aborted handshake, login that never completes — parks
/// in read_packet() forever. The fd is never released and the kernel socket
/// stays ESTABLISHED with no timer, i.e. immortal.
///
/// That is invisible upstream (clients disconnect cleanly and hold a full
/// IPv4 with 64k ephemeral ports) but fatal behind CGNAT/MAP-T, where a
/// subscriber owns a ~2k port slice: the leak pins their whole range and they
/// can no longer open *any* connection. Observed on pg3 2026-07-22: 3369 stale
/// ESTAB sockets on :25565, 1369 of them from a single SFR MAP-T subscriber
/// whose allocation is ports 3040-4991 — ~70% of her range consumed.
///
/// 30s is twice the default keep_alive_interval_seconds (15). Play-state
/// clients — including players held in the waiting room while the backend is
/// down — resolve the select! via the keep-alive branch every 15s and never
/// reach this deadline. Only genuinely silent sockets do.
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

/// [Dynastia] Much shorter deadline for the pre-play states (Handshake,
/// Status). These states are meant to be transient — a status ping is a few
/// round-trips and a login handshake arrives within one — so a client stuck
/// here for seconds is always a degenerate case, never a healthy client.
///
/// The case we care about: the gate's cookie handshake converts the client's
/// data-bearing 3rd ACK into a bare SYN and drops the piggybacked payload
/// (the MC Handshake packet). On a high-RTT line the client piggybacks often,
/// so its first packet is silently lost; PicoLimbo then sits in Handshake
/// waiting for a packet that never comes. With the 30s IDLE_TIMEOUT that is a
/// 30s freeze in the server list ("it spins"). Closing after 3s instead makes
/// the client re-open a fresh connection — a new SYN, a new chance, usually
/// without the piggyback — turning a 30s freeze into a ~3s retry.
///
/// Scoped to Handshake/Status ONLY. Login keeps the full timeout because
/// premium auth (Mojang/nLogin) can legitimately take a few seconds, and Play
/// (including waiting-room holds while the backend is down) is governed by the
/// 15s keep-alive, never this path.
const PREPLAY_TIMEOUT: Duration = Duration::from_secs(3);

async fn read(
    addr: SocketAddr,
    client_data: &ClientData,
    server_state: &Arc<RwLock<ServerState>>,
    was_in_play_state: &mut bool,
) -> Result<(), PacketProcessingError> {
    // Pick the read deadline from the current protocol state. Read the state
    // in its own scope so the ClientState lock is not held across the select!.
    let deadline = match client_data.client().await.state() {
        State::Handshake | State::Status => PREPLAY_TIMEOUT,
        _ => IDLE_TIMEOUT,
    };
    tokio::select! {
        result = tokio::time::timeout(deadline, client_data.read_packet()) => {
            // [Dynastia] Outer Err = the deadline elapsed; treat it as a
            // disconnect so handle_client breaks and calls shutdown().
            let raw_packet = match result {
                Err(_elapsed) => return Err(PacketProcessingError::Disconnected),
                Ok(inner) => inner?,
            };
            process_packet(addr, client_data, server_state, raw_packet, was_in_play_state).await?;
        }
        () = client_data.keep_alive_tick() => {
            send_keep_alive(client_data).await?;
        }
    }
    Ok(())
}

/// [Dynastia] Flattens a chat component tree to plain text.
///
/// The proxied MOTD is a nested `{color, text, extra: [...]}` tree, so reading
/// `/description/text` alone yields an empty string on anything but the
/// simplest MOTD. Walks `text` then recurses through `extra`, dropping
/// formatting — the legacy field is NUL-delimited and plain text is the safest
/// thing to put in it.
fn flatten_component(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Array(items) => items.iter().map(flatten_component).collect(),
        serde_json::Value::Object(map) => {
            let mut out = map
                .get("text")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .to_string();
            if let Some(extra) = map.get("extra") {
                out.push_str(&flatten_component(extra));
            }
            out
        }
        _ => String::new(),
    }
}

/// [Dynastia] Fallback MOTD when the upstream status has not been polled yet.
///
/// The limbo MOTD can itself be a serialised chat component, so flatten it if
/// it parses as JSON rather than shipping raw `{"text":...}` to the client.
fn legacy_motd_fallback(raw: &str) -> String {
    serde_json::from_str::<serde_json::Value>(raw)
        .map_or_else(|_| raw.to_string(), |v| flatten_component(&v))
}

/// [Dynastia] Answers a pre-1.7 Legacy Server List Ping.
///
/// Wire format is a 0xFF kick packet carrying a UTF-16BE string:
///   §1 \0 <protocol> \0 <version> \0 <motd> \0 <online> \0 <max>
/// Length is a big-endian u16 counting UTF-16 code units, not bytes.
///
/// Why this matters: vanilla clients fall back to this protocol whenever a
/// modern ping fails. Without a handler the 0xFE decodes as a 254-byte VarInt
/// length and the connection blocks waiting for bytes that never arrive, so a
/// one-off modern failure becomes a server that spins forever in the list —
/// and the client keeps preferring legacy for that entry. Answering it makes
/// the entry resolve normally instead.
async fn respond_legacy_ping(
    client_data: &ClientData,
    server_state: &Arc<RwLock<ServerState>>,
) -> Result<(), PacketProcessingError> {
    let (protocol, version, motd, online, max) = {
        let state = server_state.read().await;
        let version = state.backend_version_name();
        let protocol = state.vg_effective_max();
        // Reuse the proxied upstream status so the legacy line matches what
        // modern clients see. Falls back to the limbo MOTD when the backend
        // status has not been polled yet.
        let (motd, online, max) = state.cached_upstream_status().map_or_else(
            || (legacy_motd_fallback(&state.motd().to_legacy()), 0i64, 0i64),
            |json| {
                serde_json::from_str::<serde_json::Value>(&json).map_or_else(
                    |_| (state.motd().to_legacy(), 0i64, 0i64),
                    |v| {
                        let motd = v
                            .pointer("/description")
                            .map(flatten_component)
                            .unwrap_or_default();
                        let online = v
                            .pointer("/players/online")
                            .and_then(serde_json::Value::as_i64)
                            .unwrap_or(0);
                        let max = v
                            .pointer("/players/max")
                            .and_then(serde_json::Value::as_i64)
                            .unwrap_or(0);
                        (motd, online, max)
                    },
                )
            },
        );
        (protocol, version, motd, online, max)
    };

    // The legacy payload is NUL-separated, so strip any NUL and newline from
    // the MOTD or the client mis-parses the remaining fields.
    let motd = motd.replace(['\0', '\n'], " ");
    let payload = format!("\u{00A7}1\0{protocol}\0{version}\0{motd}\0{online}\0{max}");

    let utf16: Vec<u16> = payload.encode_utf16().collect();
    let len = u16::try_from(utf16.len()).unwrap_or(u16::MAX);
    let mut buf = Vec::with_capacity(3 + utf16.len() * 2);
    buf.push(0xFF);
    buf.extend_from_slice(&len.to_be_bytes());
    for unit in utf16 {
        buf.extend_from_slice(&unit.to_be_bytes());
    }

    client_data
        .stream()
        .await
        .get_stream()
        .write_all(&buf)
        .await
        .map_err(|e| PacketProcessingError::Custom(e.to_string()))
}

async fn handle_client(
    socket: TcpStream,
    addr: SocketAddr,
    server_state: Arc<RwLock<ServerState>>,
) {
    let client_data = ClientData::new(socket);
    let mut was_in_play_state = false;

    loop {
        match read(addr, &client_data, &server_state, &mut was_in_play_state).await {
            Ok(()) => {}
            Err(PacketProcessingError::LegacyPing) => {
                if let Err(e) = respond_legacy_ping(&client_data, &server_state).await {
                    debug!("Legacy ping response failed: {}", e);
                } else {
                    debug!("Answered legacy server list ping from {}", addr);
                }
                break;
            }
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

/// Poll upstream game server status and cache the JSON response.
/// Also flips the `alive` AtomicBool so the transfer decision (in process_packet)
/// can hold players on the limbo while the backend is unreachable and resume
/// them once it recovers. Poll interval comes from `[upstream].poll_secs` in
/// gate.toml (default 10).
fn poll_upstream_status_thread(
    addr: String,
    cache: std::sync::Arc<std::sync::RwLock<Option<String>>>,
    alive: std::sync::Arc<std::sync::atomic::AtomicBool>,
    poll_secs: u64,
    server_state: std::sync::Arc<tokio::sync::RwLock<ServerState>>,
) {
    use std::sync::atomic::Ordering;
    let poll_secs = poll_secs.max(1);

    eprintln!("[poller] Starting for {} (every {}s)", addr, poll_secs);
    let mut backend_was_up = alive.load(Ordering::Acquire);

    std::thread::sleep(std::time::Duration::from_secs(2));

    loop {
        match fetch_upstream_status_blocking(&addr) {
            Ok(json) => {
                // Backend is up — cache the full status and persist to disk
                let _ = std::fs::write("./upstream_status.json", &json);
                if let Ok(mut c) = cache.write() {
                    *c = Some(json);
                }
                // Flip the live flag so in-flight holds can resume their Transfer.
                alive.store(true, Ordering::Release);
                // Enable status replies (AtomicBool + try_write ServerState)
                if let Ok(mut s) = server_state.try_write() {
                    if !s.reply_to_status() {
                        s.set_reply_to_status_live(true);
                        eprintln!("[poller] First successful poll — status replies enabled");
                    }
                }
                if !backend_was_up {
                    eprintln!("[poller] Backend is back up — held players will be transferred");
                    backend_was_up = true;
                }
            }
            Err(e) => {
                eprintln!("[poller] fetch failed: {}", e);
                alive.store(false, Ordering::Release);
                if backend_was_up {
                    // Backend just went down — zero the player count in cache
                    if let Ok(mut c) = cache.write() {
                        if let Some(ref cached_json) = *c {
                            if let Ok(mut status) =
                                serde_json::from_str::<serde_json::Value>(cached_json)
                            {
                                if let Some(players) = status.get_mut("players") {
                                    players["online"] = serde_json::Value::Number(0.into());
                                }
                                *c = Some(
                                    serde_json::to_string(&status)
                                        .unwrap_or_else(|_| cached_json.clone()),
                                );
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

fn fetch_upstream_status_blocking(
    addr: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
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
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(e) => return Err(e.into()),
        }
    }
    extract_status_json(&buf[..total]).ok_or_else(|| "Incomplete status response".into())
}

fn extract_status_json(data: &[u8]) -> Option<String> {
    let mut i = 0;
    // Skip packet length varint
    while i < data.len() && data[i] & 0x80 != 0 {
        i += 1;
    }
    if i >= data.len() {
        return None;
    }
    i += 1;
    // Skip packet id varint
    while i < data.len() && data[i] & 0x80 != 0 {
        i += 1;
    }
    if i >= data.len() {
        return None;
    }
    i += 1;
    // Read string length varint
    let mut str_len: usize = 0;
    let mut shift = 0;
    while i < data.len() && data[i] & 0x80 != 0 {
        str_len |= ((data[i] & 0x7f) as usize) << shift;
        shift += 7;
        i += 1;
    }
    if i >= data.len() {
        return None;
    }
    str_len |= ((data[i] & 0x7f) as usize) << shift;
    i += 1;
    if i + str_len > data.len() {
        return None;
    }
    String::from_utf8(data[i..i + str_len].to_vec()).ok()
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

/// Write a transfer token to the game_tokens BPF map (pinned at /sys/fs/bpf/game_tokens).
/// The token is a random u32 that the game port XDP uses to validate incoming connections
/// from players whose IP changed between gate and game (e.g. game accelerators).
fn write_game_token(token: u32) -> std::io::Result<()> {
    use std::os::unix::io::RawFd;

    let path = std::ffi::CString::new("/sys/fs/bpf/game_tokens").unwrap();

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

    let key = token;
    let value: u64 = 1; // marker value

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

    let ret = unsafe {
        libc::syscall(
            libc::SYS_bpf,
            2i32, // BPF_MAP_UPDATE_ELEM
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

/// Generate a random u32 transfer token.
fn generate_token() -> u32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    // Mix PID, thread ID, and high-resolution time for a non-predictable token.
    // Not cryptographic, but sufficient — the token lives <10s and is one-time.
    let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let seed = t.as_nanos() as u64
        ^ (std::process::id() as u64) << 32
        ^ (t.subsec_nanos() as u64).wrapping_mul(0x9E3779B97F4A7C15);
    // xorshift mix
    let mut x = seed;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    (x.wrapping_mul(0x2545F4914F6CDD1D) >> 32) as u32
}
