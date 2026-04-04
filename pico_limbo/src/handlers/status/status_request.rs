use crate::server::batch::Batch;
use crate::server::client_state::ClientState;
use crate::server::packet_handler::{PacketHandler, PacketHandlerError};
use crate::server::packet_registry::PacketRegistry;
use crate::server_state::ServerState;
use minecraft_packets::status::data::status_response::StatusResponse;
use minecraft_packets::status::status_request_packet::StatusRequestPacket;
use minecraft_packets::status::status_response_packet::StatusResponsePacket;
use minecraft_protocol::prelude::ProtocolVersion;

impl PacketHandler for StatusRequestPacket {
    fn handle(
        &self,
        client_state: &mut ClientState,
        server_state: &ServerState,
    ) -> Result<Batch<PacketRegistry>, PacketHandlerError> {
        let mut batch = Batch::new();

        // [Dynastia] Use cached upstream status if available.
        // Override version with the limbo's version gate so incompatible clients
        // see a red X in the server list.
        let packet = if let Some(cached_json) = server_state.cached_upstream_status() {
            let effective_floor = std::cmp::max(766, server_state.version_gate_protocol());
            let version_name = server_state.version_gate_version_name();
            if let Ok(mut status) = serde_json::from_str::<serde_json::Value>(&cached_json) {
                status["version"] = serde_json::json!({
                    "name": format!("{}+", version_name),
                    "protocol": effective_floor
                });
                StatusResponsePacket::from_raw_json(
                    serde_json::to_string(&status).unwrap_or(cached_json)
                )
            } else {
                StatusResponsePacket::from_raw_json(cached_json)
            }
        } else {
            // No upstream cache — use PicoLimbo's own status (fallback)
            let client_protocol_version = client_state.protocol_version();
            let (version_string, version_number) =
                if client_protocol_version.is_any() || client_protocol_version.is_unsupported() {
                    let oldest = ProtocolVersion::oldest().humanize();
                    let latest = ProtocolVersion::latest().humanize();
                    let version_string = format!("PicoLimbo {oldest}-{latest}");
                    (version_string, -1)
                } else {
                    (
                        client_protocol_version.humanize().to_string(),
                        client_protocol_version.version_number(),
                    )
                };

            let status_response = StatusResponse::new(
                version_string,
                version_number,
                server_state.motd(),
                server_state.online_players(),
                server_state.max_players(),
                server_state.fav_icon(),
            );
            StatusResponsePacket::from_status_response(&status_response)
        };

        batch.queue(|| PacketRegistry::StatusResponse(packet));
        Ok(batch)
    }
}
