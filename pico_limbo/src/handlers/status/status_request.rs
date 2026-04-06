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

        // Serve the cached upstream status (MOTD, players, icon) with two tweaks:
        //  1. version.name is replaced with the configured protocol range
        //     (e.g. "1.21.2") instead of the backend's native string.
        //  2. If the client's protocol is in range, echo it in version.protocol
        //     so their client shows green bars (ViaVersion trick).
        let packet = if let Some(cached_json) = server_state.cached_upstream_status() {
            let client_proto = client_state.protocol_version().version_number();
            if let Ok(mut status) = serde_json::from_str::<serde_json::Value>(&cached_json) {
                // Replace version.name with the configured range so the server
                // list shows e.g. "1.21.2" instead of Velocity's "1.7.2-1.21.4".
                status["version"]["name"] = serde_json::Value::from(
                    server_state.backend_version_name(),
                );
                if server_state.vg_in_range(client_proto) {
                    status["version"]["protocol"] = serde_json::Value::from(client_proto);
                }
                StatusResponsePacket::from_raw_json(
                    serde_json::to_string(&status).unwrap_or(cached_json),
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
