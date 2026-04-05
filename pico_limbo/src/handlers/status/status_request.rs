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

        // Serve the cached upstream status verbatim (MOTD, players, icon,
        // version.name, version.protocol — everything from the backend). The backend
        // is the single source of truth for what version to advertise.
        //
        // Only tweak: if the client's protocol is within the accepted range, echo
        // their protocol in version.protocol so their Minecraft client shows green
        // bars (the standard ViaVersion trick). Clients outside the range see the
        // backend's protocol unchanged → natural red X with the backend version as
        // the label.
        let packet = if let Some(cached_json) = server_state.cached_upstream_status() {
            let client_proto = client_state.protocol_version().version_number();
            if let Ok(mut status) = serde_json::from_str::<serde_json::Value>(&cached_json) {
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
