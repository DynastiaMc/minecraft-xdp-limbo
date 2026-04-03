use crate::server::batch::Batch;
use crate::server::client_state::ClientState;
use crate::server::packet_registry::PacketRegistry;
use crate::server_state::ServerState;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PacketHandlerError {
    #[error("An error occurred while handling a packet: {0}")]
    Custom(String),
    #[error("{0}")]
    InvalidState(String, bool),
}

impl PacketHandlerError {
    #[inline]
    pub fn custom(message: impl ToString) -> Self {
        Self::Custom(message.to_string())
    }

    #[inline]
    pub fn invalid_state(message: impl ToString) -> Self {
        Self::InvalidState(message.to_string(), true)
    }

    #[inline]
    pub fn disconnect(message: impl ToString) -> Self {
        Self::InvalidState(message.to_string(), false)
    }
}

pub trait PacketHandler {
    fn handle(
        &self,
        client_state: &mut ClientState,
        server_state: &ServerState,
    ) -> Result<Batch<PacketRegistry>, PacketHandlerError>;
}

impl From<pico_registries::Error> for PacketHandlerError {
    fn from(error: pico_registries::Error) -> Self {
        Self::Custom(error.to_string())
    }
}

impl From<pico_nbt::Error> for PacketHandlerError {
    fn from(error: pico_nbt::Error) -> Self {
        Self::Custom(error.to_string())
    }
}
