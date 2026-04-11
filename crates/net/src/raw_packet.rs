use minecraft_protocol::prelude::{
    BinaryReader, BinaryWriter, BinaryWriterError, EncodePacket, Identifiable, ProtocolVersion,
    VarInt,
};
use std::fmt::Display;
use thiserror::Error;

/// Wire-format raw packet (uncompressed body), stored as `[VarInt packet_id][payload]`.
///
/// The packet_id is intentionally a full VarInt — Minecraft 26.1 (protocol 775)
/// pushed several play-state clientbound packets past id 127 (e.g. `transfer`
/// became 129), so a single-byte packet_id is no longer correct on the wire.
#[derive(Debug, Default)]
pub struct RawPacket {
    /// Encoded as `[VarInt packet_id][payload]`.
    data: Vec<u8>,
}

#[derive(Error, Debug)]
pub enum RawPacketError {
    #[error("invalid packet length")]
    InvalidPacketLength,
    #[error("failed to encode packet {id} for version {version}")]
    EncodePacket { id: i32, version: i32 },
}

impl RawPacket {
    /// Wraps a raw body that already starts with a `VarInt` packet id followed
    /// by the payload (this is the on-the-wire layout after stripping the
    /// outer length prefix). The body must not be empty and must start with a
    /// parseable VarInt, otherwise `InvalidPacketLength` is returned.
    pub fn new(data: Vec<u8>) -> Result<Self, RawPacketError> {
        if data.is_empty() {
            return Err(RawPacketError::InvalidPacketLength);
        }
        // Validate that the body starts with a parseable packet_id VarInt.
        let mut reader = BinaryReader::new(&data);
        reader
            .read::<VarInt>()
            .map_err(|_| RawPacketError::InvalidPacketLength)?;
        Ok(Self { data })
    }

    /// Builds a raw packet from a packet_id and an already-encoded payload,
    /// VarInt-encoding the packet_id at the front of the body.
    pub fn from_bytes(packet_id: i32, bytes: &[u8]) -> Self {
        let mut data = VarInt::new(packet_id).to_bytes().unwrap_or_default();
        data.extend_from_slice(bytes);
        Self { data }
    }

    /// Creates a new raw packet from a serializable packet struct.
    pub fn from_packet<T>(
        packet_id: i32,
        version_number: i32,
        packet: &T,
    ) -> Result<Self, BinaryWriterError>
    where
        T: EncodePacket + Identifiable,
    {
        let mut writer = BinaryWriter::new();
        writer.write(&VarInt::new(packet_id))?;
        packet.encode(&mut writer, ProtocolVersion::from(version_number))?;

        let data = writer.into_inner();
        Ok(Self { data })
    }

    pub const fn size(&self) -> usize {
        self.data.len()
    }

    /// Returns the decoded packet id, or `None` if the body does not start
    /// with a valid VarInt.
    pub fn packet_id(&self) -> Option<i32> {
        let mut reader = BinaryReader::new(&self.data);
        reader.read::<VarInt>().ok().map(|v| v.inner())
    }

    /// Returns the payload (everything after the VarInt packet id).
    pub fn data(&self) -> &[u8] {
        let n = self.packet_id_size();
        if n >= self.data.len() {
            &[]
        } else {
            &self.data[n..]
        }
    }

    /// Returns the full encoded body, ready to be written after a length
    /// prefix.
    pub fn bytes(&self) -> &[u8] {
        &self.data
    }

    /// Number of bytes consumed by the leading packet_id VarInt.
    fn packet_id_size(&self) -> usize {
        let mut n = 0;
        for &b in &self.data {
            n += 1;
            if b & 0x80 == 0 || n >= 5 {
                break;
            }
        }
        n
    }
}

impl Display for RawPacket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for byte in self.data() {
            write!(f, "{byte:02X} ")?;
        }
        Ok(())
    }
}
