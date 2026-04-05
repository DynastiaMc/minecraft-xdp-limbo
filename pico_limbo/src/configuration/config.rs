use crate::configuration::boss_bar::BossBarConfig;
use crate::configuration::commands::CommandsConfig;
use crate::configuration::compression::CompressionConfig;
use crate::configuration::env_placeholders::{EnvPlaceholderError, expand_env_placeholders};
use crate::configuration::forwarding::ForwardingConfig;
use crate::configuration::game_mode_config::GameModeConfig;
use crate::configuration::server_list::ServerListConfig;
use crate::configuration::tab_list::TabListConfig;
use crate::configuration::title::TitleConfig;
use crate::configuration::world_config::WorldConfig;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::{fs, io};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("TOML serialization error: {0}")]
    TomlSerialize(#[from] toml::ser::Error),

    #[error("TOML deserialization error: {0}")]
    TomlDeserialize(#[from] toml::de::Error),

    #[error("Failed to apply environment placeholders: {0}")]
    EnvPlaceholder(#[from] EnvPlaceholderError),
}

/// Upstream game server configuration for status proxy and auto-transfer.
#[derive(Serialize, Deserialize)]
#[serde(default)]
pub struct UpstreamConfig {
    /// Address to poll for status (MOTD, players, icon). Empty = disabled.
    pub status_addr: String,
    /// Poll interval in seconds.
    pub poll_secs: u64,
    /// Hostname sent in the Transfer packet to the player.
    pub transfer_host: String,
    /// Port sent in the Transfer packet. 0 = no transfer.
    pub transfer_port: i32,
    /// Health check retry interval in seconds (when game server is down).
    pub retry_secs: u64,
    /// Spacing in milliseconds between deferred transfers when backend
    /// recovers. Prevents a thundering herd: N held players are drained at
    /// most `1000 / transfer_rate_slot_ms` per second. 0 disables the stagger
    /// (all held players transfer on their next retry tick).
    pub transfer_rate_slot_ms: u64,
}

impl Default for UpstreamConfig {
    fn default() -> Self {
        Self {
            status_addr: String::new(),
            poll_secs: 10,
            transfer_host: String::new(),
            transfer_port: 0,
            retry_secs: 5,
            transfer_rate_slot_ms: 200,
        }
    }
}

/// Version gate: only players within the accepted protocol range can join.
///
/// Display info (version name/protocol shown in server list and kick messages)
/// is never stored here — it comes from the cached upstream backend status, so
/// the limbo always advertises the backend's "recommended version" to clients.
///
/// Acceptance bounds are clamped at runtime:
///   - hard floor 766 (1.20.5, first version with Transfer packet)
///   - hard ceiling `ProtocolVersion::latest()` (what PicoLimbo can actually parse)
#[derive(Serialize, Deserialize)]
#[serde(default)]
pub struct VersionGateConfig {
    /// Minimum accepted protocol (inclusive). Clamped to >= 766 at runtime.
    pub min_protocol: i32,
    /// Maximum accepted protocol (inclusive). Clamped to <= ProtocolVersion::latest() at runtime.
    pub max_protocol: i32,
    /// Kick message sent when a client is outside the accepted range.
    /// `<version>` is replaced with the cached backend version name.
    pub kick_message: String,
}

impl Default for VersionGateConfig {
    fn default() -> Self {
        Self {
            min_protocol: 767, // 1.21.0
            max_protocol: 775, // 26.1
            kick_message: "This server accepts Minecraft <version> only.".into(),
        }
    }
}

/// Application configuration, serializable to/from TOML.
#[derive(Serialize, Deserialize)]
#[serde(default)]
#[serde(deny_unknown_fields)]
#[allow(clippy::struct_excessive_bools)]
pub struct Config {
    /// Server listening address and port.
    ///
    /// Specify the IP address and port the server should bind to.
    /// Use 0.0.0.0 to listen on all network interfaces.
    pub bind: String,

    pub forwarding: ForwardingConfig,

    pub world: WorldConfig,

    pub server_list: ServerListConfig,

    /// Message sent to the player after spawning in the world.
    pub welcome_message: String,

    pub action_bar: String,

    /// Sets the default game mode for players
    /// Valid values are: "survival", "creative", "adventure" or "spectator"
    pub default_game_mode: GameModeConfig,

    /// If set to true, will spawn the player in hardcode mode
    pub hardcore: bool,

    pub compression: CompressionConfig,

    pub tab_list: TabListConfig,

    pub fetch_player_skins: bool,

    pub reduced_debug_info: bool,

    pub allow_unsupported_versions: bool,

    pub allow_flight: bool,

    pub accept_transfers: bool,

    pub boss_bar: BossBarConfig,

    pub title: TitleConfig,

    pub commands: CommandsConfig,

    pub version_gate: VersionGateConfig,

    pub upstream: UpstreamConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:25565".into(),
            server_list: ServerListConfig::default(),
            welcome_message: "Welcome to PicoLimbo!".into(),
            action_bar: "Welcome to PicoLimbo!".into(),
            forwarding: ForwardingConfig::default(),
            default_game_mode: GameModeConfig::default(),
            world: WorldConfig::default(),
            hardcore: false,
            tab_list: TabListConfig::default(),
            fetch_player_skins: false,
            reduced_debug_info: false,
            boss_bar: BossBarConfig::default(),
            compression: CompressionConfig::default(),
            title: TitleConfig::default(),
            allow_unsupported_versions: false,
            allow_flight: false,
            accept_transfers: false,
            commands: CommandsConfig::default(),
            version_gate: VersionGateConfig::default(),
            upstream: UpstreamConfig::default(),
        }
    }
}

/// Loads a `Config` from the given path.
/// If the file does not exist, it will be created (parent dirs too)
/// and populated with default values.
pub fn load_or_create<P: AsRef<Path>>(path: P) -> Result<Config, ConfigError> {
    let path = path.as_ref();

    if path.exists() {
        let raw_toml_str = fs::read_to_string(path)?;

        if raw_toml_str.trim().is_empty() {
            create_default_config(path)
        } else {
            let expanded_toml_str = expand_env_placeholders(&raw_toml_str)?;
            let cfg: Config = toml::from_str(expanded_toml_str.as_ref())?;
            Ok(cfg)
        }
    } else {
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir)?;
        }
        create_default_config(path)
    }
}

fn create_default_config<P: AsRef<Path>>(path: P) -> Result<Config, ConfigError> {
    let cfg = Config::default();
    let toml_str = toml::to_string_pretty(&cfg)?;
    fs::write(path, toml_str)?;
    Ok(cfg)
}
