# Troubleshooting and Common Issues

## Network Protocol Error when using ViaVersion

When ViaVersion is installed on the proxy, you may encounter "Network Protocol Error" when logging in PicoLimbo.
In the Velocity section of your ViaVersion's `config.yml`, ensure the protocol version number is set to `-1`, at least for the limbo server:
```yaml
velocity-servers:
  default: 775
  limbo: -1
```
