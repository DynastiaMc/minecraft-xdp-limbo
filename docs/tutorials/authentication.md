# Using PicoLimbo as an Authentication Server

This page is still work in progress and needs to be written/improved.

## LibreLogin / LibreLoginProd

LibreLogin ([kyngs/LibreLogin](https://github.com/kyngs/LibreLogin)) is no longer actively maintained. LibreLoginProd ([Navio1430/LibreLoginProd](https://github.com/Navio1430/LibreLoginProd)) is a community fork of LibreLogin. However, the developer has since moved on to create [NavAuth](https://github.com/Navio1430/NavAuth) from scratch.

For this tutorial, we'll be using Velocity and LibreLoginProd.

### Server Setup

Assume we have the following servers:
- Velocity proxy on port 25565
- Paper server on port 30066
- PicoLimbo on port 30067

### Configuration

Configure your `config.conf` from LibreLoginProd and `velocity.toml` file as follows:

:::code-group

```text [config.conf] {3,8}
# ... rest of the configuration omitted
limbo=[
    limbo
]

lobby {
    root=[
        survival
    ]
}
# ... rest of the configuration omitted
```

```toml [velocity.toml] {3}
# Usually, if you use an authentication plugin,
# you'd set online mode to false
online-mode = false

[servers]
survival = "127.0.0.1:30066"
limbo = "127.0.0.1:30067"

try = ["survival", "limbo"]
```

:::

## Other Authentication Plugin Recommendations

### NavAuth

NavAuth is available at [Navio1430/NavAuth](https://github.com/Navio1430/NavAuth).

### LimboAuth

LimboAuth can be found at [Elytrium/LimboAuth](https://github.com/Elytrium/LimboAuth). It has comparable popularity to the original LibreLogin plugin.

### AuthMe / AuthMeReloaded

AuthMe / AuthMeReloaded is probably the most popular and oldest authentication plugin available. You can find it at [AuthMe/AuthMeReloaded](https://github.com/AuthMe/AuthMeReloaded). It may however not be compatible with limbo servers (I haven't tested it yet.)
