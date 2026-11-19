# Changelog

## 0.4.0

- `koyomi init` can set up a machine without SMTP. `koyomi status` and `koyomi list` flag disabled email alerts. Previously configured SMTP continues to work.
- The README covers launchd on macOS and systemd on Linux. The agent skill can read private site notes for host and deployment details.
- Koyomi is licensed under GPL-3.0-only.

Upgrade the hub first, then every host. All machines must run the same version.
