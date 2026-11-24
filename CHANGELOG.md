# Changelog

All notable changes to Koyomi are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.4.0] - 2026-11-24

### Added

- Added the GPL-3.0-only license.
- Added warnings to `koyomi status` and `koyomi list` when email alerts are disabled on the hub or on this host. `koyomi list` prints its warning to stderr, so `koyomi list --json` can still be piped.
- Added install steps for launchd on macOS and systemd on Linux to the README.

### Changed

- Changed email alerts to be optional. `koyomi init` no longer requires `--smtp-env`, `--email-from` and `--email-to`; give all three to turn email on. Without SMTP on the hub, Koyomi still records runs and logs and queues no alert email. Existing configs with SMTP work unchanged.
- Changed the agent skill to work with any set of hosts. It reads site-specific hosts, SSH destinations and deployment steps from `site-notes.md` in the Koyomi state directory (`${KOYOMI_HOME:-~/.koyomi}`) when present.
- Changed the hub protocol version to 0.4.0. Upgrade the hub first, then every host: until all machines run the same version, their hub calls fail with "version mismatch".

### Fixed

- Fixed disabled jobs counting as failing in `koyomi status`, `koyomi fix` and the dashboard header.
