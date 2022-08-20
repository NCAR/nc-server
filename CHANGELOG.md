# Changelog

This file summarizes notable changes to this project between versions.  The
changes between package releases are listed in the packaging files,
[nc-server.spec](nc_server.spec) and [debian/changelog](debian/changelog).

The format is based on [Keep a Changelog].

## [2.0] - Unreleased

### Changed

- Removed dynamic exception specifiers from server functions.
- All repo files now included in the RPM source archive.
- Converted README to markdown.

## [1.3] - 2022-08-10

- fix wrong string constructor referencing uninitialized memory, caught by
  valgrind
- build on platforms with libtirpc

## [1.2] - 2017-12-31

- standalone mode
- systemd support
- Debian packaging, especially multiarch support

## [1.1] - 2016-04-25

- initial version

<!-- Links -->
[keep a changelog]: https://keepachangelog.com/en/1.0.0/
[semantic versioning]: https://semver.org/spec/v2.0.0.html

<!-- Versions -->
[2.0]: https://github.com/ncareol/nc-server/compare/v1.3...v2.0
[1.3]: https://github.com/ncareol/nc-server/compare/v1.2...v1.3
[1.2]: https://github.com/ncareol/nc-server/compare/v1.1...v1.2
[1.1]: https://github.com/ncareol/nc-server/releases/tag/v1.1
