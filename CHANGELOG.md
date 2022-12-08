# Changelog

This file summarizes notable changes to this project between versions.  The
changes between package releases are listed in the packaging files,
[nc-server.spec](nc_server.spec) and [debian/changelog](debian/changelog).

The format is based on [Keep a Changelog], the versions should follow
[semantic versioning].

## [2.0] - 2022-12-07

### Changed

- RPM packaging now uses the `build_rpm` script built into `eol_scons`.  This
  supports packaging from a specific clean release tag as well as from commit
  snapshots, and the commit number from `git describe` is no longer used as a
  package release number (`releasenum`), since the package release number is
  meant to distinguish different builds of the same source release.
- Make sure server and clients only link to the nidas util library, not all
  the nidas libraries.
- Clients recognize the `NC_SERVER_PORT` environment variable.
- Provide a more testing-friendly install: `nc_server.check` and a
  `logrotate.conf` are generated with the install directory built-in, to run
  `nc_server` with more verbose logging and unlimited core files.
- Removed dynamic exception specifiers from server functions.
- All repo files now included in the RPM source archive.
- Converted README to markdown.

## [1.4] - 2022-09-28

- fix a bug where a standalone nc_server instance would call `pmap_unset()`
  and unregister an existing server instance already bound to the portmapper.

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
[2.0]: https://github.com/ncareol/nc-server/compare/v1.4...v2.0
[1.4]: https://github.com/ncareol/nc-server/compare/v1.3...v1.4
[1.3]: https://github.com/ncareol/nc-server/compare/v1.2...v1.3
[1.2]: https://github.com/ncareol/nc-server/compare/v1.1...v1.2
[1.1]: https://github.com/ncareol/nc-server/releases/tag/v1.1
