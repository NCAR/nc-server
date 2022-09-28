# Changelog

This file summarizes notable changes to this project between versions.  The
changes between package releases are listed in the packaging files,
[nc-server.spec](nc_server.spec) and [debian/changelog](debian/changelog).

The format is based on [Keep a Changelog], the versions should follow
[semantic versioning].

## Unreleased

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
[1.4]: https://github.com/ncareol/nc-server/compare/v1.3...v1.4
[1.3]: https://github.com/ncareol/nc-server/compare/v1.2...v1.3
[1.2]: https://github.com/ncareol/nc-server/compare/v1.1...v1.2
[1.1]: https://github.com/ncareol/nc-server/releases/tag/v1.1
