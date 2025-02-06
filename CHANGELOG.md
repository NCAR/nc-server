# Changelog

This file summarizes notable changes to this project between versions.  The
changes between package releases are listed in the packaging files,
[nc-server.spec](nc_server.spec) and [debian/changelog](debian/changelog).

The format is based on [Keep a Changelog], the versions should follow
[semantic versioning].

## [Unreleased] - Unreleased

- `nc_server` still uses the legacy netCDF C++ interface, and that library is
  not always current or even available as a system package.  So if the library
  is found under `PREFIX`, then `nc_server` automatically builds against that
  installation instead of expecting a system installation.  For easy
  reference, the legacy source is available on this [Unidata downloads
  page](https://downloads.unidata.ucar.edu/netcdf/):

  - [netcdf-cxx-4.2.tar.gz](https://downloads.unidata.ucar.edu/netcdf-cxx/4.2/netcdf-cxx-4.2.tar.gz)

## [2.2] - 2025-02-03

- This release requires at least version 1.2.5 of NIDAS due to changes to the
  `SampleTag::getVariables()` API.  Also, NIDAS 1.2.5 added new syntax to
  netCDF attribute values to support different attribute types, and
  `nc-server` 2.2 is required to support that syntax.  Earlier versions of
  `nc-server` will not build against NIDAS versions before 1.2.5 because of
  the removal of the `svnStatus()` function.

- Given the binary compatibility requirements with NIDAS shared libraries,
  `nc_server` now defaults to installing into the NIDAS directory against
  which it is built, for installs from source and also RPM packages.  (Debian
  packages have not changed.)  If `PREFIX` is not set, then it defaults to the
  prefix variable from the NIDAS `pkg-config` file, and otherwise
  `/opt/nidas`.  Libraries are installed into `PREFIX/lib` on all platforms;
  same as for NIDAS, there is no longer an architecture-specific library path.
  Besides simplifying the layout and installation, nc-server can be installed
  from source into an existing NIDAS install by specifying the NIDAS prefix,
  as below.  The `PREFIX` is inserted automatically to the front of
  `PKG_CONFIG_PATH`.

  ```sh
  scons PREFIX=/opt/local/nidas install
  ```

  Using `setup_nidas.sh` to add that NIDAS installation to the environment
  automatically includes a binary-compatible nc-server installation, especially
  critical for the NIDAS plugins `NetcdfRPCChannel` and `NetcdfRPCOutput`.

- NetCDF output files can be split within a time interval by passing the time
  period in environment variables `ISFS_CONFIG_BEGIN` and `ISFS_CONFIG_END`.
  The `NetcdfRPCChannel` detects these variables and passes them to the
  `nc_server` as special global attributes `isfs_config_begin` and
  `isfs_config_end`.  Those settings are used to clamp the begin and end time
  of a file whose samples fall after or before those times and within the same
  file interval (eg, hour).  This allows changes within the same time interval
  which must necessarily be in different netcdf files, such as because of
  attributes changes (sensor metadata) or dimension changes (sensor rates).

- Similarly, project configuration version information can be set in the
  environment variable `ISFS_CONFIG_VERSION`.  The value of that variable is
  passed to the server as global attribute `project_version`.

- By default, netcdf output files write the global attribute `project_config`
  using `getConfigName()` on the `Project` instance, where that name is
  typically the XML file path.  However, that path alone is insufficient to
  identify the project config.  The `ISFS_CONFIG_SPECIFIER` environment
  variable can be set to override the default config name with a more complete
  config specifier, and the value of that variable will be written to the
  `project_config` global attribute.

- Add an extra check against null pointer returns for attribute values.  The
  netCDF-C++ `NcAtt::as_string()` implementation does not check for null
  returns from `nc_values()`, which can happen if there is an error accessing
  the file, as indicated by occasional core dumps in production.

## [2.1] - 2022-02-10

### Changed

- The NIDAS NetcdfRPCChannel and NetcdfRPCOutput classes, being nc_server
  clients themselves, are now compiled and installed in this repo alongside
  the nc_server client library.  NIDAS programs which need those modules load
  them dynamically at runtime, so NIDAS itself does not have to be built
  against the nc_server client library.

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
[Unreleased]: https://github.com/ncareol/nc-server/
[2.2]: https://github.com/ncareol/nc-server/compare/v2.1...v2.2
[2.1]: https://github.com/ncareol/nc-server/compare/v2.0...v2.1
[2.0]: https://github.com/ncareol/nc-server/compare/v1.4...v2.0
[1.4]: https://github.com/ncareol/nc-server/compare/v1.3...v1.4
[1.3]: https://github.com/ncareol/nc-server/compare/v1.2...v1.3
[1.2]: https://github.com/ncareol/nc-server/compare/v1.1...v1.2
[1.1]: https://github.com/ncareol/nc-server/releases/tag/v1.1
