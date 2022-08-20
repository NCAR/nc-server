# nc_server - Server for NetCDF file writing

## Release notes

See [CHANGELOG.md](CHANGELOG.md) for changes.

## Troubleshooting

If `nc_ping` reports:

```sh
rpcinfo: RPC: Port mapper failure - Unable to receive: errno 113 (No route to host)
```

You probably need to open ports in the firewall on the host running `nc_server`.

Port 111 is used by rpcbind, which RPC clients query to find out what port to
contact for a specific RPM service.

`nc_server` by default, provides its service on port 30005.

If `firewalld.service` is running on the server host:

```sh
firewall-cmd  --add-port=111/udp 
firewall-cmd --permanent --add-port=111/udp

firewall-cmd  --add-port=111/tcp
firewall-cmd --permanent --add-port=111/tcp
```

If necessary:

```sh
firewall-cmd  --add-port=30005/udp --add-port=30005/tcp
firewall-cmd --permanent --add-port=30005/udp --add-port=30005/tcp
```

## Building from source

The main repository for **nc-server** is hosted at github:

- [https://github.com/ncareol/nc-server](https://github.com/ncareol/nc-server)

The build uses [SCons](https://www.scons.org/) and
[eol_scons](https://github.com/NCAR/eol_scons), and the source depends on
[NIDAS](https://github.com/ncareol/nidas).  NIDAS can be installed from source
or with the `nidas-devel` package.

Run `scons -h` to see available build variables.  There are two install aliases:

- `install`: Install all files under the `PREFIX` directory, `/opt/nc_server`
  by default.  Use this to install to a non-root directory for use by a single
  user or for testing.
- `install.root`: Install files for a system installation, including a systemd
  system unit, shell resource files for profile.d, and a ld.so config file.
  The destinations for the system files are controlled by variables
  `SYSCONFIGDIR`, `UNITDIR`, and `PKGCONFIGDIR`.

## Creating a source release

Update the [CHANGELOG.md](CHANGELOG.md) file with noteworthy changes since the
last source release.

Create an annotated tag for the repo, either in a clone or through the project
site on github.  This tags the latest commit, so make sure all changes in the
current repo intended for this version have been committed.

```sh
git tag -a -m 'tag NEXT.VERSION' v<NEXT>.<VERSION>
```

If necessary, push that tag to github and create a release from it.

## Creating a package release

When the RPM package should build a new source release, update the file
[nc_server.spec](nc_server.spec) with the source version.  Since the major and
minor versions of the shared library are tied to the source version, there are
macros to set the major and minor versions at the top of the spec file.  If
there is a prerelease label, define it in `version_alpha`, otherwise make sure
`version_alpha` is undefined.

Add a changelog entry to the spec file for the new version being packaged.
The `rpmdev-bumpspec` tool can be used for this, in the `rpmdevtools` package:

```sh
rpmdev-bumpspec -c 'update to v2.0' nc_server.spec
```

That adds a changelog entry in the right format, however the modifications to
the release number on the `Release:` line have to be removed.

Package releases can follow a different schedule than source releases.  The
packaging and the package builds can change even though the source release
being built does not change.  Likewise, the package release does not need to
change just because a new source release has been created.  The source release
is specified by the `Version` file in the spec file, while the package release
number tracks changes to the package build.  The `build_rpm.sh` ensures that
only clean tagged source releases are ever packaged, and it tries to bump the
release number automatically according to the last release number available in
the package repository.

It is possible to create prelease source tags in a form like `v2.0-beta`.  If
those releases are going to be packaged, such as for testing the packaging,
then the spec file specifies the source version as `2.0~beta`.  The
`build_rpm.sh` script translates this package version to source tag
`v2.0-beta`.  The `~` ensures that package `v2.0` will be considered a newer
release than `v2.0~beta`.  The `rpmdev-vercmp` tool can help verify package
release ordering.
