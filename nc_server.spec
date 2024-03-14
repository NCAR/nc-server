Name: nc_server
Version: 2.1
Release: %{releasenum}%{?dist}
Summary: Server for NetCDF file writing.
License: GPL
Group: Applications/Engineering
URL: https://github.com/ncareol/nc-server
# Allow this package to be relocatable to other places than /opt/nc_server
# rpm --relocate /opt/nc_server=/usr
Prefix: /opt/nc_server

# Because we know the shared library version numbers are derived from the
# source version, just replicate that here.  Technically the shared library
# version could/should be independent, in which case we would need a way to
# "ask" the source for the current shared library versions, to know the names
# of the shared library files and links that will be installed.  Or else the
# versions could be hardcoded here to match the source release.
%define version_major %(echo v%{version} | sed -E 's/^[vV]([0-9]+)\.([0-9]+)([.-~].*)?$/\\1/')
%define version_minor %(echo v%{version} | sed -E 's/^[vV]([0-9]+)\.([0-9]+)([.-~].*)?$/\\2/')

# These are also prerequisites for the build, but I don't know if they
# belong in the BuildRequires:

#   gcc-g++ rpm-build systemd-rpm-macros

# The systemd-rpm-macros package installs
# /usr/lib/rpm/macros.d/macros.systemd, and that defines _unitdir macro.
# The rpm build fails if _unitdir macro is not defined.

BuildRequires: netcdf-cxx-devel netcdf-devel
BuildRequires: libcap-devel nidas-devel eol_scons
BuildRequires: libtirpc-devel rpcgen
%{?systemd_requires}
Vendor: UCAR
Source: %{name}-%{version}.tar.gz

%description
Server for NetCDF file writing.

%package lib
Summary: nc_server library
Group: Applications/Engineering
%description lib
libnc_server_rpc.so library

%package devel
Summary: nc_server library and header file
Group: Applications/Engineering
%description devel
Header file and symbolic links to library

%package clients
Summary: Some client programs of nc_server
Group: Applications/Engineering
%description clients
Some client programs of nc_server

%prep
%setup

%build
pwd
scons gitinfo=off PREFIX=%{prefix} REPO_TAG=v%{version}

%install
rm -rf $RPM_BUILD_ROOT
scons gitinfo=off INSTALL_PREFIX=%{buildroot}/ PREFIX=%{prefix} \
    REPO_TAG=v%{version} SYSCONFIGDIR=%{_sysconfdir} UNITDIR=%{_unitdir} \
    PKGCONFIGDIR=%{_libdir}/pkgconfig \
    install install.root

%post
%systemd_post nc_server.service
exit 0

%preun
%systemd_preun nc_server.service
exit 0

%postun
%systemd_postun_with_restart nc_server.service
exit 0

%post -n nc_server-devel
ldconfig
exit 0

%files
%caps(cap_net_bind_service,cap_setgid+p) %{prefix}/bin/nc_server
%{prefix}/bin/nc_shutdown
%{prefix}/bin/nc_server.check
%{prefix}/bin/nc_check
%config(noreplace) %{_sysconfdir}/default/nc_server
/opt/nc_server/etc/systemd/user
%{_unitdir}/nc_server.service

%files lib
%config %{_sysconfdir}/ld.so.conf.d/nc_server.conf
%{prefix}/%{_lib}/libnc_server_rpc.so.%{version_major}.%{version_minor}
# the version for these modules is tied to nidas, which is currently hardcoded
# to expect .1.  Technically they are only used at runtime and not meant to be
# linked against, so the version-less link is included here instead of devel
%{prefix}/%{_lib}/libnidas_dynld_isff_NetcdfRPCChannel.so
%{prefix}/%{_lib}/libnidas_dynld_isff_NetcdfRPCChannel.so.1
%{prefix}/%{_lib}/libnidas_dynld_isff_NetcdfRPCOutput.so
%{prefix}/%{_lib}/libnidas_dynld_isff_NetcdfRPCOutput.so.1

%files devel
%{prefix}/include/nc_server_rpc.h
%{prefix}/%{_lib}/libnc_server_rpc.so.%{version_major}
%{prefix}/%{_lib}/libnc_server_rpc.so
%{prefix}/%{_lib}/pkgconfig/nc_server.pc
%config %{_libdir}/pkgconfig/nc_server.pc

%files clients
%{prefix}/bin/nc_ping
%{prefix}/bin/nc_close
%{prefix}/bin/nc_sync
%config(noreplace) %{_sysconfdir}/profile.d/nc_server.sh
%config(noreplace) %{_sysconfdir}/profile.d/nc_server.csh

%changelog
* Fri Feb 10 2023 Gary Granger <granger@ucar.edu> - 2.1
- update package release to source release v2.1

* Thu Dec 08 2022 Gary Granger <granger@ucar.edu> - 2.0
- build v2.0

* Tue Dec 06 2022 Gary Granger <granger@ucar.edu> - 2.0~alpha3
- build v2.0-alpha3

* Wed Sep 28 2022 Gary Granger <granger@ucar.edu> - 1.4-1
- package version 1.4

* Wed Aug 10 2022 Gary Granger <granger@ucar.edu> - 1.3-1
- package version 1.3

* Thu Aug 04 2022 Gary Granger <granger@ucar.edu> - 1.2.1-1
- nc_server v1.2.1
