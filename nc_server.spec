Summary: Server for NetCDF file writing.
Name: nc_server
Version: 1.3
Release: %{releasenum}%{?dist}
License: GPL
Group: Applications/Engineering
Url: http://www.eol.ucar.edu/
Packager: Gordon Maclean <maclean@ucar.edu>
# Allow this package to be relocatable to other places than /opt/nc_server
# rpm --relocate /opt/nc_server=/usr
Prefix: /opt/nc_server

# These are also prerequisites for the build, but I don't know if they
# belong in the BuildRequires:

#   gcc-g++ rpm-build systemd-rpm-macros

# The systemd-rpm-macros package installs
# /usr/lib/rpm/macros.d/macros.systemd, and that defines _unitdir macro.
# The rpm build fails if _unitdir macro is not defined.

BuildRequires: netcdf-cxx-devel netcdf-devel
BuildRequires: libcap-devel nidas-devel eol_scons
%if 0%{?fedora} > 28
BuildRequires: libtirpc-devel rpcgen
%endif
%{?systemd_requires}
Vendor: UCAR
Source: %{name}-%{version}.tar.gz
Requires: nc_server-clients
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
%setup -n nc-server

%build
pwd
scons gitinfo=off PREFIX=/opt/nc_server REPO_TAG=v%{version}

%install
rm -rf $RPM_BUILD_ROOT
scons gitinfo=off PREFIX=${RPM_BUILD_ROOT}/opt/nc_server REPO_TAG=v%{version} install

install -d $RPM_BUILD_ROOT%{_sysconfdir}
cp -r etc/{ld.so.conf.d,profile.d,default} $RPM_BUILD_ROOT%{_sysconfdir}
sed -i -e 's,/lib,/%{_lib},' $RPM_BUILD_ROOT%{_sysconfdir}/ld.so.conf.d/nc_server.conf

install -d $RPM_BUILD_ROOT%{_libdir}/pkgconfig
#  scons puts entire $RPM_BUILD_ROOT in nc_server.pc, remove it.
sed -i -e "s,$RPM_BUILD_ROOT,," \
    $RPM_BUILD_ROOT/opt/nc_server/%{_lib}/pkgconfig/nc_server.pc

cp $RPM_BUILD_ROOT/opt/nc_server/%{_lib}/pkgconfig/nc_server.pc \
        $RPM_BUILD_ROOT%{_libdir}/pkgconfig

install -d $RPM_BUILD_ROOT/opt/nc_server/systemd
cp -r systemd/user $RPM_BUILD_ROOT/opt/nc_server/systemd

install -d $RPM_BUILD_ROOT%{_unitdir}
cp systemd/system/nc_server.service $RPM_BUILD_ROOT%{_unitdir}

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

%clean
rm -rf $RPM_BUILD_ROOT

%files
%caps(cap_net_bind_service,cap_setgid+p) /opt/nc_server/bin/nc_server
/opt/nc_server/bin/nc_shutdown
/opt/nc_server/bin/nc_server.check
/opt/nc_server/bin/nc_check
%config(noreplace) %{_sysconfdir}/default/nc_server
/opt/nc_server/systemd/user
%{_unitdir}/nc_server.service

%files lib
%config %{_sysconfdir}/ld.so.conf.d/nc_server.conf
/opt/nc_server/%{_lib}/libnc_server_rpc.so.1.3

%files devel
/opt/nc_server/include/nc_server_rpc.h
/opt/nc_server/%{_lib}/libnc_server_rpc.so.1
/opt/nc_server/%{_lib}/libnc_server_rpc.so
/opt/nc_server/%{_lib}/pkgconfig/nc_server.pc
%config %{_libdir}/pkgconfig/nc_server.pc

%files clients
/opt/nc_server/bin/nc_ping
/opt/nc_server/bin/nc_close
/opt/nc_server/bin/nc_sync
%config(noreplace) %{_sysconfdir}/profile.d/nc_server.sh
%config(noreplace) %{_sysconfdir}/profile.d/nc_server.csh

%changelog
* Wed Aug 10 2022 Gary Granger <granger@ucar.edu> - 1.3-1
- package version 1.3

* Thu Aug 04 2022 Gary Granger <granger@ucar.edu> - 1.2.1-1
- nc_server v1.2.1
