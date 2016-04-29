
Summary: Server for NetCDF file writing.
Name: nc_server
Version: %{gitversion}
Release: %{releasenum}%{?dist}
License: GPL
Group: Applications/Engineering
Url: http://www.eol.ucar.edu/
Packager: Gordon Maclean <maclean@ucar.edu>
# Allow this package to be relocatable to other places than /opt/nc_server
# rpm --relocate /opt/nc_server=/usr
Prefix: /opt/nc_server
BuildRequires: netcdf-devel libcap-devel nidas-devel eol_scons systemd
%{?systemd_requires}
Vendor: UCAR
Source: %{name}-%{version}.tar.gz
Requires: nc_server-clients isfs-syslog
%description
Server for NetCDF file writing.

%package devel
Summary: nc_server library and header file
Group: Applications/Engineering
%description devel
libnc_server_rpc.so library and header file

%package clients
Summary: Some client programs of nc_server
Group: Applications/Engineering
Obsoletes: nc_server-auxprogs
%description clients
Some client programs of nc_server

%prep
%setup -n nc_server

%build
pwd
scons PREFIX=${RPM_BUILD_ROOT}/opt/nc_server
 
%install
rm -rf $RPM_BUILD_ROOT
scons PREFIX=${RPM_BUILD_ROOT}/opt/nc_server install

cp scripts/* ${RPM_BUILD_ROOT}/opt/nc_server/bin

install -d $RPM_BUILD_ROOT%{_sysconfdir}
cp -r etc/{ld.so.conf.d,profile.d,default} $RPM_BUILD_ROOT%{_sysconfdir}
install -d $RPM_BUILD_ROOT%{_libdir}/pkgconfig
cp -r usr/lib/pkgconfig/* $RPM_BUILD_ROOT%{_libdir}/pkgconfig

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

%files devel
/opt/nc_server/include/nc_server_rpc.h
/opt/nc_server/lib/libnc_server_rpc.so.*
/opt/nc_server/lib/libnc_server_rpc.so
%config %{_sysconfdir}/ld.so.conf.d/nc_server.conf
%config %{_libdir}/pkgconfig/nc_server.pc

%files clients
/opt/nc_server/bin/nc_ping
/opt/nc_server/bin/nc_close
/opt/nc_server/bin/nc_sync
%config(noreplace) %{_sysconfdir}/profile.d/nc_server.sh
%config(noreplace) %{_sysconfdir}/profile.d/nc_server.csh

%changelog
