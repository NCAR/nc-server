%define has_systemd 0
%{?systemd_requires: %define has_systemd 1}

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
BuildRequires: netcdf-devel libcap-devel nidas-devel eol_scons
Vendor: UCAR
Source: %{name}-%{version}.tar.gz
Requires: nc_server-clients
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
cp -r etc/{ld.so.conf.d,profile.d} $RPM_BUILD_ROOT%{_sysconfdir}
install -d $RPM_BUILD_ROOT%{_libdir}/pkgconfig
cp -r usr/lib/pkgconfig/* $RPM_BUILD_ROOT%{_libdir}/pkgconfig

%if %has_systemd == 1
cp -r systemd $RPM_BUILD_ROOT/opt/nc_server
%else
cp -r etc/init.d $RPM_BUILD_ROOT%{_sysconfdir}
%endif

%post

%if %has_systemd == 1
echo "See /opt/nc_server/systemd/user/README"
%else
    # To enable the boot script, uncomment this:
    if ! chkconfig --level 3 nc_server; then
        chkconfig --add nc_server 
    fi

    if ! chkconfig --list nc_server > /dev/null 2>&1; then
        echo "nc_server is not setup to run at boot time"
        chkconfig --list nc_server
    fi
%endif
exit 0

%post -n nc_server-devel
ldconfig
exit 0

%clean
rm -rf $RPM_BUILD_ROOT

%files
/opt/nc_server/bin/nc_server
/opt/nc_server/bin/nc_shutdown
/opt/nc_server/bin/nc_server.check
/opt/nc_server/bin/nc_check

%caps(cap_net_bind_service,cap_setgid+p) /opt/nc_server/bin/nc_server

%if %has_systemd == 1
/opt/nc_server/systemd
%else
%config(noreplace) %{_sysconfdir}/init.d/nc_server
%endif

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
