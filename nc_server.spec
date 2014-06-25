Summary: Server for NetCDF file writing.
Name: nc_server
Version: 1.0
Release: %{release}%{?dist}
License: GPL
Group: Applications/Engineering
Url: http://www.eol.ucar.edu/
Packager: Gordon Maclean <maclean@ucar.edu>
# Allow this package to be relocatable to other places than /opt/nc_server
# rpm --relocate /opt/nc_server=/usr
Prefix: /opt/nc_server

# becomes RPM_BUILD_ROOT, except on newer versions of rpmbuild
BuildRoot:  %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
Vendor: UCAR
Source: %{name}-%{version}.tar.gz
BuildRequires: netcdf-devel netcdf-cxx-devel libcap-devel nidas-devel
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
install -d $RPM_BUILD_ROOT%{_sysconfdir}/init.d
install -d $RPM_BUILD_ROOT%{_sysconfdir}/ld.so.conf.d
cp -r etc/* $RPM_BUILD_ROOT%{_sysconfdir}
install -d $RPM_BUILD_ROOT%{_libdir}/pkgconfig
cp -r usr/lib/pkgconfig/* $RPM_BUILD_ROOT%{_libdir}/pkgconfig

%pre

adduser=false
addgroup=false
grep -q ^nidas %{_sysconfdir}/passwd || adduser=true
grep -q ^eol %{_sysconfdir}/group || addgroup=true

# check if NIS is running. If so, check if nidas.eol is known to NIS
if which ypwhich > /dev/null 2>&1 && ypwhich > /dev/null 2>&1; then
    ypmatch nidas passwd > /dev/null 2>&1 && adduser=false
    ypmatch eol group > /dev/null 2>&1 && addgroup=false
fi

$addgroup && /usr/sbin/groupadd -g 1342 -o eol
$adduser && /usr/sbin/useradd  -u 10035 -o -N -M -g eol -s /sbin/nologin -d /tmp -c NIDAS -K PASS_MAX_DAYS=-1 nidas || :

%post
# To enable the boot script, uncomment this:
if ! chkconfig --level 3 nc_server; then
    chkconfig --add nc_server 
fi

if ! chkconfig --list nc_server > /dev/null 2>&1; then
    echo "nc_server is not setup to run at boot time"
    chkconfig --list nc_server
fi
exit 0

%post -n nc_server-devel
ldconfig
exit 0

%triggerin -- sudo
# trigger script for nc_server

tmpsudo=/tmp/sudoers_$$
cp %{_sysconfdir}/sudoers $tmpsudo

# Remove requiretty requirement for nidas account so that we can
# do sudo from non-login (crontab) scripts.
if grep -E -q "^Defaults[[:space:]]+requiretty" $tmpsudo; then
    if ! grep -E -q '^Defaults[[:space:]]*:[[:space:]]*[^[:space:]]+[[:space:]]+!requiretty' $tmpsudo; then
        sed -i '
/^Defaults[[:space:]]*requiretty/a\
# The /opt/nc_server/bin/nc_server.check script runs /etc/init.d/nc_server via sudo,\
# which may be handy if it needs to be started from a crontab or at other\
# than boot time.\
# The following statements add permission for the "nidas" user to start\
# nc_server via sudo. If nidas is not a login account, change "nidas"\
# to a login account that will want to run nc_server.check or otherwise\
# start nc_server via sudo. Change this !requiretty\ line and the\
# nc_server lines below.\
Defaults:nidas !requiretty\
' $tmpsudo
    fi
fi

if ! { grep NOPASSWD $tmpsudo | grep -q /etc/init.d/nc_server; }; then
cat << \EOD >> $tmpsudo
nidas ALL=NOPASSWD: /etc/init.d/nc_server
EOD
fi

if ! { grep NOPASSWD $tmpsudo | grep -q /opt/nc_server/bin/nc_server; }; then
cat << \EOD >> $tmpsudo
nidas ALL=NOPASSWD: /opt/nc_server/bin/nc_server
EOD
fi

# remove old sudo entries
if grep -q /usr/bin/nc_server $tmpsudo; then
    sed -i \\,/usr/bin/nc_server,d $tmpsudo
fi

visudo -c -f $tmpsudo && cp $tmpsudo %{_sysconfdir}/sudoers
rm -f $tmpsudo

%clean
rm -rf $RPM_BUILD_ROOT

%files
/opt/nc_server/bin/nc_server
/opt/nc_server/bin/nc_shutdown
/opt/nc_server/bin/nc_server.check
%config(noreplace) %{_sysconfdir}/init.d/nc_server

%files devel
/opt/nc_server/include/nc_server_rpc.h
/opt/nc_server/lib/libnc_server_rpc.so.*
/opt/nc_server/lib/libnc_server_rpc.so
%config %{_sysconfdir}/ld.so.conf.d/nc_server.conf
%config %{_libdir}/pkgconfig/nc_server.pc

%files clients
/opt/nc_server/bin/nc_check
/opt/nc_server/bin/nc_ping
/opt/nc_server/bin/nc_close
/opt/nc_server/bin/nc_sync
%config(noreplace) %{_sysconfdir}/profile.d/nc_server.sh
%config(noreplace) %{_sysconfdir}/profile.d/nc_server.csh

%changelog
* Mon Apr  2 2012 Gordon Maclean <maclean@ucar.edu> 1.0-12
- Create counts variable in existing variable group if necessary.
* Mon Jan  2 2012 Gordon Maclean <maclean@ucar.edu> 1.0-11
- Fix -Weffc++ warnings. Don't set process timezone to GMT.
- Always update units attributes on connection.
* Tue Oct 18 2011 Gordon Maclean <maclean@ucar.edu> 1.0-10
- Put client programs in nc_server-clients.
- Path setting in /etc/profile.d/nc_server.*sh is commented out,
- so by default users must add nc_server to their own path.
- /etc/init.d/nc_server sets its path.
* Sun Oct 16 2011 Gordon Maclean <maclean@ucar.edu> 1.0-9
- Everything now installed to /opt/nc_server, and always to lib, not lib64.
* Thu Oct 13 2011 Gordon Maclean <maclean@ucar.edu> 1.0-8
- Improved error handling in definedatarec call.
* Tue Oct 11 2011 Gordon Maclean <maclean@ucar.edu> 1.0-7
- Added CHECKERROR procedure for batch writers to use instead of NULLPROC.
* Sun Oct  2 2011 Gordon Maclean <maclean@ucar.edu> 1.0-6
- Reduced SYNC_CHECK_INTERVAL from 60 to 5 seconds.
- Cleaned up nc_server.check, which now runs /etc/init.d/nc_server
- instead of nc_server directly.
* Sun Aug 21 2011 Gordon Maclean <maclean@ucar.edu> 1.0-5
- Fix usage of getpwnam_r, getgrnam_r, getgrid_r.
* Wed Aug 10 2011 Gordon Maclean <maclean@ucar.edu> 1.0-4
- Keep incrementing connecionId to avoid the possibility of a paused process
- attempting writes after the connection has timed out and the id has been
- given to another process.
- Much rework of counts variable handling.
- Fix bug in /etc/init.d/nc_server stop - was killing itself.
* Fri Apr 22 2011 Gordon Maclean <maclean@ucar.edu> 1.0-3
- Support more than one -g option to add supplemental group ids to this process.
- Add SET_SETGID capability for the setgroups() call.
- Report strerror_r() if ncerror.get_err() is 0.
* Fri Apr 15 2011 Gordon Maclean <maclean@ucar.edu> 1.0-2
- added /etc/init.d/nc_server boot script. %pre does useradd/groupadd of nidas.eol.
- nc_server has -g runstring option to set the group. sudo is not needed to start.
* Mon Jun  7 2010 Gordon Maclean <maclean@ucar.edu> 1.0-1
- original
