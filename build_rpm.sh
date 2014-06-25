#!/bin/sh

script=`basename $0`

dopkg=all
install=false

while [ $# -gt 0 ]; do
    case $1 in
        -i)
            install="true"
            ;;
        *)
            dopkg=$1
            ;;
    esac
    shift
done

source repo_scripts/repo_funcs.sh

topdir=`get_rpm_topdir`
rroot=`get_eol_repo_root`

log=/tmp/$script.$$
trap "{ rm -f $log; }" EXIT

set -o pipefail

get_version() 
{
    awk '/^Version:/{print $2}' $1
}

get_release() 
{
    # discard M,S,P, mixed versions
    v=$(svnversion $1 | sed 's/:.*$//' | sed s/[A-Z]//g)
    [ $v == exported ] && v=1
    echo $v
}


pkg=nc_server
if [ $dopkg == all -o $dopkg == $pkg ];then
    version=`get_version ${pkg}.spec`
    release=$(get_release .)

    # tar czf $topdir/SOURCES/${pkg}-${version}.tar.gz --exclude .svn -C ../../.. --transform="s/^nidas/nidas-bin/" nidas/src/SConstruct nidas/src/nidas nidas/src/site_scons nidas/xml

    tar czf $topdir/SOURCES/${pkg}-${version}.tar.gz --exclude .svn -C .. \
        ${pkg}/SConstruct ${pkg}/nc_server.h ${pkg}/nc_server.cc ${pkg}/nc_server_rpc.x \
        ${pkg}/nc_server_rpc_procs.cc ${pkg}/nc_check.c ${pkg}/nc_close.cc ${pkg}/nc_shutdown.cc \
        ${pkg}/nc_sync.cc ${pkg}/site_scons ${pkg}/scripts ${pkg}/etc ${pkg}/usr

    rpmbuild -v -ba --define "release $release" \
        --define "debug_package %{nil}" \
        ${pkg}.spec | tee -a $log  || exit $?
fi

echo "RPMS:"
egrep "^Wrote:" $log
rpms=`egrep '^Wrote:' $log | egrep /S?RPMS/ | awk '{print $2}'`
echo "rpms=$rpms"

if $install && [ -d $rroot ]; then
    echo "Moving rpms to $rroot"
    copy_rpms_to_eol_repo $rpms
elif $install; then
    echo "$rroot not found. Leaving RPMS in $topdir"
else
    echo "-i option not specified. RPMS will not be installed in $rroot"
fi

