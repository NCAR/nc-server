#!/bin/sh

script=`basename $0`

pkg=nc_server

install=false

while [ $# -gt 0 ]; do
    case $1 in
        -i)
            install="true"
            ;;
    esac
    shift
done


source repo_scripts/repo_funcs.sh

topdir=${TOPDIR:-`get_rpm_topdir`}

sourcedir=$(rpm --define "_topdir $topdir" --eval %_sourcedir)
[ -d $sourcedir ] || mkdir -p $sourcedir

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

version=`get_version ${pkg}.spec`

# jenkins sets SVN_REVISION
release=${SVN_REVISION:=$(get_release .)}

# use --transform to put the package name in the tar path names
tar czf $sourcedir/${pkg}-${version}.tar.gz --exclude .svn --transform="s,./,$pkg/," \
    ./SConstruct ./nc_server.h ./*.cc ./nc_check.c ./nc_server_rpc.x \
    ./site_scons ./scripts ./etc ./usr

rpmbuild -v -ba \
    --define "_topdir $topdir"  \
    --define "release $release" \
    --define "debug_package %{nil}" \
    ${pkg}.spec | tee -a $log  || exit $?

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

