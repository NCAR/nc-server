#!/bin/sh

script=$(basename $0)

hashcheck=false
while [ $# -gt 0 ]; do
    case $1 in
        -c)
            hashcheck=true
            ;;
        *)
            echo "Usage: $script [-c]"
            echo "  -c: check git hash to see if build is required"
            exit 1
    esac
    shift
done

# directory containing script
srcdir=$(readlink -f ${0%/*})

hashfile=$srcdir/.last_hash

set -o pipefail

pkg=nc_server

if $hashcheck; then
    [ -f $hashfile ] && last_hash=$(cat $hashfile)
    pushd $srcdir
    this_hash=$(git log -1 --format=%H .)
    popd
    if [ "$this_hash" == "$last_hash" ]; then
        echo "No updates since last build"
        exit 0
    fi
fi

topdir=${TOPDIR:-$(rpmbuild --eval %_topdir)_$(hostname)}
sourcedir=$(rpm --define "_topdir $topdir" --eval %_sourcedir)
[ -d $sourcedir ] || mkdir -p $sourcedir

log=$(mktemp /tmp/${script}_XXXXXX)
tmpspec=`mktemp /tmp/${script}_XXXXXX.spec`
awkcom=`mktemp /tmp/${script}_XXXXXX.awk`

trap "{ rm -f $log $tmpspec $awkcom; }" EXIT

if ! gitdesc=$(git describe --match "v[0-9]*"); then
    echo "git describe failed, looking for a tag of the form v[0-9]*"
    # gitdesc="v1.0"
    exit 1
fi

# example output of git describe: v2.0-14-gabcdef123
gitdesc=${gitdesc/#v}       # remove leading v
version=${gitdesc%%-*}       # 2.0

release=${gitdesc#*-}       # 14-gabcdef123
release=${release%-*}       # 14
[ $gitdesc == "$release" ] && release=0 # no dash

# run git describe on each hash to create a version
cat << \EOD > $awkcom
/^[0-9a-f]{7}/ {
    cmd = "git describe --match '[vV][0-9]*' " $0 " 2>/dev/null"
    res = (cmd | getline version)
    close(cmd)
    if (res == 0) {
        version = ""
    }
}
/^\*/ { print $0,version }
/^-/ { print $0 }
/^$/ { print $0 }
EOD

# create change log from git log messages.
# Put SHA hash by itself on first line. Above awk script then
# converts it to the output of git describe, and appends it to "*" line.
# Truncate subject line at 60 characters 
# git convention is that the subject line is supposed to be 50 or shorter
git log --max-count=100 --date-order --format="%H%n* %cd %aN%n- %s%n" --date=local . | \
    sed -r 's/[0-9]+:[0-9]+:[0-9]+ //' | sed -r 's/(^- .{,60}).*/\1/' | \
    awk --re-interval -f $awkcom | cat ${pkg}.spec - > $tmpspec

scons version.h

cd ..

tar czf $sourcedir/${pkg}-${version}.tar.gz --exclude .svn --exclude .git \
    nc_server/SC* nc_server/nc_server.h nc_server/*.cc \
    nc_server/nc_check.c nc_server/*.x nc_server/version.h \
    nc_server/scripts \
    nc_server/etc nc_server/usr nc_server/systemd || exit $?
cd -

rpmbuild -v -ba \
    --define "_topdir $topdir"  \
    --define "gitversion $version" --define "releasenum $release" \
    --define "debug_package %{nil}" \
    $tmpspec | tee -a $log  || exit $?

$hashcheck && echo $this_hash > $hashfile

echo "RPMS:"
egrep "^Wrote:" $log
rpms=`egrep '^Wrote:' $log | egrep /S?RPMS/ | awk '{print $2}'`
echo "rpms=$rpms"

