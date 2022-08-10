#!/bin/bash

script=$(basename $0)

pkg=nc_server
specfile="${pkg}.spec"
# releasenum defaults to 1
releasenum=1

# directory containing script
srcdir=$(readlink -f ${0%/*})

set -o pipefail

topdir=${TOPDIR:-$(rpmbuild --eval %_topdir)_$(hostname)}
sourcedir=$(rpm --define "_topdir $topdir" --eval %_sourcedir)
[ -d $sourcedir ] || mkdir -p $sourcedir


get_specversion() # specfile
{
    specfile="$1"
    specversion=`rpmspec --define "releasenum $releasenum" --srpm -q --queryformat "%{VERSION}\n" nc_server.spec`
}


get_releasenum() # version
{
    version="$1"
    # The release number enumerates different packages built from the same
    # version version of the source.  On each new source version, the release
    # num restarts at 1.  The repo is the definitive source for the latest
    # release num for each version.
    url='https://archive.eol.ucar.edu/software/rpms/fedora-signed/$releasever/$basearch'
    release=`yum --repofrompath "eol-temp,$url" --repo=eol-temp list available nc_server | \
        egrep nc_server | awk '{ print $2; }'`
    repoversion=`echo "$release" | sed -e 's/-.*//'`
    if [ "$repoversion" != "$version" ]; then
        # this version is not the latest release, so start at 1
        :
    elif [ -n "$release" ]; then
        releasenum=`echo "$release" | sed -e 's/.*-//' | sed -e 's/\..*$//'`
    else
        echo "Could not determine current release number, cannot continue."
        exit 1
    fi
}


create_build_clone() # tag
{
    # Create a clean clone of the current repo in its own build directory.
    # But make sure it looks like we're running from the top of a nc-server
    # checkout, both to make sure we don't arbitrarily remove the wrong
    # directory, and because this needs to be a git clone to clone it again.
    if test -d .git && git remote get-url origin | egrep -q nc-server ; then
        (set -x; rm -rf build
        mkdir build
        git clone . build/nc-server)
    else
        echo "This needs to be run from the top of an nc-server clone."
        exit 1
    fi
    tag="$1"
    if [ -n "$tag" ]; then
        (set -x; cd build/nc-server; git checkout "$tag")
        if [ $? != 0 ]; then
            exit 1
        fi
    fi
    # Update version.h.
    scons -C build/nc-server version.h
}


# get the version from the source instead of the spec file
git_version()
{
    eval `scons ./gitdump | grep REPO_`

    # REPO_TAG is the most recent tagged version, so that's what the package is
    # built from.
    if [ "$REPO_TAG" == "unknown" ]; then
        echo "No latest version tag found."
        exit 1
    fi
    tag="$REPO_TAG"
    version=${tag/#v}
}

# get the version to package from the spec file
get_specversion "$specfile"
version="${specversion}"
tag="v${version}"

echo "Getting source for tag ${tag}..."

create_build_clone "$tag"

get_releasenum "$version"

# get the arch the spec file will build
arch=`rpmspec --define "releasenum $releasenum" -q --srpm --queryformat="%{ARCH}\n" $specfile`

echo "Building package for version ${version}, release ${releasenum}, arch: ${arch}."
srpm=`rpmspec --define "releasenum $releasenum" --srpm -q "${specfile}"`.src.rpm
# not sure why rpmspec returns the srpm with the arch, even though rpmbuild
# will generate the srpm without it.
srpm="${srpm/.${arch}}"
rpms=`rpmspec --define "releasenum $releasenum" -q "${specfile}" | while read rpmf; do echo ${rpmf}.rpm ; done`
rpms="$srpms $rpms"
echo "Expecting RPMS:"
for rpmfile in ${rpms}; do
    echo $rpmfile
done

# now we can build the source archive and the package...

(cd build && tar czf $sourcedir/${pkg}-${version}.tar.gz \
    --exclude .svn --exclude .git \
    nc-server/SC* nc-server/nc_server.h nc-server/*.cc \
    nc-server/nc_check.c nc-server/*.x nc-server/version.h \
    nc-server/scripts \
    nc-server/etc nc-server/nc_server.pc.in nc-server/systemd) || exit $?

rpmbuild -v -ba \
    --define "_topdir $topdir"  \
    --define "releasenum $releasenum" \
    --define "debug_package %{nil}" \
    nc_server.spec || exit $?

# SRPM ends up in topdir/SRPMS/$srpmfile
# RPMs end up in topdir/RPMS/<arch>/$rpmfile

for rpmfile in $srpm $rpms ; do
    xfile="$topdir/SRPMS/$rpmfile"
    if [ -f "$xfile" ]; then
        echo "SRPM: $xfile"
    else
        xfile="$topdir/RPMS/$arch/$rpmfile"
        if [ -f "$xfile" ]; then
            echo "RPM: $xfile"
        else
            echo "Missing RPM: $rpmfile"
            exit 1
        fi
    fi
done
