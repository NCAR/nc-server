#!/bin/bash

script=$(basename $0)

# specfile must be specified, then pkg name is extracted from it.
specfile=
pkgname=
# releasenum defaults to 1
releasenum=1
tag=
version=
arch=
srpm=
rpms=

# temporary directory to clone source and create archive.  we want it to be
# local rather than in /tmp so git can optimize the clone with hard
# links.
builddir=build/build_rpm.$$

set -o pipefail

topdir=${TOPDIR:-$(rpmbuild --eval %_topdir)_$(hostname)}
sourcedir=$(rpm --define "_topdir $topdir" --eval %_sourcedir)
[ -d $sourcedir ] || mkdir -p $sourcedir


get_pkgname_from_spec() # specfile
{
    specfile="$1"
    pkgname=`rpmspec --define "releasenum $releasenum" --srpm -q --queryformat "%{NAME}\n" "$specfile"`
}


# set version and tag from the spec file
get_version_and_tag_from_spec() # specfile
{
    specfile="$1"
    version=`rpmspec --define "releasenum $releasenum" --srpm -q --queryformat "%{VERSION}\n" "$specfile"`
    tag="v${version}"
    tag=`echo "$tag" | sed -e 's/~/-/'`
}


# get the version and tag from the source instead of the spec file
get_version_and_tag_from_git()
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


get_releasenum() # version
{
    version="$1"
    # The release number enumerates different packages built from the same
    # version version of the source.  On each new source version, the release
    # num restarts at 1.  The repo is the definitive source for the latest
    # release num for each version.
    local eolreponame
    get_eolreponame
    url="https://archive.eol.ucar.edu/software/rpms/${eolreponame}-signed"
    url="$url/\$releasever/\$basearch"
    yum="yum --refresh --repofrompath eol-temp,$url --repo=eol-temp"
    # yum on centos7 does not support --refresh or --repofrompath, so for now
    # resort to relying on the eol repo to be already defined, and explicitly
    # update the cache for it...
    yum="yum --disablerepo=* --enablerepo=eol-signed"
    $yum makecache
    entry=`$yum list $pkgname egrep $pkgname | tail -1`
    echo "$entry"
    release=`echo "$entry" | awk '{ print $2; }'`
    repoversion=`echo "$release" | sed -e 's/-.*//'`
    if [ "$repoversion" != "$version" ]; then
        echo "Version $version looks new, restarting at releasenum 1."
        releasenum=1
    elif [ -n "$release" ]; then
        releasenum=`echo "$release" | sed -e 's/.*-//' | sed -e 's/\..*$//'`
        releasenum=$((1+$releasenum))
    else
        echo "Could not determine current release number, cannot continue."
        exit 1
    fi
}


create_build_clone() # tag
{
    # Create a clean clone of the current repo in its own build directory.
    tag="$1"
    echo "Cloning source for tag: ${tag}..."
    # we want to copy the origin url in the cloned repository so it shows
    # up same as in the source repository.
    url=`git config --local --get remote.origin.url`
    git="git -c advice.detachedHead=false"
    if test -d .git ; then
        (set -x; rm -rf "$builddir"
        mkdir -p "$builddir"
        $git clone . "$builddir/$pkgname"
        cd "$builddir/$pkgname" && git remote set-url origin "$url")
    else
        echo "This needs to be run from the top of the repository."
        exit 1
    fi
    if [ -n "$tag" ]; then
        (set -x;
         cd "$builddir/$pkgname";
         $git checkout "$tag")
        if [ $? != 0 ]; then
            exit 1
        fi
    fi
    # Update version headers using the gitinfo alias.
    scons -C "$builddir/$pkgname" versionfiles
}

# get the full paths to the rpm files given the spec file and the release
# number.  sets variables rpms, srpm, and arch
get_rpms() # specfile releasenum
{
    local specfile="$1"
    local releasenum="$2"
    rpms=""
    if [ -z "$specfile" -o -z "$releasenum" ]; then
        echo "get_rpms {specfile} {releasenum}"
        exit 1
    fi
    # get the arch the spec file will build
    arch=`rpmspec --define "releasenum $releasenum" -q --srpm --queryformat="%{ARCH}\n" $specfile`

    srpm=`rpmspec --define "releasenum $releasenum" --srpm -q "${specfile}"`.src.rpm
    # not sure why rpmspec returns the srpm with the arch, even though srpm
    # actually built by rpmbuild does not have it.
    srpm="${srpm/.${arch}}"
    # SRPM ends up in topdir/SRPMS/$srpmfile
    # RPMs end up in topdir/RPMS/<arch>/$rpmfile
    srpm="$topdir/SRPMS/$srpm"
    rpms=`rpmspec --define "releasenum $releasenum" -q "${specfile}" | while read rpmf; do echo "$topdir/RPMS/$arch/${rpmf}.rpm" ; done`
}


# set eolreponame to fedora or epel according to the current dist
get_eolreponame()
{
    case `rpm -E %{dist}` in

        *fc*) eolreponame=fedora ;;
        *el*) eolreponame=epel ;;
        *) eolreponame=epel ;;

    esac
}


clean_rpms() # rpms
{
    echo "Removing expected RPMS:"
    for rpmfile in ${rpms}; do
        (set -x ; rm -f "$rpmfile")
    done
}


run_rpmbuild()
{
    get_pkgname_from_spec "$specfile"

    # get the version to package from the spec file
    get_version_and_tag_from_spec "$specfile"

    create_build_clone "$tag"

    get_releasenum "$version"

    get_rpms "$specfile" "$releasenum"

    clean_rpms "$rpms"

    # now we can build the source archive and the package...
    cat <<EOF
Building ${pkgname} version ${version}, release ${releasenum}, arch: ${arch}.
EOF

    (cd "$builddir" && tar czf $sourcedir/${pkgname}-${version}.tar.gz \
        --exclude .svn --exclude .git $pkgname) || exit $?

    rpmbuild -v -ba \
        --define "_topdir $topdir"  \
        --define "releasenum $releasenum" \
        --define "debug_package %{nil}" \
        $specfile || exit $?

    cat /dev/null > rpms.txt
    for rpmfile in $srpm $rpms ; do
        if [ -f "$rpmfile" ]; then
            echo "RPM: $rpmfile"
            echo "$rpmfile" >> rpms.txt
        else
            echo "Missing RPM: $rpmfile"
            exit 1
        fi
    done
}


usage()
{
    echo "Usage: ${script} {specfile} {op} [args]"
    echo "ops: pkgname, releasenum, version, clone, build"
}

specfile="$1"
if [ -z "$specfile" ]; then
    usage
    exit 1
fi
shift

if [ ! -f "$specfile" ]; then
    echo "spec file not found: $specfile"
    exit 1
fi

op="$1"
if [ -z "$op" ]; then
    op=build
else
    shift
fi

case "$op" in

    pkgname)
        get_pkgname_from_spec "$specfile"
        echo Package name: $pkgname
        ;;

    releasenum)
        if [ -z "$1" ]; then
            echo "Usage: $script {specfile} releasenum {version}"
            exit 1
        fi
        get_pkgname_from_spec "$specfile"
        get_releasenum "$@"
        echo Next releasenum: "$releasenum"
        ;;

    version)
        get_version_and_tag_from_spec "$specfile"
        echo $tag
        ;;

    clone)
        get_pkgname_from_spec "$specfile"
        create_build_clone "$@"
        ;;

    build)
        run_rpmbuild
        ;;

    *)
        echo "unknown operation: $op"
        usage
        exit 1
        ;;

esac
