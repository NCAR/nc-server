#! /bin/bash

# Gateway script for CI functionality.

# TOPDIR is the path to the top of the rpmbuild output tree.  We have to set
# it here so that each step uses the same value.  Packages are written there
# after being built, then signed, then pushed to the EOL package repository.

# If the Jenkins WORKSPACE environment variable is set, then use it to set
# TOPDIR.  Otherwise use the default that build_rpm.sh would use.
if [ -n "$WORKSPACE" ]; then
    export TOPDIR=$WORKSPACE/rpm_build
fi
export TOPDIR=${TOPDIR:-$(rpmbuild --eval %_topdir)_$(hostname)}

# In EOL Jenkins, these are global properties set in Manage Jenkins ->
# Configure System.  Provide defaults here to test outside of Jenkins.
DEBIAN_REPOSITORY="${DEBIAN_REPOSITORY:-/net/ftp/pub/archive/software/debian}"
YUM_REPOSITORY="${YUM_REPOSITORY:-/net/www/docs/software/rpms}"
export DEBIAN_REPOSITORY YUM_REPOSITORY
export GPGKEY="NCAR EOL Software <eol-prog2@eol.ucar.edu>"

echo WORKSPACE=$WORKSPACE
echo TOPDIR=$TOPDIR
echo DEBIAN_REPOSITORY=$DEBIAN_REPOSITORY
echo YUM_REPOSITORY=$YUM_REPOSITORY


compile()
{
    # cache configuration first, then compile with warnings as errors
    (set -x; scons allow_warnings=off -j5)
}


runtests()
{
    compile
    (set -x; scons allow_warnings=off test)
}


build_rpms()
{
    # Only clean the rpmbuild space if it's Jenkins, since otherwise it can be
    # the user's local rpmbuild space with unrelated packages, and we should
    # not go around removing them.
    if [ -n "$WORKSPACE" ]; then
        (set -x; rm -rf "$TOPDIR/RPMS"; rm -rf "$TOPDIR/SRPMS")
    fi
    # this conveniently creates a list of built rpm files in rpms.txt.
    (set -x; scons build_rpm nc_server.spec "$@")
}


update_local_packages()
{
    # convert rpm files to package names, and leave package names alone.
    # technically the rpm files might include SRPMS, but that shouldn't matter
    # as long as the name matches an installed package.
    package_names=""
    for pkg in "$@" ; do
        case "$pkg" in
            *.rpm)
                pkg=$(rpm -q --qf "%{name}\n" -p "$pkg")
                ;;
        esac
        package_names="$package_names $pkg"
    done

    # These commands must be matched by a NOPWCMDS setting in /etc/sudoers.
    # Use install to either install a new package or update an existing one.
    # As of CentOS 8, dnf supports --refresh.  This could also use --repo=eol
    # in place of the usage below, but this is the usage that matches the
    # NOPWCMDS setting.
    dnf="dnf -y --disablerepo=* --enablerepo=eol --refresh --"
    (set -x; sudo -n $dnf install $package_names)
}


method="${1:-help}"
shift

case "$method" in

    compile)
        compile "$@"
        ;;

    test)
        runtests "$@"
        ;;

    build_rpms)
        build_rpms build "$@"
        ;;

    snapshot)
        build_rpms snapshot "$@"
        ;;

    push_rpms)
        $HOME/eol-repo/scripts/upload_packages.sh upload `cat rpms.txt`
        ;;

    update_rpms)
        update_local_packages `cat rpms.txt`
        ;;

    *)
        if [ "$method" != "help" ]; then
            echo Unknown command "$1".
        fi
        echo Available commands: build_rpms, push_rpms, update_rpms.
        exit 1
        ;;

esac
