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
export GPGKEY="NCAR EOL Software <eol-prog@eol.ucar.edu>"

echo WORKSPACE=$WORKSPACE
echo TOPDIR=$TOPDIR
echo DEBIAN_REPOSITORY=$DEBIAN_REPOSITORY
echo YUM_REPOSITORY=$YUM_REPOSITORY


build_rpms()
{
    # Only clean the rpmbuild space if it's Jenkins, since otherwise it can be
    # the user's local rpmbuild space with unrelated packages, and we should
    # go around removing them.  Skip the hash check when a user wants to
    # build, but keep it for Jenkins.
    checkhash=""
    if [ -n "$WORKSPACE" ]; then
        (set -x; rm -rf "$TOPDIR/RPMS"; rm -rf "$TOPDIR/SRPMS")
        checkhash="-c"
    fi
    (set -x; ./build_rpm.sh $checkhash)
}


# [RpmSignPlugin] - Starting signing RPMs ...
# [nc-server-rhel7] $ gpg --fingerprint eol-prog@eol.ucar.edu
# pub   2048R/3376B2DC 2014-08-29
#       Key fingerprint = 80C3 C53F 0D89 4192 CF7B  77A5 DE26 CBC0 3376 B2DC
# uid                  NCAR EOL Software <eol-prog@eol.ucar.edu>
# sub   2048R/1857091F 2014-08-29

sign_rpms()
{
    (set -x; rpm --addsign --define="%_gpg_name ${GPGKEY}" ${TOPDIR}/RPMS/*/*.rpm ${TOPDIR}/SRPMS/*.rpm)
}


push_eol_repo()
{
    source $YUM_REPOSITORY/scripts/repo_funcs.sh
    move_rpms_to_eol_repo ${TOPDIR}/RPMS/*/*.rpm
    move_rpms_to_eol_repo ${TOPDIR}/SRPMS/nc_server*.rpm
    update_eol_repo $YUM_REPOSITORY
}


update_local_packages()
{
    # This command must be matched by a NOPWCMDS setting in /etc/sudoers.
    sudo -n yum -y --disablerepo="*" --enablerepo=eol-signed --refresh -- update \
        nc_server-lib nc_server-devel nc_server-clients nc_server
}



method="${1:-help}"
shift

case "$method" in

    build_rpms)
        build_rpms "$@"
        ;;

    sign_rpms)
        sign_rpms
        ;;

    push_rpms)
        push_eol_repo
        ;;

    update_rpms)
        update_local_packages
        ;;

    *)
        if [ "$method" != "help" ]; then
            echo Unknown command "$1".
        fi
        echo Available commands: build_rpms, sign_rpms, push_rpms, update_rpms.
        exit 1
        ;;

esac
