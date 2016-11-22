#!/bin/bash

set -e

key='<eol-prog@eol.ucar.edu>'

usage() {
    echo "Usage: ${1##*/} [-s] [-i repository ] arch"
    echo "-s: sign the package files with $key"
    echo "-c: build in a chroot"
    echo "-i: install them with reprepro to the repository"
    echo "-n: don't clean source tree, passing -nc to dpkg-buildpackage"
    echo "arch is armel, armhf or amd64"
    exit 1
}

if [ $# -lt 1 ]; then
    usage $0
fi

sign=false
arch=amd64
args="--no-tgz-check -sa"
use_chroot=false
while [ $# -gt 0 ]; do
    case $1 in
    -i)
        shift
        repo=$1
        ;;
    -c)
        use_chroot=true
        ;;
    -n)
        args="$args -nc -F"
        ;;
    -s)
        sign=true
        ;;
    armel)
        export CC=arm-linux-gnueabi-gcc
        arch=$1
        ;;
    armhf)
        export CC=arm-linux-gnueabihf-gcc
        arch=$1
        ;;
    amd64)
        arch=$1
        ;;
    *)
        usage $0
        ;;
    esac
    shift
done

args="$args -a$arch"

if $use_chroot; then
    dist=$(lsb_release -c | awk '{print $2}')
    if [ $arch == amd64 ]; then
        chr_name=${dist}-amd64-sbuild
    else
        chr_name=${dist}-amd64-cross-${arch}-sbuild
    fi
    if ! schroot -l | grep -F chroot:${chr_name}; then
        echo "chroot named ${chr_name} not found"
        exit 1
    fi
fi

dir=$(dirname $0)
dir=$(readlink -f $dir)
cd $dir

hashfile=$dir/.last_hash_$arch

if $repo; then
    [ -f $hashfile ] && last_hash=$(cat $hashfile)
    this_hash=$(git log -1 --format=%H .)
    if [ "$this_hash" == "$last_hash" ]; then
        echo "No updates in $PWD since last build"
        exit 0
    fi
fi

# create changelog
./deb_changelog.sh > debian/changelog

karg=
if $sign; then
    if [ -z "$GPG_AGENT_INFO" -a -f $HOME/.gpg-agent-info ]; then
        . $HOME/.gpg-agent-info
        export GPG_AGENT_INFO
    fi
    karg=-k"$key"
else
    args="$args -us -uc"
fi

# clean old results
rm -f ../nc-server_*.tar.xz ../nc-server_*.dsc
rm -f $(echo ../nc-server\*_{$arch,all}.{deb,build,changes})

# export DEBUILD_DPKG_BUILDPACKAGE_OPTS="$args"

if $use_chroot; then
    # as normal user, could not
    # sbuild-shell ${dist}-${arch}-sbuild
    # but could
    schroot -c $chr_name --directory=$PWD << EOD
        set -e
        [ -f $HOME/.gpg-agent-info ] && . $HOME/.gpg-agent-info
        export GPG_AGENT_INFO
        debuild $args "$karg" \
            --lintian-opts --suppress-tags dir-or-file-in-opt,package-modifies-ld.so-search-path,package-name-doesnt-match-sonames
EOD
else
    debuild $args "$karg" \
        --lintian-opts --suppress-tags dir-or-file-in-opt,package-modifies-ld.so-search-path,package-name-doesnt-match-sonames
fi

# debuild puts results in parent directory
cd ..

if [ -n "$repo" ]; then
    umask 0002
    chngs=nc-server_*_$arch.changes 
    # pkgs=$(grep "^Binary:" $chngs | sed 's/Binary: //')

    flock $repo sh -c "
        reprepro -V -b $repo --keepunreferencedfiles include jessie $chngs;" \
            && echo $this_hash > $hashfile

    rm -f nc-server_*_$arch.build nc-server_*.dsc nc-server_*.tar.xz nc-server*_all.deb nc-server*_$arch.deb $chngs

else
    echo "build results are in $PWD"
fi
