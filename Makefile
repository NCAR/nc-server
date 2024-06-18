#
# Makefile for nc-server Debian packages
# The primary task is to invoke scons to do the build and
# install to the $DESTDIR.

SCONSPATH = scons
BUILDS ?= "host"
REPO_TAG ?= v1.1
PREFIX=/opt/nc_server

# Where to find pkg-configs of other software
PKG_CONFIG_PATH := /usr/lib/$(DEB_HOST_GNU_TYPE)/pkgconfig:/usr/lib/pkgconfig:/usr/share/pkgconfig

SCONS = $(SCONSPATH) BUILDS=$(BUILDS) REPO_TAG=$(REPO_TAG) \
  PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) \
  PKGCONFIGDIR=/usr/lib/$(DEB_HOST_GNU_TYPE)/pkgconfig \
  LDCONFFILE=nc_server-$(DEB_HOST_GNU_TYPE).conf PREFIX=$(PREFIX)

.PHONY : build clean install

$(info DESTDIR=$(DESTDIR))
$(info DEB_BUILD_GNU_TYPE=$(DEB_BUILD_GNU_TYPE))
$(info DEB_HOST_GNU_TYPE=$(DEB_HOST_GNU_TYPE))
$(info PKG_CONFIG_PATH=$(PKG_CONFIG_PATH))

build:
	$(SCONS) --config=force -j 4

install:
	$(SCONS) INSTALL_PREFIX=$(DESTDIR) install install.root

clean:
	$(SCONS) -c
