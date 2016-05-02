#
# Makefile for nc-server Debian packages
# The primary task is to invoke scons to do the build and
# install to the $DESTDIR.

SCONS = scons
BUILDS ?= "host"
REPO_TAG ?= v1.1
PREFIX=/opt/nc_server

# Where scons installs things
SCONSLIBDIR = $(DESTDIR)$(PREFIX)/lib
SCONSBINDIR = $(DESTDIR)$(PREFIX)/bin
SCONSINCDIR = $(DESTDIR)$(PREFIX)/include

# Where we want them in the package
LIBDIR = $(DESTDIR)$(PREFIX)/lib/$(DEB_HOST_GNU_TYPE)
BINDIR = $(DESTDIR)$(PREFIX)/bin
INCDIR = $(DESTDIR)$(PREFIX)/include

LDCONF = $(DESTDIR)/etc/ld.so.conf.d/nc_server-$(DEB_HOST_GNU_TYPE).conf
PKGCONFIG = $(DESTDIR)/usr/lib/$(DEB_HOST_GNU_TYPE)/pkgconfig/nc_server.pc

.PHONY : build install clean scons_install $(LDCONF) $(PKGCONFIG)

$(info DESTDIR=$(DESTDIR))
$(info DEB_BUILD_GNU_TYPE=$(DEB_BUILD_GNU_TYPE))
$(info DEB_HOST_GNU_TYPE=$(DEB_HOST_GNU_TYPE))

build:
	$(SCONS) --config=force -j 4 BUILDS=$(BUILDS) REPO_TAG=$(REPO_TAG)

$(LDCONF):
	@mkdir -p $(@D)
	echo "/opt/nidas/lib/$(DEB_HOST_GNU_TYPE)" > $@

$(PKGCONFIG): nc_server.pc
	@mkdir -p $(@D)
	sed -e 's,@PREFIX@,$(PREFIX),g' -e 's/@DEB_HOST_GNU_TYPE@/$(DEB_HOST_GNU_TYPE)/g' -e 's/@REPO_TAG@/$(REPO_TAG)/g' $< > $@

scons_install:
	$(SCONS) -j 4 BUILDS=$(BUILDS) REPO_TAG=$(REPO_TAG) PREFIX=$(DESTDIR)$(PREFIX) install

install: scons_install $(LDCONF) $(PKGCONFIG)
	mkdir -p $(LIBDIR);\
	mv $(SCONSLIBDIR)/*.so* $(LIBDIR);\
	cp scripts/nc_ping $(BINDIR)

clean:
	$(SCONS) -c BUILDS="host"

