# -*- python -*-
#
# SConstruct for building nc_server RPC server program, shared library for
# clients, and related utilities.

# The server requires: nidas, netcdf, then svc, procs, and xdr generated from .x
# interface definition.
#
# The client library only requires: clnt and xdr.
#
# The nc_check utility is strictly a netcdf utility, it only needs netcdf.
#
# The other utilities, nc_close, nc_sync, and nc_shutdown, are strictly
# wrappers which call the client library.
#
# So after creating one default environment, derive from it 4 environments
# according to the various requirements above: srv_env, lib_env,
# client_env, nc_env.

import re
import eol_scons

eol_scons.RunScripts()

# Disable the install alias so the installs can be divided between 'install'
# and 'install.root', where the install.root alias is for files which can be
# installed into system directories outside of the PREFIX directory.
eol_scons.EnableInstallAlias(False)

from SCons.Script import Environment, Configure, PathVariable, EnumVariable
from SCons.Script import Delete, SConscript

env = Environment(tools=['default', 'gitinfo', 'symlink', 'rpcgen'])

conf = Configure(env)
if conf.CheckCHeader('sys/capability.h'):
    conf.env.Append(CPPDEFINES = ['HAS_CAPABILITY_H'])
conf.CheckLib('cap')
env = conf.Finish()

opts = eol_scons.GlobalVariables('config.py')
opts.AddVariables(PathVariable('PREFIX','installation path',
                               '/opt/nc_server', PathVariable.PathAccept))
opts.AddVariables(PathVariable('INSTALL_PREFIX',
                               'path to be prepended to all install paths',
                               '', PathVariable.PathAccept))
opts.AddVariables(PathVariable('SYSCONFIGDIR','/etc installation path',
                               '/etc', PathVariable.PathAccept))
opts.AddVariables(PathVariable('PKGCONFIGDIR',
                               'system dir to install nc_server.pc',
                               '/usr/$ARCHLIBDIR/pkgconfig',
                               PathVariable.PathAccept))
opts.AddVariables(PathVariable('UNITDIR','systemd unit install path',
                               '$SYSCONFIGDIR/systemd/system',
                               PathVariable.PathAccept))

opts.Add('LDCONFFILE',
         'Name of the ld.conf file.  It must be overridden on Debian '
         'and set to nc_server-$(DEB_HOST_GNU_TYPE).conf.',
         'nc_server.conf')
opts.Add('REPO_TAG',
         'git tag of the source, in the form "vX.Y", when '
         'building outside of a git repository')
opts.Add(EnumVariable('BUILDS',
         help='Build architecture: host, armbe, armel or armhf.',
         default='host',
         allowed_values=['host', 'armbe', 'armel', 'armhf']))
opts.Add('ARCHLIBDIR', 
         'Where to install nc_server libraries relative to $PREFIX')
opts.Add('PKG_CONFIG_PATH', 
         'Path to pkg-config files, if you need other than the system default')
opts.Update(env)

# Propagate path to the process environment for running pkg-config
if 'PKG_CONFIG_PATH' in env:
    env['ENV']['PKG_CONFIG_PATH'] = env['PKG_CONFIG_PATH']

# use sharedlibrary tool to get ARCHLIBDIR, but not using the builders in
# favor of the built-in scons shared library builders.
env.Tool('sharedlibrary')

BUILDS = env['BUILDS']
if BUILDS == 'armel':
    env.Tool('armelcross')
if BUILDS == 'armhf':
    env.Tool('armhfcross')

# Default settings for all builds.
env['CCFLAGS'] = ['-g', '-Wall', '-O2']
env['CXXFLAGS'] = ['-std=c++11', '-Weffc++']

env.GitInfo("version.h", "#")

# Clone the netcdf environment before adding the RPC/XDR settings.
nc_env = env.Clone()
nc_env.Require('netcdfcxx')

# -L: generated code sends rpc server errors to syslog
env['RPCGENSERVICEFLAGS'] = ['-L']

# Tool which adds build settings for RPC/XDR.
def rpc(env):
    # As of Fedora 28, glibc does not include the deprecated Sun RPC
    # interface because it does not support IPv6:
    #
    # https://fedoraproject.org/wiki/Changes/SunRPCRemoval
    #
    # So use the tirpc package config if available, otherwise fall back to
    # the legacy rpc built into glibc.
    try:
        env.ParseConfig('pkg-config --cflags --libs libtirpc')
        env.PrintProgress("Using libtirpc.")
        env['PCREQUIRES'] = "libtirpc"
    except OSError:
        env.PrintProgress("Using legacy rpc.")
        pass

Export('rpc')
# The rest of the environments to setup, server, lirbrary, and clients,
# will need RPC/XDR.
env.Tool(rpc)

# Generate the rpcgen products from the base environment.
clnt = env.RPCGenClient('nc_server_rpc.x')
header = env.RPCGenHeader('nc_server_rpc.x')
svc = env.RPCGenService('nc_server_rpc.x')
xdr = env.RPCGenXDR('nc_server_rpc.x')
env.Depends(xdr, header)

# Build the client library
lib_env = env.Clone()
lib_env.Append(CCFLAGS = ['-Wno-unused', '-Wno-strict-aliasing'])
libobjs = lib_env.SharedObject([xdr, clnt])
tag = env.get('REPO_TAG')
shlibversion = re.sub(r'^[vV]([0-9.]+)(-.*)?$', r'\1', tag)
lib = lib_env.SharedLibrary('nc_server_rpc', libobjs,
                            SHLIBVERSION=shlibversion)

# Define a tool to build against the nc_server client library.
def nc_server_client(env):
    # dynld client environment needs INSTALL_PREFIX and ARCHLIBDIR
    opts.Update(env)
    if 'PKG_CONFIG_PATH' in env:
        env['ENV']['PKG_CONFIG_PATH'] = env['PKG_CONFIG_PATH']
    env.Tool('sharedlibrary')
    env.AppendUnique(LIBPATH='#')
    # only need nidas_util, so get only the lib path for nidas and append
    # nidas_util ourselves
    env.ParseConfig('pkg-config --cflags --libs-only-L nidas')
    env['LIBNC_SERVER_RPC'] = lib
    env.Append(LIBS=['nc_server_rpc', 'nidas_util'])
    env.Tool(rpc)

Export('nc_server_client')

# clients need the client library
clnt_env = env.Clone()
clnt_env.Tool(nc_server_client)

# Server needs xdr from the client library, so derive the server
# environment from the client env, then add netcdf, and get nidas using
# pkg-config.  This might be able to use the nidas tool instead, but that
# has not been tried yet.
srv_env = clnt_env.Clone()
srv_env.Require(['netcdfcxx'])

srcs = ["nc_server.cc", "nc_server_rpc_procs.cc", svc]

env.PrintProgress("ARCHLIBDIR=%s" % env['ARCHLIBDIR'])

nc_server = srv_env.Program('nc_server', srcs)

nc_close = clnt_env.Program('nc_close', ['nc_close.cc'])

nc_sync = clnt_env.Program('nc_sync', ['nc_sync.cc'])

nc_shutdown = clnt_env.Program('nc_shutdown', ['nc_shutdown.cc'])

nc_check = nc_env.Program('nc_check','nc_check.c')

installs = []
libdir = '$PREFIX/$ARCHLIBDIR'
libtgt = env.InstallVersionedLib('${INSTALL_PREFIX}'f'{libdir}', lib)
installs += libtgt
installs += env.Install('${INSTALL_PREFIX}$PREFIX/bin',
                        [nc_server, nc_close, nc_sync, nc_shutdown, nc_check])
installs += env.Install('${INSTALL_PREFIX}$PREFIX/include', 'nc_server_rpc.h')

env['SUBST_DICT'] = {'@NC_SERVER_HOME@': "$PREFIX",
                     '@NC_SERVER_LIBDIR@': libdir,
                     '@PREFIX@': '$PREFIX',
                     '@ARCHLIBDIR@': '$ARCHLIBDIR',
                     '@REPO_TAG@': '$REPO_TAG',
                     '@REQUIRES@': '$PCREQUIRES' }
ncscheck = env.Substfile('scripts/nc_server.check.in')
env.AddPostAction(ncscheck, Chmod(ncscheck[0], 0o775))
installs += env.Install('${INSTALL_PREFIX}$PREFIX/bin', ['scripts/nc_ping', ncscheck])
logconf = env.Substfile('scripts/logrotate.conf.in')
env.Alias('install.logs', env.Install('${INSTALL_PREFIX}$PREFIX/logs', logconf))

# Create nc_server.pc, replacing @token@
pc = env.Substfile('nc_server.pc.in')

# pkgconfig file gets installed to two places
installs += env.Install('${INSTALL_PREFIX}$PREFIX/$ARCHLIBDIR/pkgconfig', pc)
env.Alias('install.root', env.Install('${INSTALL_PREFIX}${PKGCONFIGDIR}', pc))

# Install userspace systemd unit examples.
installs += env.Install('${INSTALL_PREFIX}$PREFIX/etc/systemd', 'systemd/user')

# Normal installs get a normal install alias
env.Alias('install', installs)

# Set the library directory in the ld.so config file
env.Substfile('etc/ld.so.conf.d/$LDCONFFILE',
              'etc/ld.so.conf.d/nc_server.conf.in')

# Install sysconfig files.
sysconfigfiles = env.Split("""
ld.so.conf.d/$LDCONFFILE
profile.d/nc_server.sh
profile.d/nc_server.csh
default/nc_server
""")
for f in sysconfigfiles:
    # the target must be passed as a string and not a node, otherwise the
    # --install-sandbox node factory is not applied to generate the node under
    # the sandbox directory.
    etcfile = env.InstallAs('${INSTALL_PREFIX}${SYSCONFIGDIR}/'f'{f}', f'etc/{f}')
    env.Alias('install.root', etcfile)

sdunit = env.Install("${INSTALL_PREFIX}${UNITDIR}", "systemd/system/nc_server.service")
env.Alias('install.root', sdunit)

SConscript('dynld/SConscript')

# This works, but it prints a message for every file and directory removed, so
# resort to just executing the Delete action on the build directory:
#
# env.Clean('build', 'build')

if env.GetOption('clean'):
    env.Execute(Delete(["build", "rpms.txt"]))

env.SetHelp()
