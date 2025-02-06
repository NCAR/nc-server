# -*- python -*-
#
# SConstruct for building nc_server RPC server program, shared library for
# clients, and related utilities.

# The server requires: nidas, netcdf, then svc, procs, and xdr generated from
# .x interface definition.
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
import subprocess as sp

from SCons.Script import Environment, Configure, PathVariable, EnumVariable
from SCons.Script import Delete, SConscript, Export, Chmod
from SCons.Errors import StopError

import eol_scons

eol_scons.RunScripts()

# Disable the install alias so the installs can be divided between 'install'
# and 'install.root', where the install.root alias is for files which can be
# installed into system directories outside of the PREFIX directory.
eol_scons.EnableInstallAlias(False)

env = Environment(tools=['default', 'gitinfo', 'symlink', 'rpcgen'],
                  GLOBAL_TOOLS=['buildmode'])

conf = Configure(env)
if conf.CheckCHeader('sys/capability.h'):
    conf.env.Append(CPPDEFINES=['HAS_CAPABILITY_H'])
conf.CheckLib('cap')
env = conf.Finish()

opts = eol_scons.GlobalVariables('config.py')
opts.AddVariables(PathVariable('PREFIX',
                               'install path, defaults to NIDAS prefix',
                               None, PathVariable.PathAccept))
opts.AddVariables(PathVariable('INSTALL_PREFIX',
                               'path to be prepended to all install paths',
                               '', PathVariable.PathAccept))
opts.AddVariables(PathVariable('SYSCONFIGDIR', '/etc installation path',
                               '/etc', PathVariable.PathAccept))
opts.AddVariables(PathVariable('PKGCONFIGDIR',
                               'system dir to install nc_server.pc',
                               '/usr/lib64/pkgconfig',
                               PathVariable.PathAccept))
opts.AddVariables(PathVariable('UNITDIR', 'systemd unit install path',
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
opts.Add('PKG_CONFIG_PATH',
         'Paths to search for pkg-config files, besides defaults.  '
         'PREFIX is added automatically if set, so if NIDAS is installed '
         'there, nc_server will build against that NIDAS and install there.')
opts.Update(env)

PREFIX = env.get('PREFIX')

if PREFIX:
    env.PrintProgress(f"Using PREFIX override: {PREFIX}")

# If PREFIX is not set yet, then set it from NIDAS.
if not PREFIX:
    cp = sp.run(['pkg-config', 'nidas', '--variable=prefix'],
                capture_output=True, text=True)
    if cp.returncode != 0:
        raise StopError("PREFIX not specified and NIDAS prefix not "
                        f"found from pkg-config: {cp.stderr}")
    PREFIX = cp.stdout.strip()
    env.PrintProgress(f"Using PREFIX from nidas package: {PREFIX}")

if not PREFIX:
    PREFIX = '/opt/nidas'
    env.PrintProgress(f"Using default PREFIX: {PREFIX}")


def nc_server_prefix(env: Environment):
    "Update global PREFIX in this Environment."
    opts.Update(env)
    env['PREFIX'] = PREFIX
    pkgpath = f'{PREFIX}/lib/pkgconfig'
    if env.get('PKG_CONFIG_PATH'):
        pkgpath += ':' + env['PKG_CONFIG_PATH']
    env['PKG_CONFIG_PATH'] = pkgpath
    # Propagate path to the process environment for running pkg-config
    env['ENV']['PKG_CONFIG_PATH'] = env['PKG_CONFIG_PATH']


env.RequireGlobal(nc_server_prefix)
env.PrintProgress("PKG_CONFIG_PATH: " + env.get('PKG_CONFIG_PATH', ''))


BUILDS = env['BUILDS']
if BUILDS == 'armel':
    env.Tool('armelcross')
if BUILDS == 'armhf':
    env.Tool('armhfcross')

# Default settings for all builds.
env['CCFLAGS'] = ['-g', '-Wall', '-O2']
env['CXXFLAGS'] = ['-std=c++11', '-Weffc++']

env.GitInfo("version.h", "#")


def netcdfcxx(env):
    "Build against netcdf_c++ in PREFIX if found there."
    slib = env.File('$PREFIX/lib/libnetcdf_c++.a')
    dlib = env.File('$PREFIX/lib/libnetcdf_c++.so')
    if slib.exists():
        env.Append(LIBS=[slib])
    elif dlib.exists():
        env.Append(LIBS=[dlib])
    else:
        env.Require('netcdfcxx')
        return
    env.Append(CPPPATH=["$PREFIX/include"])
    env.Append(LIBPATH=["$PREFIX/lib"])
    env.Require('netcdf')


# Clone the netcdf environment before adding the RPC/XDR settings.
nc_env = env.Clone()
nc_env.Require(netcdfcxx)

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
lib_env.Append(CCFLAGS=['-Wno-unused', '-Wno-strict-aliasing'])
libobjs = lib_env.SharedObject([xdr, clnt])
tag = env.get('REPO_TAG')
shlibversion = re.sub(r'^[vV]([0-9.]+)(-.*)?$', r'\1', tag)
lib = lib_env.SharedLibrary('nc_server_rpc', libobjs,
                            SHLIBVERSION=shlibversion)


# Define a tool to build against the nc_server client library.
def nc_server_client(env):
    # dynld client environment needs INSTALL_PREFIX
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
srv_env.Require(netcdfcxx)

srcs = ["nc_server.cc", "nc_server_rpc_procs.cc", svc]

server_lib = srv_env.StaticLibrary("nc_server", srcs)

nc_server = srv_env.Program('nc_server', ["nc_server_main.cc"] + server_lib)

nc_close = clnt_env.Program('nc_close', ['nc_close.cc'])

nc_sync = clnt_env.Program('nc_sync', ['nc_sync.cc'])

nc_shutdown = clnt_env.Program('nc_shutdown', ['nc_shutdown.cc'])

nc_check = nc_env.Program('nc_check', 'nc_check.c')

env.Default([nc_server, nc_close, nc_sync, nc_shutdown, nc_check])

installs = []
libdir = '$PREFIX/lib'
libtgt = env.InstallVersionedLib('${INSTALL_PREFIX}'f'{libdir}', lib)
installs += libtgt
installs += env.Install('${INSTALL_PREFIX}$PREFIX/bin',
                        [nc_server, nc_close, nc_sync, nc_shutdown, nc_check])
installs += env.Install('${INSTALL_PREFIX}$PREFIX/include', 'nc_server_rpc.h')

env['SUBST_DICT'] = {'@NC_SERVER_HOME@': "$PREFIX",
                     '@NC_SERVER_LIBDIR@': libdir,
                     '@PREFIX@': '$PREFIX',
                     '@REPO_TAG@': '$REPO_TAG',
                     '@REQUIRES@': '$PCREQUIRES'}
ncscheck = env.Substfile('scripts/nc_server.check.in')
env.AddPostAction(ncscheck, Chmod(ncscheck[0], 0o775))
installs += env.Install('${INSTALL_PREFIX}$PREFIX/bin',
                        ['scripts/nc_ping', ncscheck])
logconf = env.Substfile('scripts/logrotate.conf.in')
env.Alias('install.logs',
          env.Install('${INSTALL_PREFIX}$PREFIX/logs', logconf))

# Create nc_server.pc, replacing @token@
pc = env.Substfile('nc_server.pc.in')

# pkgconfig file gets installed to two places
installs += env.Install('${INSTALL_PREFIX}$PREFIX/lib/pkgconfig', pc)
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
default/nc_server
""")
for f in sysconfigfiles:
    # the target must be passed as a string and not a node, otherwise the
    # --install-sandbox node factory is not applied to generate the node under
    # the sandbox directory.
    etcfile = env.InstallAs('${INSTALL_PREFIX}${SYSCONFIGDIR}/'f'{f}',
                            f'etc/{f}')
    env.Alias('install.root', etcfile)

sdunit = env.Install("${INSTALL_PREFIX}${UNITDIR}",
                     "systemd/system/nc_server.service")
env.Alias('install.root', sdunit)

SConscript('dynld/SConscript')

test_env = srv_env.Clone()
test_env.Require(['boost_test', 'testing'])
testprog = test_env.Program(['test_nc_server.cc'] + server_lib)

test_env['VALGRIND_OPTS'] = [
    "--num-callers=30",
    "--show-below-main=yes",
    "--leak-check=yes",
    "--error-exitcode=1",
    "--errors-for-leak-kinds=definite,possible"]
test_env['VALGRIND'] = "valgrind $VALGRIND_OPTS"
# find shared rpc library in current directory and append LIBPATH to find
# whatever was set by nidas pkg-config.
test_env['ENV']['LD_LIBRARY_PATH'] = ([test_env.Dir('.').abspath] +
                                      test_env.get('LIBPATH', []))

cmd = "$VALGRIND ./$SOURCE.file --log_level=all"
log = test_env.File("xtest.log")
xtest = test_env.Command([log], testprog,
                         test_env.LogAction([cmd], logpath=log.abspath,
                                            patterns=None))

test_env.Alias('test', xtest)
test_env.AlwaysBuild(xtest)

# This works, but it prints a message for every file and directory removed, so
# resort to just executing the Delete action on the build directory:
#
# env.Clean('build', 'build')

if env.GetOption('clean'):
    env.Execute(Delete(["build", "rpms.txt"]))

env.SetHelp()
