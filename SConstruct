
import eol_scons

env = Environment(tools=['default', 'gitinfo', 'symlink'])

conf = Configure(env)
if conf.CheckCHeader('sys/capability.h'):
    conf.env.Append(CPPDEFINES = ['HAS_CAPABILITY_H'])
if conf.CheckLib('cap'):
    conf.env.AppendUnique(LIBS = 'cap')
env = conf.Finish()

opts = Variables()
opts.AddVariables(PathVariable('PREFIX','installation path',
    '/opt/nc_server', PathVariable.PathAccept))
opts.Add('REPO_TAG',
    'git tag of the source, in the form "vX.Y", when building outside of a git repository')
opts.Add('BUILDS',
    'A host architecture to build for: host, armbe, armel or armhf.', 'host')

opts.Update(env)

BUILDS = Split(env['BUILDS'])

if 'host' in BUILDS:
    # Must wait to load sharedlibrary until REPO_TAG is set in all situations
    env = env.Clone(tools=['sharedlibrary'])
elif 'armel' in BUILDS:
    # Must wait to load sharedlibrary until REPO_TAG is set in all situations
    env = env.Clone(tools=['armelcross', 'sharedlibrary'])
elif 'armhf' in BUILDS:
    # Must wait to load sharedlibrary until REPO_TAG is set in all situations
    env = env.Clone(tools=['armhfcross', 'sharedlibrary'])

nidas_flags = env.ParseFlags('!pkg-config --cflags --libs nidas')
# hdf_flags = env.ParseFlags('!pkg-config --cflags --libs hdf5')
nc_flags = env.ParseFlags('!pkg-config --cflags --libs netcdf')

env.GitInfo("version.h", "#")

env['CCFLAGS'] = [ '-g', '-Wall', '-O2' ]
env['CXXFLAGS'] = [ '-Weffc++' ]

# -L: generated code sends rpc server errors to syslog
env['RPCGENSERVICEFLAGS'] = ['-L']

env.RPCGenClient('nc_server_rpc.x')
env.RPCGenHeader('nc_server_rpc.x')
env.RPCGenService('nc_server_rpc.x')
env.RPCGenXDR('nc_server_rpc.x')
env.Depends('nc_server_rpc_xdr.c', ['nc_server_rpc.h'])

libsrcs = Split("""
    nc_server_rpc_xdr.c
    nc_server_rpc_clnt.c
""")

libobjs = env.SharedObject(libsrcs,
    CCFLAGS=env['CCFLAGS'] + ['-Wno-unused', '-Wno-strict-aliasing'],
    CPPPATH='')

# Don't want nidas libraries searched here
lib = env.SharedLibrary3('nc_server_rpc',libobjs, LIBS='', LIBPATH='')

srcs = Split("""
    nc_server.cc
    nc_server_rpc_procs.cc
    nc_server_rpc_svc.c
""")

env.Append(LIBPATH='.')

p1 = env.Program('nc_server', srcs,
    CPPPATH=[nidas_flags['CPPPATH']],
    LIBS=["nc_server_rpc", 'nidas_util', 'netcdf_c++', nc_flags['LIBS']],
    LIBPATH=['.', nidas_flags['LIBPATH'], nc_flags['LIBPATH']])

#    CPPDEFINES = ['RPC_SVC_FG']

p2 = env.Program('nc_close','nc_close.cc',
    LIBS=["nc_server_rpc"],LIBPATH=['.'])

p3 = env.Program('nc_sync','nc_sync.cc',
    LIBS=["nc_server_rpc"],LIBPATH=['.'])

p4 = env.Program('nc_shutdown','nc_shutdown.cc',
    LIBS=["nc_server_rpc"],LIBPATH=['.'])

p5 = env.Program('nc_check','nc_check.c',
    LIBS=['nc_server_rpc', 'netcdf_c++', nc_flags['LIBS']],
    LIBPATH=['.',nc_flags['LIBPATH']])

libtgt = env.SharedLibrary3Install('$PREFIX',lib)
env.Install('$PREFIX/bin',[p1,p2,p3,p4,p5])
env.Install('$PREFIX/include','nc_server_rpc.h')
env.Alias('install', [ '$PREFIX' ])
env.Alias('install',libtgt)

