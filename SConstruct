
env = Environment(platform = 'posix').Clone(tools=['sharedlibrary'])

conf = Configure(env)
if conf.CheckCHeader('sys/capability.h'):
    conf.env.Append(CPPDEFINES = ['HAS_CAPABILITY_H'])
if conf.CheckLib('cap'):
    conf.env.AppendUnique(LIBS = 'cap')
env = conf.Finish()

# library major and minor numbers
env['SHLIBMAJORVERSION'] = '1'
env['SHLIBMINORVERSION'] = '0'

opts = Variables()
opts.AddVariables(PathVariable('PREFIX','installation path',
    '/usr', PathVariable.PathAccept))
opts.Update(env)

env['CCFLAGS'] = [ '-g', '-Wall', '-O2' ]

env.RPCGenClient('nc_server_rpc.x')
env.RPCGenHeader('nc_server_rpc.x')
env.RPCGenService('nc_server_rpc.x')
env.RPCGenXDR('nc_server_rpc.x')

libsrcs = Split("""
    nc_server_rpc_xdr.c
    nc_server_rpc_clnt.c
""")

libobjs = env.SharedObject(libsrcs,
    CCFLAGS=env['CCFLAGS'] + ['-Wno-unused', '-Wno-strict-aliasing'])

lib = env.SharedLibrary3('nc_server_rpc',libobjs)

srcs = Split("""
    nc_server.cc
    nc_server_rpc_procs.cc
    nc_server_rpc_svc.c
""")

p1 = env.Program('nc_server', srcs,
    LIBS=["nc_server_rpc","nidas_util","netcdf_c++","netcdf","hdf5","hdf5_hl"],
    LIBPATH=["/opt/local/nidas/x86/" + env["LIBDIR"],"."],
    CPPPATH="/opt/local/nidas/x86/include")

#    CPPDEFINES = ['RPC_SVC_FG']

p2 = env.Program('nc_close','nc_close.cc',
    LIBS=["nc_server_rpc"],
    LIBPATH=["."])

p3 = env.Program('nc_sync','nc_sync.cc',
    LIBS=["nc_server_rpc"],
    LIBPATH=["."])

p4 = env.Program('nc_shutdown','nc_shutdown.cc',
    LIBS=["nc_server_rpc"],
    LIBPATH=["."])

p5 = env.Program('nc_check','nc_check.c',
    LIBS=["netcdf","hdf5","hdf5_hl"])

libtgt = env.SharedLibrary3Install('$PREFIX',lib)
env.Install('$PREFIX/bin',[p1,p2,p3,p4,p5])
env.Install('$PREFIX/include','nc_server_rpc.h')
env.Alias('install', [ '$PREFIX' ])
env.Alias('install',libtgt)

