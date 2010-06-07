
env = Environment(platform = 'posix')

env['CCFLAGS'] = [ '-Wall', '-O2' ]

env.RPCGenClient('nc_server_rpc.x')
env.RPCGenHeader('nc_server_rpc.x')
env.RPCGenService('nc_server_rpc.x')
env.RPCGenXDR('nc_server_rpc.x')


libsrcs = Split("""
    nc_server_rpc_xdr.c
    nc_server_rpc_clnt.c
""")

libobjs = env.SharedObject(libsrcs,CCFLAGS=[])

lib = env.SharedLibrary('nc_server_rpc',libobjs)

srcs = Split("""
    nc_server.cc
    nc_server_rpc_procs.cc
    nc_server_rpc_svc.c
""")

env.Program('nc_server', srcs,
    LIBS=["nc_server_rpc","nidas_util","netcdf_c++","netcdf","hdf5","hdf5_hl"],
    LIBPATH=["/opt/local/nidas/x86/lib","."],
    CPPPATH="/opt/local/nidas/x86/include")

#    CPPDEFINES = ['RPC_SVC_FG']

env.Program('nc_close','nc_close.cc',
    LIBS=["nc_server_rpc"],
    LIBPATH=["."])

env.Program('nc_sync','nc_sync.cc',
    LIBS=["nc_server_rpc"],
    LIBPATH=["."])

env.Program('nc_shutdown','nc_shutdown.cc',
    LIBS=["nc_server_rpc"],
    LIBPATH=["."])

env.Program('nccheck','nccheck.c',
    LIBS=["netcdf","hdf5","hdf5_hl"])
