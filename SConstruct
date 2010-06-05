
env = Environment(platform = 'posix')

env.RPCGenClient('nc_server_rpc.x')
env.RPCGenHeader('nc_server_rpc.x')
env.RPCGenService('nc_server_rpc.x')
env.RPCGenXDR('nc_server_rpc.x')

srcs = Split("""
    nc_server.cc
    nc_server_rpc_procs.cc
    nc_server_rpc_svc.c
    nc_server_rpc_xdr.c
""")

env.Program('nc_server', srcs,
    LIBS=["nidas_util","netcdf_c++","netcdf","hdf5","hdf5_hl"],
    LIBPATH="/opt/local/nidas/x86/lib",
    CPPPATH="/opt/local/nidas/x86/include")

env.Program('nc_close','nc_close.cc')

env.Program('nc_sync','nc_sync.cc')

env.Program('nc_shutdown','nc_shutdown.cc')


env.Program('nccheck','nccheck.c',
    LIBS=["netcdf","hdf5","hdf5_hl"])
