
import os
import re
import subprocess

def get_lib():
    try:
        revline = subprocess.Popen(['uname','-m'],
            stdout=subprocess.PIPE).stdout.readline()
        print 'revline=' + revline
        if revline == 'x86_64\n':
            return 'lib64'
        else:
            return 'lib'
    except OSError, (errno,strerror):
        print "Error: %s: %s" %('uname -m',strerror)
        return None

env = Environment(platform = 'posix')

# library major and minor numbers
major = '1'
minor = '0'

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

# libnc_server_rpc.so
libname = env.subst('$SHLIBPREFIX') + 'nc_server_rpc' + env.subst('$SHLIBSUFFIX')

# libnc_server_rpc.so.major
soname = libname + '.' + major

libobjs = env.SharedObject(libsrcs,
    CCFLAGS=env['CCFLAGS'] + ['-Wno-unused', '-Wno-strict-aliasing'])

lib = env.SharedLibrary('nc_server_rpc',libobjs,
    SHLIBSUFFIX=env.subst('$SHLIBSUFFIX') + '.' + major + '.' + minor,
    SHLINKFLAGS=[env['SHLINKFLAGS'] + ['-Wl,-soname=' + soname]])

# link libnc_server_rpc.so.major.minor to libnc_server_rpc.so
env.Command(libname,lib,'cd $TARGET.dir; ln -sf $SOURCE.file $TARGET.file')

# link libnc_server_rpc.so.major.minor to libnc_server_rpc.so.major
env.Command(soname,lib,'cd $TARGET.dir; ln -sf $SOURCE.file $TARGET.file')

srcs = Split("""
    nc_server.cc
    nc_server_rpc_procs.cc
    nc_server_rpc_svc.c
""")

p1 = env.Program('nc_server', srcs,
    LIBS=["nc_server_rpc","nidas_util","netcdf_c++","netcdf","hdf5","hdf5_hl"],
    LIBPATH=["/opt/local/nidas/x86/lib","."],
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

libdir = get_lib()
print 'libdir=' + libdir
env.Install('$PREFIX/bin',[p1,p2,p3,p4,p5])
env.Install('$PREFIX/' + libdir,lib)
env.Command('$PREFIX/' + libdir + '/' + libname,lib,'cd $TARGET.dir; ln -sf $SOURCE.file $TARGET.file')
env.Command('$PREFIX/' + libdir + '/' + soname,lib,'cd $TARGET.dir; ln -sf $SOURCE.file $TARGET.file')
env.Install('$PREFIX/include','nc_server_rpc.h')
env.Alias('install', [ '$PREFIX' ])

