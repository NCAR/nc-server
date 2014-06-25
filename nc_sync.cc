//              Copyright (C) by UCAR
//
// File       : $RCSfile: nc_sync.cxx,v $
// Revision   : $Revision$
// Directory  : $Source: /code/cvs/pam/isff/src/nc_server/nc_sync.cxx,v $
// System     : PAM
// Date       : $Date$
//
// Description:
//   Request that nc_server do a sync on all open files, so
//   that they can be read successfully.

#include "nc_server_rpc.h"

#include <iostream>
#include <stdlib.h>

int main(int argc, char *argv[])
{
    char *host;
    CLIENT *clnt;
    int  *res;

    if (argc < 2) {
        fprintf(stderr,"\
******************************************************************\n\
nc_sync sends a sync command to the nc_server program on a host via RPC.\n\
This will cause the nc_server program to do a ncsync on all NetCDF files that it is\n\
currently writing to.  nc_sync is part of the nc_server-auxprogs package\n\
******************************************************************\n\n");
        fprintf(stderr,"usage:  %s server_host\n", argv[0]);
        exit(1);
    }
    host = argv[1];

    clnt = clnt_create(host, NETCDFSERVERPROG, NETCDFSERVERVERS, "tcp");
    if (clnt == (CLIENT *) NULL) {
        clnt_pcreateerror(host);
        exit(1);
    }

    res = sync_files_2((void *)0, clnt);
    if (res == (int *) NULL) {
        clnt_perror(clnt, "call failed");
    }
    clnt_destroy(clnt);
}
