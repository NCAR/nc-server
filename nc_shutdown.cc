//              Copyright (C) by UCAR
//
// File       : $RCSfile: nc_shutdown.cxx,v $
// Revision   : $Revision$
// Directory  : $Source: /code/cvs/pam/isff/src/nc_server/nc_shutdown.cxx,v $
// System     : PAM
// Date       : $Date$
//
// Description:

#include "nc_server_rpc.h"

#include <iostream>
#include <stdlib.h>

int main(int argc, char *argv[])
{
    char *host;
    CLIENT *clnt;

    if (argc < 2) {
        fprintf(stderr,"\
******************************************************************\n\
nc_shutdown sends a shutdown command to the nc_server program on a host via RPC.\n\
This will cause the nc_server program to close all NetCDF files that it is\n\
currently writing to and exit.  nc_shutdown is part of the nc_server_rpc package\n\
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

    shutdown_1((void *)0, clnt);
    clnt_destroy(clnt);
}
