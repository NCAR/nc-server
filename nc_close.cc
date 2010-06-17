//              Copyright (C) by UCAR
//
// File       : $RCSfile: nc_close.cxx,v $
// Revision   : $Revision$
// Directory  : $Source: /code/cvs/pam/isff/src/nc_server/nc_close.cxx,v $
// System     : PAM
// Date       : $Date$
//
// Description:
//

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
nc_close sends a close command to the nc_server program on a host via RPC.\n\
This will cause the nc_server program to close all NetCDF files that it is\n\
currently writing to.  nc_close is part of the nc_server_rpc package\n\
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

    res = closefiles_1((void *)0, clnt);
    if (res == (int *) NULL) {
        clnt_perror(clnt, "call failed");
    }
    clnt_destroy(clnt);
}
