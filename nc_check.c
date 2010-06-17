/*              Copyright (C) by UCAR
 *
 * File       : $RCSfile: nccheck.c,v $
 * Revision   : $Revision$
 * Directory  : $Source: /net/aster/cvs/aster/isff/src/nc_server/nccheck.c,v $
 * System     : PAM
 * Date       : $Date$
 *
 * Description:
 * This programs tries to thoroughly read a NetCDF file in order
 * to detect a corrupt file.
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include "netcdf.h"

int main(int argc, char **argv)
{

    char *cdfname;
    int ncid;			/* netCDF id */

    int recdim;
    int ndims,nvars,ngatts,natts;

    /* variable shapes */
    int dims[MAX_VAR_DIMS];
    long mindex0[MAX_VAR_DIMS];
    long mindexl[MAX_VAR_DIMS];
    long nrecs,dimsize;

    int zerorecs;

    nc_type vartype;
    nc_type atttype;
    int attlen;
    char *attstring;
    char varname[MAX_NC_NAME];
    char dimname[MAX_NC_NAME];
    char attname[MAX_NC_NAME];
    int iv,ia,id;
    extern int ncopts,ncerr;

    void *voidptr;
    double doubleval;
    float floatval;
    int intval;
    short shortval;

    if (argc < 2) {
        fprintf(stderr,"\
******************************************************************\n\
Check if a NetCDF version 3 file is valid. Reads character attributes and\n\
first and last values of all variables in a NetCDF file.\n\
Exits with status of 0 if successfull or 1 if an error is encountered.\n\
Output is generated on stderr only if an error is encountered.\n\
nc_check is part of the nc_server_rpc package\n\
******************************************************************\n\n");
        fprintf(stderr, "Usage: %s NetCDFfileName\n", argv[0]);
        exit(1);
    }

    ncopts = NC_VERBOSE;

    cdfname = argv[1];

    if ((ncid = ncopen(cdfname, NC_NOWRITE)) < 0) {
        fprintf(stderr,"ncopen %s: %s\n",cdfname,nc_strerror(ncerr));
        exit(1);
    }

    if (ncinquire(ncid,&ndims,&nvars,&ngatts,&recdim) < 0) {
        fprintf(stderr,"ncinquire %s: %s\n",cdfname,nc_strerror(ncerr));
        exit(1);
    }

    if (recdim >= 0) {
        if (ncdiminq(ncid,recdim,dimname,&nrecs) < 0) {
            fprintf(stderr,"ncdiminq %s: %s\n",cdfname,nc_strerror(ncerr));
            exit(1);
        }
    }

    for (iv=0; iv < nvars; iv++) {
        if (ncvarinq(ncid,iv,varname,&vartype,&ndims,dims,&natts) < 0) {
            fprintf(stderr,"ncvarinq %s: %s\n",cdfname,nc_strerror(ncerr));
            exit(1);
        }
        if (ndims > MAX_VAR_DIMS) {
            fprintf(stderr,
                    "%s: %s: maximum number of dimensions(%d) exceeded, ndim=%d\n",
                    cdfname,varname,MAX_VAR_DIMS,ndims);
            exit(1);
        }
        for (ia = 0; ia < natts; ia++) {
            if (ncattname(ncid,iv,ia,attname) < 0) {
                fprintf(stderr,"ncattname %s: %s\n",cdfname,nc_strerror(ncerr));
                exit(1);
            }
            if (ncattinq(ncid,iv,attname,&atttype,&attlen) < 0) {
                fprintf(stderr,"ncattinq %s: %s\n",cdfname,nc_strerror(ncerr));
                exit(1);
            }
            switch (atttype) {
            case NC_CHAR:
                if ((attstring = malloc(attlen+2)) == (void *)0 ) {
                  perror(argv[0]);
                  exit(1);
                }
                if (ncattget(ncid,iv,attname,attstring) < 0) {
                  fprintf(stderr,"ncattget %s: %s\n",cdfname,nc_strerror(ncerr));
                  exit(1);
                }
                free(attstring);
                break;
            default:
                break;
            }
        }

        zerorecs = 0;
        for (id = 0; id < ndims; id++) {
            mindex0[id] = 0;
            if (ncdiminq(ncid,dims[id],dimname,&dimsize) < 0) {
                fprintf(stderr,"ncdiminq %s: %s\n",cdfname,nc_strerror(ncerr));
                exit(1);
            }
            if (dims[id] == recdim && dimsize == 0) zerorecs = 1;
            mindexl[id] = dimsize - 1;
        }

        switch(vartype) {
        case NC_SHORT:
            voidptr = &shortval;
            break;
        case NC_INT:
            voidptr = &intval;
            break;
        case NC_FLOAT:
            voidptr = &floatval;
            break;
        case NC_DOUBLE:
            voidptr = &doubleval;
            break;
        default:
            voidptr = 0;
        }

        /*
         * If this variable has an unlimited dimension, which is zero, don't
         * read it.
         */
        if (voidptr != 0 && !zerorecs) {
            if (ncvarget1(ncid,iv,mindex0,voidptr) < 0) {
                fprintf(stderr,"ncvarget1 %s: %s: %s\n",cdfname,varname,nc_strerror(ncerr));
                exit(1);
            }
            if (ncvarget1(ncid,iv,mindexl,voidptr) < 0) {
                fprintf(stderr,"ncvarget1 last %s: %s: %s\n",cdfname,varname,nc_strerror(ncerr));
                fprintf(stderr,"mindex[0]=%ld\n",mindexl[0]);
                exit(1);
            }
        }
    }
    ncclose(ncid);

    return 0;
}
