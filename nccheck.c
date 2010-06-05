/*              Copyright (C) by UCAR
 *
 * File       : $RCSfile: nccheck.c,v $
 * Revision   : $Revision$
 * Directory  : $Source: /net/aster/cvs/aster/isff/src/nc_server/nccheck.c,v $
 * System     : PAM
 * Date       : $Date$
 *
 * Description:
 *
 * Updates    : $Log: nccheck.c,v $
 * Revision 1.3  1998/06/11  17:22:22  maclean
 * will try netcdf-3.4
 *
 * Revision 1.2  1998/01/29  18:23:05  maclean
 * Added RCS header
 *
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include "netcdf.h"

#ifndef SVR4
char *strerror(int);
#endif

/* #define PRIOR_TO_NETCDF_3_3 */
#ifdef PRIOR_TO_NETCDF_3_3

#define N_NCERRORS 23
char *nc_strerror(int err)
{
  /*
   * Taken from netcdf.h, version 2.3
   * An XDR error was also defined as NC_EXDR 32, but we'll ignore that
   */
  static char *errorstring[N_NCERRORS]={
        "No Error",                                     /* 0 */
        "Not a netcdf id",                              /* 1 */
        "Too many netcdfs open",                        /* 2 */
        "netcdf file exists && NC_NOCLOBBER",           /* 3 */
        "Invalid Argument",                             /* 4 */
        "Write to read only",                           /* 5 */
        "Operation not allowed in data mode",           /* 6 */
        "Operation not allowed in define mode",         /* 7 */
        "Coordinates out of Domain",                    /* 8 */
        "MAX_NC_DIMS exceeded",                         /* 9 */
        "String match to name in use",                  /* 10 */
        "Attribute not found",                          /* 11 */
        "MAX_NC_ATTRS exceeded",                        /* 12 */
        "Not a netcdf data type",                       /* 13 */
        "Invalid dimension id",                         /* 14 */
        "NC_UNLIMITED in the wrong index",              /* 15 */
        "MAX_NC_VARS exceeded",                         /* 16 */
        "Variable not found",                           /* 17 */
        "Action prohibited on NC_GLOBAL varid",         /* 18 */
        "Not a netcdf file",                            /* 19 */
        "In Fortran, string too short",                 /* 20 */
        "MAX_NC_NAME exceeded",                         /* 21 */
        "NC_UNLIMITED size already in use"		/* 22 */
  };

  if (err <= 0) return strerror(errno);
  else if (err < N_NCERRORS) return errorstring[err];
  else return "Unknown NetCDF error";
}
#endif


/*
 * This programs tries to thoroughly read a NetCDF file in order
 * to detect a corrupt file.
 */

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
  int i,iv,ia,id;
  extern int ncopts,ncerr;

  void *voidptr;
  double doubleval;
  float floatval;
  long longval;
  short shortval;

  if (argc < 2) {
    fprintf(stderr, "Usage: %s NetCDFfileName\n", argv[0]);
    exit(1);
  }

  ncopts = NC_VERBOSE;

  cdfname = argv[1];

  if ((ncid = ncopen(cdfname, NC_NOWRITE)) < 0) {
    fprintf(stderr,"ncopen %s: %s\n",cdfname,nc_strerror(ncerr));
    fprintf(stderr,"ncopen %s: %s\n",cdfname,strerror(errno));
    exit(1);
  }

  if (ncinquire(ncid,&ndims,&nvars,&ngatts,&recdim) < 0) {
    fprintf(stderr,"ncinquire %s: %s\n",cdfname,nc_strerror(ncerr));
    fprintf(stderr,"ncinquire %s: %s\n",cdfname,strerror(errno));
    exit(1);
  }

  if (recdim >= 0) {
    if (ncdiminq(ncid,recdim,dimname,&nrecs) < 0) {
      fprintf(stderr,"ncdiminq %s: %s\n",cdfname,nc_strerror(ncerr));
      fprintf(stderr,"ncdiminq %s: %s\n",cdfname,strerror(errno));
      exit(1);
    }
  }

  for (iv=0; iv < nvars; iv++) {
    if (ncvarinq(ncid,iv,varname,&vartype,&ndims,dims,&natts) < 0) {
      fprintf(stderr,"ncvarinq %s: %s\n",cdfname,nc_strerror(ncerr));
      fprintf(stderr,"ncvarinq %s: %s\n",cdfname,strerror(errno));
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
	fprintf(stderr,"ncattname %s: %s\n",cdfname,strerror(errno));
	exit(1);
      }
      if (ncattinq(ncid,iv,attname,&atttype,&attlen) < 0) {
	fprintf(stderr,"ncattinq %s: %s\n",cdfname,nc_strerror(ncerr));
	fprintf(stderr,"ncattinq %s: %s\n",cdfname,strerror(errno));
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
	  fprintf(stderr,"ncattget %s: %s\n",cdfname,strerror(errno));
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
	fprintf(stderr,"ncdiminq %s: %s\n",cdfname,strerror(errno));
	exit(1);
      }
      if (dims[id] == recdim && dimsize == 0) zerorecs = 1;
      mindexl[id] = dimsize - 1;
    }

    switch(vartype) {
    case NC_SHORT:
      voidptr = &shortval;
      break;
    case NC_LONG:
      voidptr = &longval;
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
	fprintf(stderr,"ncvarget1 %s: %s: %s\n",cdfname,varname,strerror(errno));
	exit(1);
      }
      if (ncvarget1(ncid,iv,mindexl,voidptr) < 0) {
	fprintf(stderr,"ncvarget1 last %s: %s: %s\n",cdfname,varname,nc_strerror(ncerr));
	fprintf(stderr,"ncvarget1 last %s: %s: %s\n",cdfname,varname,strerror(errno));
	fprintf(stderr,"mindex[0]=%d\n",mindexl[0]);
	exit(1);
      }
    }
  }
  ncclose(ncid);

  return 0;
}
