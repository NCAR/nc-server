/*              Copyright (C) by UCAR
 *
 * File       : $RCSfile: nc_server_rpc.x,v $
 * Revision   : $Revision: 1.9 $
 * Directory  : $Source: /code/cvs/pam/isff/src/lib/nc_server_rpc.x,v $
 * System     : PAM
 * Date       : $Date: 2004/10/27 23:54:51 $
 *
 * Description:
 *
 * Updates    : $Log: nc_server_rpc.x,v $
 * Updates    : Revision 1.9  2004/10/27 23:54:51  maclean
 * Updates    : added support for NS_MAXIMUM type
 * Updates    :
 * Updates    : Revision 1.8  2002/12/16 01:09:28  maclean
 * Updates    : added sync rpc function
 * Updates    :
 * Updates    : Revision 1.7  2000/01/11 18:02:19  maclean
 * Updates    : changed str_attrs member name to attrs (ANSI c++ reqmt)
 * Updates    :
 * Revision 1.6  1998/07/31  19:13:07  maclean
 * moved common/src/lib to isff/src/lib
 *
 * Revision 1.5  1998/04/29  16:39:25  maclean
 * fix for linux
 *
 * Revision 1.4  1998/01/29  17:37:21  maclean
 * set RCS comment character "cvs admin -c*"
 *
 * Revision 1.3  1998/01/29  17:34:00  maclean
 * Added shutdown RPC procedure
 * 
 *
 */

enum NS_rectype {
	NS_MINIMUM,NS_MEANN,NS_MEAN,NS_COVN,NS_COV,NS_FLUX,NS_RFLUX,NS_SFLUX,
	NS_TRIVAR,NS_PRUNEDTRIVAR,NS_TIMESERIES,NS_MAXIMUM
};

/* It ain't no accident that these match NC_LONG and NC_FLOAT in netcdf.h */
enum NS_datatype { NS_LONG=4, NS_FLOAT=5 };

typedef int NSlong;	/* XDR lang uses "int" for 32 bit integer */

struct field {
  string name<>;
  string units<>;
};

struct dimension {
  string name<>;
  int size;
};

struct str_attr {
  string name<>;
  string value<>;
};

struct str_attrs {
  str_attr attrs<>;
};

struct datadef {
  double interval;
  int connectionId;
  NS_rectype rectype;
  NS_datatype datatype;
  field fields<>;
  dimension dimensions<>;
  str_attrs attrs<>;	/* for each variable, a vector of string attributes */
  float floatFill;
  NSlong longFill;
  bool fillmissingrecords;
};


struct datarec_float {
  double time;
  float data<>;
  int connectionId;
  int datarecId;
  NSlong cnts<>;
  int start<>;
  int count<>;
};

struct datarec_long {
  double time;
  NSlong data<>;
  int connectionId;
  int datarecId;
  NSlong cnts<>;
  int start<>;
  int count<>;
};

struct historyrec {
  string history<>;
  int connectionId;
};
struct connection {
  double filelength;
  double interval;
  string filenamefmt<>;
  string outputdir<>;
  string cdlfile<>;
};

program NETCDFSERVERPROG {
  version NETCDFSERVERVERS {
    int OPENCONNECTION(connection) = 1;
    int WRITEDATAREC_FLOAT(datarec_float) = 2;
    int CLOSECONNECTION(int) = 3;
    void WRITEDATARECBATCH_FLOAT(datarec_float) = 4;
    void WRITEHISTORYRECBATCH(historyrec) = 5;
    int DEFINEDATAREC(datadef) = 6;
    int WRITEHISTORYREC(historyrec) = 7;
    int WRITEDATAREC_LONG(datarec_long) = 8;
    void WRITEDATARECBATCH_LONG(datarec_long) = 9;
    int CLOSEFILES(void) = 10;
    void SHUTDOWN(void) = 11;
    int SYNCFILES(void) = 12;
  } = 1;
} = 0x20000004;
