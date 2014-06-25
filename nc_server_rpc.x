/*              Copyright (C) by UCAR
 * Description:
 *  XDR declarations for nc_server data and procedures.
 */

enum NS_rectype {
    NS_TIMESERIES
};

/* It ain't no accident that these match NC_INT and NC_FLOAT in netcdf.h */
enum NS_datatype { NS_INT=4, NS_FLOAT=5 };

/*
   strings within structs must be declared with <> identifiers.
   If a procedure returns a string, it is not declared with <>, but there
   doesn't seem to be any documentation on this difference.
 */

/**
  * A NetCDF attribute.
  */
struct str_attr {
    string name<>;
    string value<>;
};

/**
  * A NetCDF dimension.
  */
struct dimension {
    string name<>;
    int size;
};

/**
  * A NetCDF variable
  */
struct variable {
    string name<>;
    string units<>;
    str_attr attrs<>;
};

/**
  * Definition of a data record sent to nc_server. All variables in the
  * record will have the same dimension and fill value.
  */
struct datadef {
    double interval;
    int connectionId;
    NS_rectype rectype;
    NS_datatype datatype;
    variable variables<>;
    dimension dimensions<>;
    float floatFill;
    int intFill;
    bool fillmissingrecords;
};

struct datarec_float {
    double time;
    float data<>;
    int connectionId;
    int datarecId;
    int cnts<>;
    int start<>;
    int count<>;
};

struct datarec_int {
    double time;
    int data<>;
    int connectionId;
    int datarecId;
    int cnts<>;
    int start<>;
    int count<>;
};

/**
  * Global NetCDF string attribute, "history".
  */
struct history_attr {
    string history<>;
    int connectionId;
};

/**
  * Global NetCDF string attribute.
  */
struct global_attr {
    str_attr attr;
    int connectionId;
};

/**
  * Global NetCDF integer attribute.
  */
struct global_int_attr {
    string name<>;
    int value;
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
        int OPEN_CONNECTION(connection) = 1;

        int CLOSE_CONNECTION(int) = 2;

        int DEFINE_DATAREC(datadef) = 3;

        int WRITE_DATAREC_FLOAT(datarec_float) = 4;

        void WRITE_DATAREC_BATCH_FLOAT(datarec_float) = 5;

        int WRITE_DATAREC_INT(datarec_int) = 6;

        void WRITE_DATAREC_BATCH_INT(datarec_int) = 7;

        int WRITE_HISTORY(history_attr) = 8;

        void WRITE_HISTORY_BATCH(history_attr) = 9;

        int WRITE_GLOBAL_ATTR(global_attr) = 10;

        int WRITE_GLOBAL_INT_ATTR(global_int_attr) = 11;

        int CLOSE_FILES(void) = 12;

        void SHUTDOWN(void) = 13;

        int SYNC_FILES(void) = 14;

        string CHECK_ERROR(int id) = 15;
    } = 2;
} = 0x20000004;
