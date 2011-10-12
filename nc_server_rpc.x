/*              Copyright (C) by UCAR
 *
 * File       : $RCSfile: nc_server_rpc.x,v $
 * Revision   : $Revision: 1.9 $
 * Directory  : $Source: /code/cvs/pam/isff/src/lib/nc_server_rpc.x,v $
 * System     : PAM
 * Date       : $Date: 2004/10/27 23:54:51 $
 *
 * Description:
 */

enum NS_rectype {
    NS_TIMESERIES
};

/* It ain't no accident that these match NC_INT and NC_FLOAT in netcdf.h */
enum NS_datatype { NS_INT=4, NS_FLOAT=5 };

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
        int WRITEDATAREC_INT(datarec_int) = 8;
        void WRITEDATARECBATCH_INT(datarec_int) = 9;
        int CLOSEFILES(void) = 10;
        void SHUTDOWN(void) = 11;
        int SYNCFILES(void) = 12;
        string CHECKERROR(int id) = 13;
    } = 1;
} = 0x20000004;
