// -*- mode: C++; indent-tabs-mode: nil; c-basic-offset: 4; tab-width: 4; -*-
// vim: set shiftwidth=4 softtabstop=4 expandtab:
/*
 ********************************************************************
 Copyright 2005 UCAR, NCAR, All Rights Reserved

 $LastChangedDate$

 $LastChangedRevision$

 $LastChangedBy$

 $HeadURL$
 ********************************************************************

 NetCDF file server.  Implemented with remote procedure calls (RPC).
 It only supports file writing, not reading.

 A theoretically unlimited number of clients can request a connection
 to this server.  Upon connection, the server returns a unique integer
 connectionId to each client, which the client must use in each
 RPC call to the server to indicate which client is calling.

 After connection, the clients make one or more RPC
 calls to tell the server what kind of data records they will be
 sending, what the time resolution is, what the names of the
 data variables are, and strings indicating the data units,
 what format to use for the NetCDF file names, and the time length
 of the NetCDF files.

 Once the NetCDF variables are created, the client simply makes
 successive RPC calls to write data records.

*/

#include <time.h>
#include <signal.h>
#include <vector>
#include <string>
#include <list>
#include <map>
#include <set>
#include <memory>
#include <utility>
#include <netcdf.hh>
#include <netcdf.h>

#include "nc_server_rpc.h"
#include <nidas/util/UTime.h>
#include <nidas/util/Logger.h>
#include <nidas/util/Exception.h>

#ifndef NC_SERVER_H
#define NC_SERVER_H

#define DEFAULT_RPC_PORT 30005

extern NcError ncerror;

void nc_shutdown(int);

class Connection;
class FileGroup;
class VariableGroup;
class NS_NcFile;
class NS_NcVar;
class Variable;
class OutVariable;

class InvalidInterval: public nidas::util::Exception
{
public:
    InvalidInterval(const std::string& msg): nidas::util::Exception(msg)
        {}
};

class InvalidFileLength: public nidas::util::Exception
{
public:
    InvalidFileLength(const std::string& msg): nidas::util::Exception(msg)
        {}
};

class InvalidOutputDir: public nidas::util::Exception
{
public:
    InvalidOutputDir(const std::string& msg, const std::string& op, int ierr):
        nidas::util::Exception(msg, op, ierr)
        {}
};


class NcServerApp
{
public:

    NcServerApp();

    ~NcServerApp();

    /**
     * Parse the runstring parameters.
     * If the runstring parameters are not OK, then DSMEngine::usage()
     * is called to print a message to stderr, and this method
     * then returns a error value of 1.
     * @return 0: OK, 1: failure.
     */
    void usage(const char *argv0);

    int parseRunstring(int argc, char **argv);

    void setup();

    /** main loop */
    int run();

    static void setupSignals();

    uid_t getUserID()
    {
        return _userid;
    }

    uid_t getGroupID()
    {
        return _groupid;
    }

private:

    /** Signal handler */
    static void sigAction(int sig, siginfo_t * siginfo, void *vptr);

    std::string _username;

    uid_t _userid;

    std::string _groupname;

    gid_t _groupid;

    std::list<std::string> _suppGroupNames;

    std::vector<gid_t> _suppGroupIds;

    int _daemon;

    std::string _logConfig;

    int _rpcport;

    bool _standalone;

    SVCXPRT* _transp;

    /** No copying */
    NcServerApp(const NcServerApp&);

    /** No assignment */
    NcServerApp& operator=(const NcServerApp&);

};

class BadVariable: public nidas::util::Exception
{
public:
    BadVariable(const std::string& n):
        nidas::util::Exception("BadVariable", n)
    {
    }
};

class NetCDFAccessFailed: public nidas::util::Exception
{
public:
    NetCDFAccessFailed(const std::string& file,const std::string& operation,const std::string& msg):
        nidas::util::Exception("NetCDFAccessFailed",file + ": " + operation + ": " + msg)
    {
    }
    NetCDFAccessFailed(const std::string& msg):
        nidas::util::Exception("NetCDFAccessFailed",msg)
    {
    }
};

class NcServerAccessFailed: public nidas::util::Exception
{
public:
    NcServerAccessFailed(const std::string& connstr,const std::string& op, const std::string& msg):
        nidas::util::Exception("NcServerAccessFailed",connstr + ": " + op + ": " + msg)
    {
    }
};

class Connections
{
public:
    static Connections *Instance();

    static const int CONNECTIONTIMEOUT;
    
    /**
     * Open a connection and return a positive connection id
     * handle. If return value is negative an error occurred.
     */
    int openConnection(const struct connection *) throw();

    /**
     * Close a connection, given the id. Return 0 on success, -1 if
     * the connection is not found.
     */
    int closeConnection(int);

    void closeOldConnections();

    Connection * operator[] (int) const;

    unsigned int num() const;
private:
    std::map <int, Connection*> _connections;
    int _connectionCntr;
    static Connections *_instance;
protected:
    Connections(void);
    ~Connections(void);
};

class Connection
{
public:
    /**
     * @brief Construct a new Connection object
     * @throws InvalidFileLength, InvalidInterval, InvalidOutputDir
     */
    Connection(const struct connection *, int id);

    ~Connection(void);

    int getId() const { return _id; }

    static std::string getIdStr(int id);

    int put_rec(const datarec_float * writerec) throw();

    int put_rec(const datarec_int * writerec) throw();

    int put_history(const std::string &) throw();

    int write_global_attr(const std::string & name, const std::string& value) throw();

    int write_global_attr(const std::string & name, int value) throw();

    /**
     * @return: non-negative group id or -1 on error.
     */
    int add_var_group(const struct datadef *) throw();

    time_t LastRequest()
    {
        return _lastRequest;
    }

    const std::string & get_history()
    {
        return _history;
    }

    NS_NcFile *last_file() const;

    void unset_last_file();     // the file has been closed

    enum state {CONN_OK, CONN_ERROR };

    enum state getState() const
    {
        return _state;
    }

    const std::string& getErrorMsg() const
    {
        return _errorMsg;
    }

    void setErrorMsg(const std::string& val)
    {
        _errorMsg = val;
    }

private:
    FileGroup *_filegroup;

    std::string _history;

    int _histlen;

    NS_NcFile *_lastf;          // last file written to, saved for efficiency
    time_t _lastRequest;
    int _id;

    std::string _errorMsg;

    enum state _state;

    Connection(const Connection&);
    Connection& operator=(const Connection&);

};

class AllFiles
{
public:
    static AllFiles *Instance();

    /**
     * @throws InvalidFileLength, InvalidInterval, InvalidOutputDir
     */
    FileGroup *get_file_group(const struct connection *);
    void close() throw();
    void sync() throw();
    void close_old_files(void) throw();
    void close_oldest_file(void) throw();
    int num_files(void) const;

    static void hangup(int sig);
    static void shutdown(int sig);
private:
    std::vector < FileGroup*> _filegroups;
    AllFiles(const AllFiles &); // prevent copying
    AllFiles & operator=(const AllFiles &);     // prevent assignment
    static AllFiles *_instance;
protected:
    AllFiles(void);
    ~AllFiles(void);
};


/**
 * Wrap the NcAtt pointer returned by netcdf in a unique_ptr.
 * 
 * This is a template since the implementation is the same for anything with a
 * get_att() method, both NcTypedComponent and NS_NcVar, and it works whether
 * the get_att() method returns NcAtt* or std::unique_ptr<NcAtt>.
 */
template <typename NCT>
std::unique_ptr<NcAtt>
get_att_unique(NCT* nct, const std::string& attname)
{
    return std::unique_ptr<NcAtt>{nct->get_att(attname.c_str())};
}


class NS_NcFile: public NcFile
{
public:
    using UTime = nidas::util::UTime;

    /**
     * if _interval is less than minInterval (most likely 0)
     * then the ttType is VARIABLE_DELTAT.
     */
    static const double minInterval;

    /**
     * @throws NetCDFAccessFailed
     */
    NS_NcFile(const std::string &, enum FileMode,
              double interval, double filelength,
              const UTime& basetime, const UTime& endtime);

    ~NS_NcFile(void);

    const std::string & getName() const;

    int operator <(const NS_NcFile & x)
    {
        return _startTime < x._startTime;
    }
    friend int operator <(const NS_NcFile & x, const NS_NcFile & y)
    {
        return x._startTime < y._startTime;
    }

    inline int StartTimeLE(double time) const;

    inline int EndTimeLE(double time) const;

    inline int EndTimeGT(double time) const;

    /**
     * @throws NetCDFAccessFailed
     */
    template<class REC_T, class DATA_T>
        void put_rec(const REC_T * writerec, VariableGroup *,double dtime);

    /**
     * @throws NetCDFAccessFailed
     */
    long put_time(double);

    std::unique_ptr<NcAtt>
    get_att(const std::string& attname) const
    {
        return std::unique_ptr<NcAtt>{NcFile::get_att(attname.c_str())};
    }

    NcBool add_att(const std::string& attname, const std::string& v)
    {
        return NcFile::add_att(attname.c_str(), v.c_str());
    }

    NcBool add_att(const std::string& attname, int v)
    {
        return NcFile::add_att(attname.c_str(), v);
    }

    /**
     * @throws NetCDFAccessFailed
     */
    void put_history(std::string history);

    /**
     * @throws NetCDFAccessFailed
     */
    void write_global_attr(const std::string& name, const std::string& val);

    /**
     * @throws NetCDFAccessFailed
     */
    void write_global_attr(const std::string& name, int val);

    time_t LastAccess() const
    {
        return _lastAccess;
    }
    NcBool sync(void) throw();

    std::string getCountsName(VariableGroup* vg);

    /**
     * Check validity of a counts variable name for this file.
     * @throws NetCDFAccessFailed
     * @returns:
     *   false 
     *      A variable exists with that name with wrong (non-timeseries) dimensions
     *      The name matches a counts attribute of one or more existing file
     *      variables which are not in the VariableGroup.
     *  true otherwise
     */
    bool checkCountsVariableName(const std::string& name, VariableGroup*);

    /**
     * Determine a counts attribute for a VariableGroup, given the
     * current counts attributes found in the file for the group.
     */
    std::string resolveCountsName(const std::string& cntsNameInFile, VariableGroup* vgroup);

    /**
     * Utility function to create a new name by appending and
     * underscore and an integer value to the end.
     */
    std::string createNewName(const std::string& name,int & i);

    /**
     * When writing time-series records, how frequently to check
     * to sync the NetCDF file.
     */
    static const int SYNC_CHECK_INTERVAL_SECS = 5;

private:
    std::string _fileName;
    double _startTime, _endTime;
    double _interval;
    double _lengthSecs;
    double _timeOffset;         // last timeOffset written
    NcType _timeOffsetType;     // ncFloat or ncDouble
    bool _monthLong;            //

    // time tag type, fixed dt or variable?
    enum { FIXED_DELTAT, VARIABLE_DELTAT } _ttType;

    int _timesAreMidpoints;

    NcVar *_baseTimeVar;
    NcVar *_timeOffsetVar;

    /**
     * for each variable group, a vector of variables in this file
     */
    std::map <int,std::vector<NS_NcVar*> > _vars;

    NcDim *_recdim;
    int _baseTime;
    long _nrecs;

    std::vector<std::string> _dimNames;     // names of requested dimensions

    std::vector<long> _dimSizes;            // sizes of requested dimensions

    std::vector<int> _dimIndices;           // position of requested dimensions in variable

    unsigned int _ndims;        // number of dimensions of size > 1

    std::vector<const NcDim *> _dims;        // dimensions to use when creating new vars

    int _ndims_req;             // number of requested dimensions

    time_t _lastAccess;

    time_t _lastSync;

    std::string _historyHeader;

    /**
     * @brief Get variables.
     * 
     * @throws NetCDFAccessFailed
     * 
     * @return const std::vector<NS_NcVar*>& 
     */
    const std::vector<NS_NcVar*>& get_vars(VariableGroup *);

    /**
     * Add a variable to the NS_NcFile. Set modified to true if
     * the NcFile was modified.
     *
     * @throws NetCDFAccessFailed
     */
    NS_NcVar *add_var(OutVariable * v,bool & modified);

    /**
     * @throws NetCDFAccessFailed
     */
    NcVar *find_var(OutVariable *);

    /**
     * Add attributes to the NS_NcFile. Return true if
     * the NcFile was modified.
     * 
     * @throws NetCDFAccessFailed
     */
    bool add_attrs(OutVariable * v, NS_NcVar * var,const std::string& countsAttr);

    bool check_var_dims(NcVar *);
    const NcDim *get_dim(NcToken name, long size);

    std::map <int,std::string> _countsNamesByVGId;

    NS_NcFile(const NS_NcFile &);       // prevent copying
    NS_NcFile & operator=(const NS_NcFile &);   // prevent assignment

};

// A file group is a list of similarly named files with the same
// time series data interval and length
class FileGroup
{
public:
    using UTime = nidas::util::UTime;

    /**
     * @brief Construct a new FileGroup object
     * 
     * @throws InvalidOutputDir
     */
    FileGroup(const struct connection *);
    ~FileGroup(void);

    /**
     * @throws nidas::util::Exception
     * @return NS_NcFile* 
     */
    template<class REC_T, class DATA_T>
        NS_NcFile* put_rec(const REC_T * writerec, NS_NcFile * f);

    int match(const std::string & dir, const std::string & file);
    /**
     * @brief Get the file object
     * @throws NetCDFAccessFailed
     * @param time 
     * @return NS_NcFile* 
     */
    NS_NcFile *get_file(double time);

    /**
     * @throws NetCDFAccessFailed
     * @param time 
     * @return NS_NcFile* 
     */
    NS_NcFile *open_file(double time);

    void close() throw();
    void sync() throw();
    void close_old_files(void) throw();
    void close_oldest_file(void) throw();
    void add_connection(Connection *);
    void remove_connection(Connection *);

    /**
     * @return: non-negative group id
     * @throws BadVariable
     */
    int add_var_group(const struct datadef *);

    double interval() const
    {
        return _interval;
    }
    double length() const
    {
        return _fileLength;
    }
    int active() const
    {
        return _connections.size() > 0;
    }
    int num_files() const
    {
        return _files.size();
    }
    int num_var_groups() const
    {
        return _vargroups.size();
    }
    time_t oldest_file();

    std::string build_name(const std::string & outputDir,
            const std::string & nameFormat,
            double fileLength, const UTime& basetime) const;
    int check_file(const std::string &) const;
    int ncgen_file(const std::string &, const std::string &) const;

    /**
     * @throws NetCDFAccessFailed
     **/
    void write_global_attr(const std::string & name, const std::string& value);

    /**
     * @throws NetCDFAccessFailed
     */
    void write_global_attr(const std::string & name, int value);

    /**
     * @throws NetCDFAccessFailed
     */
    void update_global_attrs();

    std::string toString() const {
        return _outputDir + '/' + _fileNameFormat;
    }

    /**
     * Given a data time, find the file times which bound it.  The file times
     * depend on file length and config bounds.  A monthly interval is
     * specified as 31 days, in which case file times are bounded by the first
     * day of the month.
     **/
    void
    get_time_bounds(double dtime, UTime& basetime, UTime& endtime) const;

    /**
     * Set @p begin and @p end from the isfs_config_begin and isfs_config_end
     * global attributes, if present, otherwise return false.
     */
    bool get_config_bounds(UTime& begin, UTime& end) const;

private:

    std::vector < Connection * >_connections;

    std::list < NS_NcFile * >_files;    // List of files in this group

    static const int FILEACCESSTIMEOUT;
    static const int MAX_FILES_OPEN;

    FileGroup(const FileGroup &);       // prevent copying
    FileGroup & operator=(const FileGroup &);   // prevent assignment

    std::string _outputDir;
    std::string _fileNameFormat;
    std::string _CDLFileName;
    std::map <int, VariableGroup*> _vargroups;
    int _vargroupId;
    double _interval;
    double _fileLength;

    std::map<std::string,std::string> _globalAttrs;

    std::map<std::string,int> _globalIntAttrs;

};

class VariableGroup
{
public:
    /**
     * @throws BadVariable
     */
    VariableGroup(const struct datadef *, int n, double finterval);

    ~VariableGroup(void);

    /**
     * A name for this group for log messages.
     */
    const std::string& getName() const
    {
        return _name;
    }

    int getId() const
    {
        return _id;
    }

    OutVariable *get_var(int n) const
    {
        return _outvars[n];
    }

    bool same_var_group(const struct datadef *) const;

    const char *suffix() const;

    void createCountsVariable(const std::string& name);

    const std::string& getCountsName() const
    {
        return _countsName;
    }

    double interval() const
    {
        return _interval;
    }
    int num_samples() const
    {
        return _nsamples;
    }
    int num_vars() const
    {
        return _vars.size();
    }
    NS_rectype rec_type() const
    {
        return _rectype;
    }
    NS_datatype data_type() const
    {
        return _datatype;
    }

    int NumCombTrivar(int n) const;

    int num_dims() const;

    long dim_size(unsigned int i) const;

    const std::string&  dim_name(unsigned int i) const;

    float floatFill() const
    {
        return _floatFill;
    }
    int intFill() const
    {
        return _intFill;
    }

    /**
     * @throws BadVariable
     */
    void check_counts_variable();

private:
    /** name for use in log messages */
    std::string _name;

    double _interval;

    std::vector <Variable *> _vars;

    std::vector <OutVariable *> _outvars;

    unsigned int _ndims;        // number of dimensions

    std::vector<long> _dimsizes;            // dimension sizes

    std::vector<std::string> _dimnames; // dimension names

    int _nsamples;              // number of samples per time record

    int _nprefixes;

    NS_rectype _rectype;

    NS_datatype _datatype;

    int _fillMissing;

    float _floatFill;

    int _intFill;

    int _id;

    /**
     * Name of counts variable, which is the unique value
     * of any counts attributes of the variables.
     */
    std::string _countsName;

    VariableGroup(const VariableGroup &);       // prevent copying

    VariableGroup & operator=(const VariableGroup &);   // prevent assignment
};

//
class NS_NcVar
{
public:
    NS_NcVar(NcVar *, int *dimIndices, int ndims_group, float ffill,
            int lfill, bool isCnts = false);
    ~NS_NcVar();
    NcVar *var() const
    {
        return _var;
    }
    NcBool set_cur(long, int, const long *);
    int put(const float *d, const long *);
    int put(const int * d, const long *);
    int put_len(const long *);
    const char *name() const
    {
        return _var->name();
    }

    std::unique_ptr<NcAtt>
    get_att(const std::string& attname) const
    {
        std::unique_ptr<NcAtt> att(_var->get_att(attname.c_str()));
        return att;
    }

    NcBool add_att(const std::string& attname, const std::string& v) const
    {
        return _var->add_att(attname.c_str(), v.c_str());
    }

    NcBool add_att(const std::string& attname, int v) const
    {
        return _var->add_att(attname.c_str(), v);
    }

    NcBool add_att(const std::string& attname, float v) const
    {
        return _var->add_att(attname.c_str(), v);
    }

    /**
     * Set an attribute on the NS_NcVar. Return true if
     * the NcFile was modified.
     * 
     * @throws NetCDFAccessFailed
     */
    bool set_att(const std::string& name, const std::string& val);

    bool &isCnts()
    {
        return _isCnts;
    }
    float floatFill() const
    {
        return _floatFill;
    }
    int intFill() const
    {
        return _intFill;
    }

private:
    NcVar *_var;
    /**
     * for requested dimensions, their position in the NetCDF variable's dimensions
     */
    int *_dimIndices;

    /**
     * length of _dimIndices. This is 2 + number of requested dimensions
     */
    int _ndimIndices;

    long *_start;
    long *_count;
    bool _isCnts;
    float _floatFill;
    int _intFill;

    NS_NcVar(const NS_NcVar&);
    NS_NcVar& operator=(const NS_NcVar&);

};

/**
 * Requested variable.
 */
class Variable
{
public:
    Variable(const std::string& n);

    Variable(const Variable &);

    virtual ~ Variable(void) {}

    const std::string& name() const
    {
        return _name;
    }
    const std::string& units() const
    {
        return att_val("units");
    }

    void set_name(const std::string&);

    /**
     * If val is an empty string, the attribute is removed.
     */
    void add_att(const std::string& name, const std::string& val);

    std::vector<std::string> get_attr_names() const;

    const std::string& att_val(const std::string& n) const;

protected:
    std::string _name;

    std::map <std::string, std::string> _strAttrs;

    Variable & operator=(const Variable &);     // prevent assignment

};

/**
 * Requested variable, with named modified for compatibility with NetCDF,
 * short_name attribute added.
 */
class OutVariable: public Variable
{
public:
    OutVariable(const Variable&, NS_datatype, float, int);

    bool &isCnts()
    {
        return _isCnts;
    }

    NS_datatype data_type() const
    {
        return _datatype;
    }

    float floatFill() const
    {
        return _floatFill;
    }

    int intFill() const
    {
        return _intFill;
    }

private:

    /**
     * If this variable is referenced as a counts variable by other variables.
     */
    bool _isCnts;

    NS_datatype _datatype;

    float _floatFill;

    int _intFill;
};

#endif
