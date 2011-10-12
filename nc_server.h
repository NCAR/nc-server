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
#include <netcdf.hh>
#include <netcdf.h>

#include "nc_server_rpc.h"
#include <nidas/util/UTime.h>
#include <nidas/util/Logger.h>
#include <nidas/util/Exception.h>

#ifndef NC_SERVER_H
#define NC_SERVER_H

extern NcError ncerror;

/**
 * If ncerror.get_err() is non-zero return NetCDF error string nc_strerror(),
 * otherwise return system error string strerror_r(errno,...)
 */
namespace {
    std::string get_error_string()
    {
        if (ncerror.get_err()) return nc_strerror(ncerror.get_err());
        std::vector<char> buf(128);
        return strerror_r(errno,&buf.front(),buf.size());
    }
}

void nc_shutdown(int);

class Connection;
class FileGroup;
class VariableGroup;
class NS_NcFile;
class NS_NcVar;
class Variable;
class OutVariable;

class NcServerApp
{
public:

    NcServerApp();

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
    void run();

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

    int _logLevel;

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
    int openConnection(const struct connection *);

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
    int _connectionId;
    static Connections *_instance;
protected:
    Connections(void);
    ~Connections(void);
};

class Connection
{
public:
    Connection(const struct connection *,int id);
    ~Connection(void);

    int getId() const { return _id; }

    static std::string getIdStr(int id);

    int put_rec(const datarec_float * writerec) throw();

    int put_rec(const datarec_int * writerec) throw();

    int put_history(const std::string &);

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

    class InvalidOutputDir
    {
    };                          // exception class

    enum state {CONN_OK, CONN_ERROR };

    enum state getState() const
    {
        return _state;
    }

    const std::string& getErrorMsg() const
    {
        return _errorMsg;
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

};

class AllFiles
{
public:
    static AllFiles *Instance();
    FileGroup *get_file_group(const struct connection *);
    void close();
    void sync();
    void close_old_files(void);
    void close_oldest_file(void);
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

class NS_NcFile: public NcFile
{
public:
    /**
     * if _interval is less than minInterval (most likely 0)
     * then the ttType is VARIABLE_DELTAT.
     */
    static const double minInterval;

    NS_NcFile(const std::string &, enum FileMode, double, double, double)
        throw(NetCDFAccessFailed);
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

    inline int StartTimeLE(double time) const
    {
#ifdef DEBUG
        DLOG(("") << "start=" << nidas::util::UTime(_startTime) <<
                ", end=" << nidas::util::UTime(_endTime) <<
                ", current=" nidas::util::UTime(time));
#endif
        return (_startTime <= time);
    };
    inline int EndTimeLE(double time) const
    {
#ifdef DEBUG
        DLOG(("") << "start=" << nidas::util::UTime(_startTime) <<
                ", end=" << nidas::util::UTime(_endTime) <<
                ", current=" nidas::util::UTime(time));
#endif
        return (_endTime <= time);
    }
    inline int EndTimeGT(double time) const
    {
#ifdef DEBUG
        DLOG(("") << "start=" << nidas::util::UTime(_startTime) <<
                ", end=" << nidas::util::UTime(_endTime) <<
                ", current=" nidas::util::UTime(time));
#endif
        return (_endTime > time);
    }

    template<class REC_T, class DATA_T>
        void put_rec(const REC_T * writerec, VariableGroup *,double dtime)
            throw(NetCDFAccessFailed);

    long put_time(double) throw(NetCDFAccessFailed);;
    int put_history(std::string history);
    time_t LastAccess() const
    {
        return _lastAccess;
    }
    NcBool sync(void);

    /**
     * Check validity of a counts variable name for this file.
     * @returns:
     *   false 
     *      A variable exists with that name with wrong (non-timeseries) dimensions
     *      The name matches a counts attribute of one or more existing file
     *      variables which are not in the VariableGroup.
     *  true otherwise
     */
    bool checkCountsVariableName(const std::string& name, VariableGroup*)
        throw(NetCDFAccessFailed);

    /**
     * Determine a counts attribute for a VariableGroup, given the
     * current counts attributes found in the file for the group.
     */
    std::string resolveCountsName(const std::set<std::string>& attrs, VariableGroup*);

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
    char _monthLong;            //

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

    const std::vector<NS_NcVar*>& get_vars(VariableGroup *) throw(NetCDFAccessFailed);

    /**
     * Add a variable to the NS_NcFile. Set modified to true if
     * the NcFile was modified.
     */
    NS_NcVar *add_var(OutVariable * v,bool & modified) throw(NetCDFAccessFailed);;

    NcVar *find_var(OutVariable *) throw(NetCDFAccessFailed);

    /**
     * Add an attribute to the NS_NcFile. Return true if
     * the NcFile was modified.
     */
    bool add_attrs(OutVariable * v, NS_NcVar * var,const std::string& countsAttr)
        throw(NetCDFAccessFailed);

    NcBool check_var_dims(NcVar *);
    const NcDim *get_dim(NcToken name, long size);

    NS_NcFile(const NS_NcFile &);       // prevent copying
    NS_NcFile & operator=(const NS_NcFile &);   // prevent assignment

};

// A file group is a list of similarly named files with the same
// time series data interval and length
class FileGroup
{
public:

    FileGroup(const struct connection *);
    ~FileGroup(void);

    template<class REC_T, class DATA_T>
        NS_NcFile* put_rec(const REC_T * writerec, NS_NcFile * f) throw(nidas::util::Exception);

    int match(const std::string & dir, const std::string & file);
    NS_NcFile *get_file(double time) throw(NetCDFAccessFailed);
    NS_NcFile *open_file(double time) throw(NetCDFAccessFailed);
    void close();
    void sync();
    void close_old_files(void);
    void close_oldest_file(void);
    void add_connection(Connection *);
    void remove_connection(Connection *);

    /**
     * @return: non-negative group id or -1 on error.
     */
    int add_var_group(const struct datadef *) throw();

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
            const std::string & nameFormat, double dtime,
            double fileLength) const;
    int check_file(const std::string &) const;
    int ncgen_file(const std::string &, const std::string &) const;

    class InvalidInterval
    {
    };
    class InvalidFileLength
    {
    };

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

};

class VariableGroup
{
public:
    VariableGroup(const struct datadef *, int n, double finterval)
        throw(BadVariable);
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

    int same_var_group(const struct datadef *) const;

    int similar_var_group(const VariableGroup *) const;

    const char *suffix() const;

    void create_outvariables(void);

    void createCountsVariable(const std::string& name);

    const std::string& getCountsName() const
    {
        return _countsName;
    }

    void setCountsName(const std::string& val);

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
        return _outvars.size();
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

private:
    /** name for use in log messages */
    std::string _name;

    double _interval;

    std::vector <Variable *> _invars;

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

    void check_counts_variable()
        throw(BadVariable);


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
    NcAtt *get_att(NcToken attname) const
    {
        return _var->get_att(attname);
    }
    NcBool add_att(NcToken attname, const char *v) const
    {
        return _var->add_att(attname, v);
    }

    NcBool add_att(NcToken attname, int v) const
    {
        return _var->add_att(attname, v);
    }

    NcBool add_att(NcToken attname, float v) const
    {
        return _var->add_att(attname, v);
    }


    /**
     * Set an attribute on the NS_NcVar. Return true if
     * the NcFile was modified.
     */
    bool set_att(const std::string& name, const std::string& val) throw(NetCDFAccessFailed);

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

};

class Variable
{
public:
    Variable(const std::string &n);

    Variable(const Variable &);

    virtual ~ Variable(void) {}

    bool &isCnts()
    {
        return _isCnts;
    }
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

    /**
     * If this variable is referenced as a counts variable by other variables.
     */
    bool _isCnts;

    std::map <std::string, std::string> _strAttrs;

    Variable & operator=(const Variable &);     // prevent assignment

};

class OutVariable:public Variable
{
public:
    OutVariable(const Variable &, NS_datatype, float, int);

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
    NS_datatype _datatype;

    float _floatFill;

    int _intFill;

    OutVariable & operator=(const OutVariable &);       // prevent assignment

    OutVariable(const OutVariable &);   // prevent copying

};

template<class REC_T,class DATA_T>
NS_NcFile *FileGroup::put_rec(const REC_T * writerec,
        NS_NcFile * f) throw(nidas::util::Exception)
{
    int groupid = writerec->datarecId;
    double dtime = writerec->time;

    if (_vargroups.find(groupid) == _vargroups.end()) {
        std::string idstr = Connection::getIdStr(writerec->connectionId);
        std::ostringstream ost;
        ost << "Invalid variable group number: " <<  groupid;
        throw NcServerAccessFailed(idstr,"put_rec",ost.str());
    }

    /* Check if last file is still current */
    if (!(f && (f->StartTimeLE(dtime) && f->EndTimeGT(dtime)))) {
#ifdef DEBUG
        DLOG(("time not contained in current file"));
#endif
        if (f)
            f->sync();
        try {
            f = get_file(dtime);
        }
        catch(const NetCDFAccessFailed& e) {
            // Too many files open
            if (ncerror.get_err() == NC_ENFILE) {
                AllFiles::Instance()->close_oldest_file();
                f = get_file(dtime);
            }
            else throw e;
        }
    }
#ifdef DEBUG
    DLOG(("Writing Record, groupid=%d,f=%s", groupid, f->getName().c_str()));
#endif

    f->put_rec<REC_T,DATA_T>(writerec, _vargroups[groupid], dtime);
    return f;
}

template<class REC_T, class DATA_T>
void NS_NcFile::put_rec(const REC_T * writerec,
        VariableGroup * vgroup, double dtime) throw(NetCDFAccessFailed)
{
    long nrec;
    long nsample = 0;
    NS_NcVar *var;
    int i, iv, nv;
    double groupInt = vgroup->interval();
    double tdiff;
    time_t tnow;
    int ndims_req = vgroup->num_dims();

    // this will add variables if necessary
#ifdef DEBUG
    DLOG(("calling get_vars"));
#endif

    const std::vector<NS_NcVar*>& vars = get_vars(vgroup);

#ifdef DEBUG
    DLOG(("called get_vars"));
    nidas::util::UTime debugUT(dtime);
#endif

    dtime -= _baseTime;
    if (_ttType == FIXED_DELTAT) {
        /* Examples:
         * times fall on even intervals
         * _interval = 1 sec (time interval of the file)
         *     times in files will be 00:00:00, 00:00:01, etc
         * groupInt = 1/60 
         * dtime = 00:00:01.0167   Not midpoint of _interval/dim
         * nsample = 1
         * tdiff = nsample * groupInt
         * dtime = 00:00:01.0
         * dtime[nsample] = dtime + nsample * (_interval / dim)
         *****************************************************************
         * times are midpoints of intervals
         * _interval = 300 sec
         *      values will have timetags of 00:02:30, 00:07:30, etc
         * groupInt = 60 sec
         *      values will have timetags of 00:00:30, 00:01:30, etc
         * dtime = 00:01:30     midpoint of _interval/dim
         * nsample = 1
         * tdiff = 90 - 150 = -60
         * dtime = 00:02:30
         * Reading:
         * dtime[nsample] = dtime - _interval/2 + nsample * groupInt + groupInt/2.
         * How does reader know _interval and groupInt?
         * Can store _interval as attribute of time. calculate groupInt = _interval/dim
         * attribute:  "interval(secs)"  "300"
         * attribute:  "resolution(secs)"  "300"
         * attribute:  "sampleInterval(secs)"  "300"
         */

        if (_timesAreMidpoints < 0) {
            _timesAreMidpoints = fabs(fmod(dtime, groupInt) - groupInt * .5) <
                groupInt * 1.e-3;
            if (_timesAreMidpoints) _timeOffset = -_interval * .5;
            else _timeOffset = -_interval;

            // #define DEBUG
#ifdef DEBUG
            DLOG(("dtime=") << dtime << " groupInt=" << groupInt <<
                    " timesAreMidpoints=" << _timesAreMidpoints);
#endif
            // #undef DEBUG
        }
        if (vgroup->num_samples() > 1) {
            if (_timesAreMidpoints) {
                nsample = (long) floor(fmod(dtime, _interval) / groupInt);
                tdiff = (nsample + .5) * groupInt - .5 * _interval;
            }
            else {
                nsample = (long) floor(fmod(dtime + groupInt/2, _interval) / groupInt);
                tdiff = nsample * groupInt;
            }

#ifdef DEBUG
            DLOG(("dtime=") << dtime << " nsample=" << nsample <<
                    " tdiff=" << tdiff);
#endif
            dtime -= tdiff;
        }
    }
#ifdef DEBUG
    DLOG(("NS_NcFile::put_rec, ut=") <<
            debugUT.format(true,"%Y %m %d %H:%M:%S.%3f"));
#endif

    nrec = put_time(dtime);

    nv = vars.size();

    int nstart = writerec->start.start_len;
    int ncount = writerec->count.count_len;
    long *start = (long *) writerec->start.start_val;
    long *count = (long *) writerec->count.count_val;
    const DATA_T *d = writerec->data.data_val;
    int nd = writerec->data.data_len;
    const DATA_T *dend = d + nd;

    if (nstart != ndims_req - 2 || nstart != ncount) {
        std::ostringstream ost;
        ost << vars[0]->name() << ": has incorrect start or count length";
        throw NetCDFAccessFailed(getName(),"put_rec",ost.str());
    }
#ifdef DEBUG
    DLOG(("nstart=%d,ncount=%d", nstart, ncount));
    for (i = 0; i < ncount; i++)
        DLOG(("count[%d]=%d", i, count[i]));
#endif

    for (iv = 0; iv < nv; iv++) {
        var = vars[iv];
        var->set_cur(nrec, nsample, start);
        if (var->isCnts()) {
            if (writerec->cnts.cnts_len > 0) {
#ifdef DEBUG
                DLOG(("put counts"));
#endif
                if (!var->put((const int *) writerec->cnts.cnts_val, count))
                    throw NetCDFAccessFailed(getName(),std::string("put_var ") + var->name(),get_error_string());
            }
        } else {
            if (d >= dend) {
                std::ostringstream ost;
                ost << var->name() << ": data array has " << nd << " values, num_variables=" << nv;
                throw NetCDFAccessFailed(getName(),"put_rec",ost.str());
            }
            else {
                if (!(i = var->put(d, count)))
                    throw NetCDFAccessFailed(getName(),std::string("put_var ") + var->name(),get_error_string());
#ifdef DEBUG
                DLOG(("var->put of %s, i=%d", var->name(), i));
#endif
                d += i;
            }
        }
    }
    if (d != dend) {
        std::ostringstream ost;
        ost << vars[0]->name() << ": data array has " << nd << " values, but only " <<
            (unsigned long)(d - writerec->data.data_val) << " written, num_variables=" << nv;
        throw NetCDFAccessFailed(getName(),"put_rec",ost.str());
    }

    if ((tnow = time(0)) - _lastSync > SYNC_CHECK_INTERVAL_SECS) sync();
    _lastAccess = tnow;
}

#endif
