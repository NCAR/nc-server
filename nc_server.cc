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
 */

#include "nc_server.h"

#include <unistd.h>
#include <assert.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <rpc/pmap_clnt.h>
#include <pwd.h>
#include <grp.h>
#include <sys/prctl.h>

#include <netcdf.h>

#include <algorithm>
#include <set>
#include <iostream>

#include <nidas/util/Process.h>

extern "C"
{
    void netcdfserverprog_1(struct svc_req *rqstp,
            register SVCXPRT * transp);
}

using namespace std;

const int Connections::CONNECTIONTIMEOUT = 43200;
const int FileGroup::FILEACCESSTIMEOUT = 900;
const int FileGroup::MAX_FILES_OPEN = 16;

namespace {
    int defaultLogLevel = nidas::util::LOGGER_NOTICE;
};

/*
#define VARNAME_DEBUG
#define DEBUG
*/

unsigned long heap()
{
    pid_t pid = getpid();
    char procname[64];

#ifdef linux
    unsigned long vsize;

    sprintf(procname, "/proc/%d/stat", pid);
    FILE *fp = fopen(procname, "r");

    // do man proc on Linux
    if (fscanf
            (fp,
             "%*d (%*[^)]) %*c%*d%*d%*d%*d%*d%*u%*u%*u%*u%*u%*d%*d%*d%*d%*d%*d%*u%*u%*d%lu%*u%*u%*u%*u%*u%*u%*u%*d%*d%*d%*d%*u",
             &vsize) != 1)
        vsize = 0;
    fclose(fp);
    return vsize;
#else

    int fd;

    sprintf(procname, "/proc/%d", pid);
    if ((fd = open(procname, O_RDONLY)) < 0)
        return 0;

    // do "man -s 4 proc" on Solaris
    struct prstatus pstatus;
    if (ioctl(fd, PIOCSTATUS, &pstatus) < 0) {
        close(fd);
        return 0;
    }
    // struct prpsinfo pinfo;
    // if (ioctl(fd,PIOCPSINFO,&pinfo) < 0) { close(fd); return 0; }

    //  cout << pinfo.pr_pid << ' ' << pinfo.pr_size << ' ' << pinfo.pr_rssize << ' ' << pinfo.pr_bysize << ' ' << pinfo.pr_byrssize << ' ' << pstatus.pr_brksize << ' ' << pstatus.pr_stksize << endl;

    close(fd);
    return pstatus.pr_brksize;
#endif
}


NcError ncerror(NcError::silent_nonfatal);

void nc_shutdown(int i)
{
    if (i)
        PLOG(("nc_server exiting abnormally: exit(%d)", i));
    else
        ILOG(("nc_server normal exit", i));
    exit(i);
}

Connections::Connections(void): _connections(),_connectionCntr(0)
{
    srandom((unsigned int)time(0));
}

Connections *Connections::_instance = 0;

Connections *Connections::Instance()
{
    if (_instance == 0)
        _instance = new Connections;
    return _instance;
}

Connections::~Connections(void)
{
    map <int, Connection * >::iterator ci = _connections.begin();
    for ( ; ci != _connections.end(); ++ci) delete ci->second;
}

int Connections::openConnection(const struct connection *conn)
{
    Connection *cp = 0;

#ifdef DEBUG
    DLOG(("_connections.size()=%d", _connections.size()));
#endif

    closeOldConnections();

    // just in case this process ever handles over 2^31 connections :-)
    if (_connectionCntr < 0) _connectionCntr = 0;

    // The low order 2 bytes of the connection id are an integer
    // that increments for every new connection. The high order
    // 2 bytes are a random value, so that if this process is
    // restarted, it is unlikely that any pre-existing client will
    // have the same id as a new client.
    int id = (_connectionCntr++ & 0xffff) + (random() & 0xffff0000UL);
    try {
        cp = new Connection(conn,id);
    }
    catch(Connection::InvalidOutputDir) {
        delete cp;
        return -1;
    }
    _connections[id] = cp;
    ILOG(("Opened connection, id=%d, #connections=%zd, heap=%zd",
            (id & 0xffff),_connections.size(), heap()));
    return id;
}

int Connections::closeConnection(int id)
{
    map <int, Connection * >::iterator ci = _connections.find(id);
    if (ci != _connections.end()) {
        int id = ci->first;
        delete ci->second;
        _connections.erase(ci);
        ILOG(("Closed connection, id=%d, #connections=%zd",
            (id & 0xffff),_connections.size()));
        return 0;
    }
    return -1;
}

void Connections::closeOldConnections()
{
    time_t utime;
    /*
     * Release timed-out connections.
     */
    utime = time(0);

    map<int, Connection*>::iterator ci = _connections.begin();
    for ( ; ci != _connections.end(); ) {
        int id = ci->first;
        Connection* co = ci->second;
        if (utime - co->LastRequest() > CONNECTIONTIMEOUT) {
            delete co;
            // map::erase doesn't return an iterator, unless
            // __GXX_EXPERIMENTAL_CXX0X__ is defined
            _connections.erase(ci++);
            ILOG(("Timeout, closed connection, id=%d, #connections=%zd",
                (id & 0xffff),_connections.size()));
        }
        else ++ci;
    }
}

unsigned int Connections::num() const
{
    return _connections.size();
}

/* static */
std::string Connection::getIdStr(int id) 
{
    ostringstream ost;
    ost << "connection#" << (id & 0xffff);
    return ost.str();
}

Connection::Connection(const connection * conn, int id)
:  _filegroup(0),_history(),_histlen(),_lastf(0),_lastRequest(time(0)),
    _id(id),_errorMsg(),_state(CONN_OK)
{

    AllFiles *allfiles = AllFiles::Instance();
    _lastRequest = time(0);

    _filegroup = allfiles->get_file_group(conn);

    _filegroup->add_connection(this);

}

Connection::~Connection(void)
{
    if (_lastf)
        _lastf->sync();
    _filegroup->remove_connection(this);

    AllFiles *allfiles = AllFiles::Instance();
    allfiles->close_old_files();
}

int Connection::add_var_group(const struct datadef *dd) throw()
{
    _lastRequest = time(0);
    try {
        return _filegroup->add_var_group(dd);
    }
    catch (const nidas::util::Exception& e) {
        PLOG(("%s",e.what()));
        _state = CONN_ERROR;
        _errorMsg = e.what();
        return -1;
    }
}

Connection *Connections::operator[] (int i) const
{
#ifdef DEBUG
    DLOG(("i=%d,_connections.size()=%d", i, _connections.size()));
#endif
    map <int, Connection*>::const_iterator ci = _connections.find(i);
    if (ci != _connections.end()) return ci->second;
    return 0;
}

NS_NcFile *Connection::last_file() const
{
    return _lastf;
}


void Connection::unset_last_file()
{
    _lastf = 0;
}

int Connection::put_rec(const datarec_float * writerec) throw()
{
    if (_state != CONN_OK) return -1;
    _lastRequest = time(0);
    try {
        _lastf = _filegroup->put_rec<datarec_float,float>(writerec, _lastf);
        _state = CONN_OK;
    }
    catch (const nidas::util::Exception& e) {
        PLOG(("%s",e.what()));
        _state = CONN_ERROR;
        _errorMsg = e.what();
        return -1;
    }
    return 0;
}

int Connection::put_rec(const datarec_int * writerec) throw()
{
    if (_state != CONN_OK) return -1;
    _lastRequest = time(0);
    try {
        _lastf = _filegroup->put_rec<datarec_int,int>(writerec, _lastf);
        _state = CONN_OK;
    }
    catch (const nidas::util::Exception& e) {
        PLOG(("%s",e.what()));
        _state = CONN_ERROR;
        _errorMsg = e.what();
        return -1;
    }
    return 0;
}

//
// Cache the history records, to be written when we close files.
//
int Connection::put_history(const string & h)
{
    _history += h;
    if (_history.length() > 0 && _history[_history.length() - 1] != '\n')
        _history += '\n';
    _lastRequest = time(0);
    return 0;
}

AllFiles::AllFiles(void): _filegroups()
{
    (void) signal(SIGHUP, hangup);
    (void) signal(SIGTERM, shutdown);
    (void) signal(SIGINT, shutdown);
}

AllFiles::~AllFiles()
{
    unsigned int i;
    for (i = 0; i < _filegroups.size(); i++)
        delete _filegroups[i];
}

AllFiles *AllFiles::_instance = 0;

AllFiles *AllFiles::Instance()
{
    if (_instance == 0)
        _instance = new AllFiles;
    return _instance;
}

// Close all open files
void AllFiles::hangup(int sig)
{
    ILOG(("Hangup signal received, closing all open files and old connections."));

    AllFiles *allfiles = AllFiles::Instance();
    allfiles->close();

    Connections *connections = Connections::Instance();
    connections->closeOldConnections();
    ILOG(("%d current connections", connections->num()));

    (void) signal(SIGHUP, hangup);
}

void AllFiles::shutdown(int sig)
{
    ILOG(("Signal %d received, shutting down.",sig));

    AllFiles *allfiles = AllFiles::Instance();
    allfiles->close();
    Connections *connections = Connections::Instance();
    connections->closeOldConnections();
    nc_shutdown(0);
}

//
// Look through existing file groups to find one with
// same output directory and file name format
// If not found, allocate a new group
//
FileGroup *AllFiles::get_file_group(const struct connection *conn)
{
    FileGroup *p;
    vector < FileGroup * >::iterator ip;

#ifdef DEBUG
    DLOG(("filegroups.size=%d", _filegroups.size()));
#endif

    for (ip = _filegroups.begin(); ip < _filegroups.end(); ++ip) {
        p = *ip;
        if (p) {
            if (!p->match(conn->outputdir, conn->filenamefmt))
                continue;
            if (conn->interval != p->interval())
                throw FileGroup::InvalidInterval();
            if (conn->filelength != p->length())
                throw FileGroup::InvalidFileLength();
            return p;
        }
    }

    // Create new file group
    p = new FileGroup(conn);

    if (ip < _filegroups.end())
        *ip = p;
    else
        _filegroups.push_back(p);

    return p;
}

int AllFiles::num_files() const
{
    int unsigned i, n;
    for (i = n = 0; i < _filegroups.size(); i++)
        if (_filegroups[i])
            n += _filegroups[i]->num_files();
    return n;
}

void AllFiles::close()
{
    unsigned int i, n = 0;

    // close all filegroups.  If a filegroup is not active, delete it
    for (i = 0; i < _filegroups.size(); i++) {
        if (_filegroups[i]) {
            _filegroups[i]->close();
            if (!_filegroups[i]->active()) {
                delete _filegroups[i];
                _filegroups[i] = 0;
            } else {
#ifdef DEBUG
                DLOG(("filegroup %d has %d open files, %d var groups",
                            n, _filegroups[i]->num_files(),
                            _filegroups[i]->num_var_groups()));
#endif
                n++;
            }
        }
    }
#ifdef DEBUG
    DLOG(("%d current file groups, heap=%d", n, heap()));
#endif
}

void AllFiles::sync()
{
    // sync all filegroups.
    for (unsigned int i = 0; i < _filegroups.size(); i++) {
        if (_filegroups[i])
            _filegroups[i]->sync();
    }
}

void AllFiles::close_old_files(void)
{
    unsigned int i, n = 0;

    for (i = 0; i < _filegroups.size(); i++) {
        if (_filegroups[i]) {
            _filegroups[i]->close_old_files();
            if (!_filegroups[i]->active()) {
                delete _filegroups[i];
                _filegroups[i] = 0;
            } else {
#ifdef DEBUG
                DLOG(("filegroup %d has %d open files, %d var groups",
                            n, _filegroups[i]->num_files(),
                            _filegroups[i]->num_var_groups()));
#endif
                n++;
            }
        }
    }
#ifdef DEBUG
    DLOG(("%d current file groups, heap=%d", n, heap()));
#endif
}

void AllFiles::close_oldest_file(void)
{
    unsigned int i;
    time_t lastaccess = time(0) - 10;
    time_t accesst;
    FileGroup *fg;
    int oldgroup = -1;

    for (i = 0; i < _filegroups.size(); i++)
        if ((fg = _filegroups[i])
                && (accesst = fg->oldest_file()) < lastaccess) {
            lastaccess = accesst;
            oldgroup = i;
        }
    if (oldgroup >= 0)
        _filegroups[oldgroup]->close_oldest_file();

}

FileGroup::FileGroup(const struct connection *conn)
    throw(Connection::InvalidOutputDir):
    _connections(),_files(),
    _outputDir(),_fileNameFormat(),
    _CDLFileName(),_vargroups(),
    _vargroupId(0),_interval(conn->interval),
    _fileLength(conn->filelength)
{

#ifdef DEBUG
    DLOG(("creating FileGroup, dir=%s,file=%s",
                conn->outputdir, conn->filenamefmt));
#endif

    if (access(conn->outputdir, F_OK)) {
        PLOG(("%s: %m", conn->outputdir));
        throw Connection::InvalidOutputDir();
    }

    _outputDir = conn->outputdir;

    _fileNameFormat = conn->filenamefmt;

    _CDLFileName = conn->cdlfile;

#ifdef DEBUG
    DLOG(("created FileGroup, dir=%s,file=%s",
                _outputDir.c_str(), _fileNameFormat.c_str()));
#endif
}

FileGroup::~FileGroup(void)
{
    close();
    map<int,VariableGroup*>::iterator vi = _vargroups.begin();
    for ( ; vi != _vargroups.end(); ++vi) delete vi->second;
}

int FileGroup::match(const string & dir, const string & file)
{
    /*
     * We want to avoid having nc_server having the same file
     * open twice - which causes major problems.
     */
    if (_fileNameFormat != file)
        return 0;               // file formats don't match
    if (_outputDir == dir)
        return 1;               // file formats and directory
    // names match, must be same

    /* If directory names don't match they could still point
     * to the same directory.  Do an inode comparison.
     */
    struct stat sbuf1, sbuf2;
    if (::stat(_outputDir.c_str(), &sbuf1) < 0) {
        PLOG(("Cannot stat %s: %m", _outputDir.c_str()));
        return 0;
    }
    if (::stat(dir.c_str(), &sbuf2) < 0) {
        PLOG(("Cannot stat %s: %m", dir.c_str()));
        return 0;
    }
    /*
     * Can't use st_dev member to see if directories are on the same 
     * disk.
     * /net/aster/... and
     * /net/isff/aster/...
     * were two automounter mount points that refered to the
     * same physical directory, but had differene sd_devs
     */

    /* If inodes are different they are definitely different */
    if (sbuf1.st_ino != sbuf2.st_ino)
        return 0;

    /*
     * At this point: same file name format strings but
     * different directory path names. Directories have same
     * inode number.
     */

    /* The following code is for the very unlikely situation that the
     * two directories could have the same inode number and be on two
     * separate disk partitions.
     * Create a tmp file on _outputDir and see if it exists on dir
     */
    int match = 0;

    string tmpfile = _outputDir + "/.nc_server_XXXXXX";
    vector < char >tmpname(tmpfile.begin(), tmpfile.end());
    tmpname.push_back('\0');

    int fd;
    if ((fd =::mkstemp(&tmpname.front())) < 0) {
        PLOG(("Cannot create tmpfile on %s: %m", _outputDir.c_str()));
        return match;
    }
    ::close(fd);

    tmpfile = dir + string(tmpname.begin() + _outputDir.length(),
            tmpname.end() - 1);

    fd =::open(tmpfile.c_str(), O_RDONLY);
    // if file successfully opens, then same directory
    if ((match = (fd >= 0)))
        ::close(fd);
    if (::unlink(&tmpname.front()) < 0) {
        PLOG(("Cannot delete %s: %m", &tmpname.front()));
    }
    return match;
}

// delete all NS_NcFile objects
void FileGroup::close()
{
    unsigned int i;
    list < NS_NcFile * >::const_iterator ni;

    for (i = 0; i < _connections.size(); i++) {
        _connections[i]->unset_last_file();
        // write history
        for (ni = _files.begin(); ni != _files.end(); ni++)
            (*ni)->put_history(_connections[i]->get_history());
    }

    while (_files.size() > 0) {
        delete _files.back();
        _files.pop_back();
    }
}

// sync all NS_NcFile objects
void FileGroup::sync()
{
    list < NS_NcFile * >::const_iterator ni;
    for (ni = _files.begin(); ni != _files.end(); ni++)
        (*ni)->sync();
}

void FileGroup::add_connection(Connection * cp)
{
    _connections.push_back(cp);
}

void FileGroup::remove_connection(Connection * cp)
{
    vector < Connection * >::iterator ic;
    Connection *p;

    list < NS_NcFile * >::const_iterator ni;
    // write history
    for (ni = _files.begin(); ni != _files.end(); ni++)
        (*ni)->put_history(cp->get_history());

    for (ic = _connections.begin(); ic < _connections.end(); ic++) {
        p = *ic;
        if (p == cp) {
            _connections.erase(ic);     // warning ic is invalid after erase
            break;
        }
    }
}


NS_NcFile *FileGroup::get_file(double dtime) throw(NetCDFAccessFailed)
{
    NS_NcFile *f = 0;
    list < NS_NcFile * >::iterator ni;
    list < NS_NcFile * >::const_iterator nie;

    nie = _files.end();

    for (ni = _files.begin(); ni != nie; ni++)
        if ((*ni)->EndTimeGT(dtime))
            break;

    /*
     * At this point:
     *    ni == nie, no files open
     *               no file with an endTime > dtime, ie all earlier
     *    or *ni->endTime > dtime
     */

    AllFiles *allfiles = AllFiles::Instance();
    if (ni == nie) {
#ifdef DEBUG
        DLOG(("new NS_NcFile: %s %s", _outputDir.c_str(),
                    _fileNameFormat.c_str()));
#endif
        if ((f = open_file(dtime)))
            _files.push_back(f);
        close_old_files();
        if (allfiles->num_files() > MAX_FILES_OPEN)
            allfiles->close_oldest_file();
    } else if (!(f = *ni)->StartTimeLE(dtime)) {
#ifdef DEBUG
        DLOG(("new NS_NcFile: %s %s", _outputDir.c_str(),
                    _fileNameFormat.c_str()));
#endif

        // If the file length is less than 0, then the file has "infinite"
        // length.  We will have problems here, because you can't
        // insert records, only overwrite or append.
        // if (length() < 0) return(0);

        if ((f = open_file(dtime)))
            _files.insert(ni, f);
        close_old_files();
        if (allfiles->num_files() > MAX_FILES_OPEN)
            allfiles->close_oldest_file();
    }
    return f;
}

NS_NcFile *FileGroup::open_file(double dtime) throw(NetCDFAccessFailed)
{
    int fileExists = 0;

    string fileName =
        build_name(_outputDir, _fileNameFormat, _fileLength, dtime);

    struct stat statBuf;
    if (!access(fileName.c_str(), F_OK)) {
        if (stat(fileName.c_str(), &statBuf) < 0) {
            PLOG(("%s: %m", fileName.c_str()));
        }
        if (statBuf.st_size > 0)
            fileExists = 1;
    }

    if (fileExists) {
        if (!check_file(fileName)) {
            string badName = fileName + ".bad";
            WLOG(("Renaming corrupt file: %s to %s", fileName.c_str(),
                        badName.c_str()));
            if (!access(badName.c_str(), F_OK)
                    && unlink(badName.c_str()) < 0)
                PLOG(("unlink %s, %m", badName.c_str()));
            if (link(fileName.c_str(), badName.c_str()) < 0)
                PLOG(("link %s %s, %m", fileName.c_str(),
                            badName.c_str()));
            if (unlink(fileName.c_str()) < 0)
                PLOG(("unlink %s, %m", fileName.c_str()));
            fileExists = 0;
        }
    }
#ifdef __GNUC__
    enum NcFile::FileMode openmode = NcFile::Write;
#else
    enum FileMode openmode = NcFile::Write;
#endif

#ifdef DEBUG
    ILOG(("NetCDF fileName=%s, exists=%d", fileName.c_str(), fileExists));
    ILOG(("access(%s,F_OK)=%d, eaccess=%d, getuid()=%d, geteuid()=%d",
            _CDLFileName.c_str(),
            access(_CDLFileName.c_str(),F_OK),
            eaccess(_CDLFileName.c_str(),F_OK),
            getuid(),geteuid()));
#endif

    // If the NetCDF file doesn't exist (or is size 0) then
    //   if a CDL file was specified and exists, try to ncgen it.
    // If the NetCDF file doesn't exist (or size 0) and it was not ncgen'd
    //   then do a create/replace.
    if (!fileExists &&
            !(_CDLFileName.length() > 0 && !access(_CDLFileName.c_str(), F_OK)
                && !ncgen_file(_CDLFileName, fileName)))
        openmode = NcFile::Replace;

    return new NS_NcFile(fileName.c_str(), openmode, _interval,
            _fileLength, dtime);
}

string FileGroup::build_name(const string & outputDir,
        const string & nameFormat, double fileLength,
        double dtime) const
{

    // If file length is 31 days, then align file times on months.
    int monthLong = fileLength == 31 * 86400;

    if (monthLong) {
        struct tm tm;
        nidas::util::UTime(dtime).toTm(true, &tm);

        tm.tm_sec = tm.tm_min = tm.tm_hour = 0;
        tm.tm_mday = 1;         // first day of month
        tm.tm_yday = 0;

        nidas::util::UTime ut(true, &tm);
        dtime = ut.toDoubleSecs();
    } else if (fileLength > 0)
        dtime = floor(dtime / fileLength) * fileLength;

    nidas::util::UTime utime(dtime);

    return outputDir + '/' + utime.format(true, nameFormat);
}

int FileGroup::check_file(const string & fileName) const
{
    bool fileok = false;

    try {
        vector < string > args;
        args.push_back("nc_check");
        args.push_back(fileName);
        nidas::util::Process proc =
            nidas::util::Process::spawn("nc_check", args);
        int status;
        char buf[512];
        string errmsg;
        for (;;) {
            ssize_t l = read(proc.getErrFd(),buf,sizeof(buf));
            if (l == 0) break;
            if (l < 0) {
                WLOG(("error reading nc_check error output: %m"));
                break;
            }
            if (errmsg.length() < 1024) errmsg += string(buf,0,l);
        }
        proc.wait(true, &status);
        if (WIFEXITED(status)) {
            if (WEXITSTATUS(status))
                WLOG(("nc_check exited with status=%d, err output=",
                            WEXITSTATUS(status)) << errmsg);
            else {
#ifdef DEBUG
                DLOG(("nc_check exited with status=%d, err output=",
                            WEXITSTATUS(status)) << errmsg);
#endif
                fileok = true;
            }
        } else if (WIFSIGNALED(status))
            WLOG(("nc_check received signal=%d, err output=",
                        WTERMSIG(status)) << errmsg);
    }
    catch(const nidas::util::IOException & e)
    {
        WLOG(("%s", e.what()));
    }

    return fileok;
}

int FileGroup::ncgen_file(const string & CDLFileName,
        const string & fileName) const
{
    int res = 1;
    try {
        vector < string > args;
        args.push_back("ncgen");
        args.push_back("-o");
        args.push_back(fileName);
        args.push_back(CDLFileName);
        nidas::util::Process proc =
            nidas::util::Process::spawn("ncgen", args);
        int status;
        char buf[512];
        string errmsg;
        for (;;) {
            ssize_t l = read(proc.getErrFd(),buf,sizeof(buf));
            if (l == 0) break;
            if (l < 0) {
                WLOG(("error reading nc_check error output: %m"));
                break;
            }
            if (errmsg.length() < 1024) errmsg += string(buf,0,l);
        }
        proc.wait(true, &status);
        if (WIFEXITED(status)) {
            if (WEXITSTATUS(status))
                WLOG(("ncgen exited with status=%d, err output=",
                            WEXITSTATUS(status)) << errmsg);
            else {
                ILOG(("ncgen -o %s %s", fileName.c_str(),CDLFileName.c_str()));
                res = 0;
            }
        } else if (WIFSIGNALED(status))
            WLOG(("ncgen received signal=%d, err output=",
                        WTERMSIG(status)) << errmsg);
    }
    catch(const nidas::util::IOException & e)
    {
        WLOG(("%s", e.what()));
    }
    return res;
}

void FileGroup::close_old_files(void)
{
    NS_NcFile *f;
    time_t now = time(0);
    vector < Connection * >::iterator ic;
    Connection *cp;

    list < NS_NcFile * >::iterator ni;

    for (ni = _files.begin(); ni != _files.end();) {
        f = *ni;
        if (now - f->LastAccess() > FILEACCESSTIMEOUT) {
            for (ic = _connections.begin(); ic < _connections.end(); ic++) {
                cp = *ic;
                if (cp->last_file() == f)
                    cp->unset_last_file();
                // write history
                f->put_history(cp->get_history());
            }
            ni = _files.erase(ni);
            delete f;
        } else
            ++ni;
    }
}

time_t FileGroup::oldest_file(void)
{
    NS_NcFile *f;
    time_t lastaccess = time(0);

    list < NS_NcFile * >::const_iterator ni;

    for (ni = _files.begin(); ni != _files.end();) {
        f = *ni;
        if (f->LastAccess() < lastaccess)
            lastaccess = f->LastAccess();
        ++ni;
    }
    return lastaccess;
}

void FileGroup::close_oldest_file(void)
{
    NS_NcFile *f;
    time_t lastaccess = time(0);
    vector < Connection * >::iterator ic;
    Connection *cp;

    list < NS_NcFile * >::iterator ni, oldest;

    oldest = _files.end();

    for (ni = _files.begin(); ni != _files.end();) {
        f = *ni;
        if (f->LastAccess() < lastaccess) {
            lastaccess = f->LastAccess();
            oldest = ni;
        }
        ++ni;
    }
    if (oldest != _files.end()) {
        f = *oldest;
        for (ic = _connections.begin(); ic < _connections.end(); ic++) {
            cp = *ic;
            if (cp->last_file() == f)
                cp->unset_last_file();
            // write history
            f->put_history(cp->get_history());
        }
        _files.erase(oldest);
        delete f;
    }
}

int FileGroup::add_var_group(const struct datadef *dd) throw(BadVariable)
{

    // check to see if this variable group is equivalent to
    // one we've received before.

    map<int,VariableGroup*>::iterator vi = _vargroups.begin();
    for ( ; vi != _vargroups.end(); ++vi) {
        int id = vi->first;
        VariableGroup* vg = vi->second;
        if (vg->same_var_group(dd)) {
            // looks to be the same. Check counts variable and update attributes.
            vg->check_counts_variable();
            vg->update_attrs(dd);
            return id;
        }
    }

    if (_vargroupId < 0) _vargroupId = 0;

    // throws BadVariable
    VariableGroup *vg = new VariableGroup(dd, _vargroupId, _interval);
    _vargroups[_vargroupId] = vg;

#ifdef DEBUG
    ILOG(("Created variable group %d", _vargroupId));
#endif

    return _vargroupId++;
}

VariableGroup::VariableGroup(const struct datadef *dd, int id, double finterval)
    throw(BadVariable):
    _name(),_interval(dd->interval),_invars(),_outvars(),
    _ndims(0),_dimsizes(), _dimnames(),
    _nsamples(0),_nprefixes(0),
    _rectype(dd->rectype),_datatype(dd->datatype),
    _fillMissing(dd->fillmissingrecords),
    _floatFill(dd->floatFill), _intFill(dd->intFill),
    _id(id),_countsName()
{
    unsigned int i, j, n;
    Variable *v;
    unsigned int nv;

    nv = dd->fields.fields_len;
    if (nv == 0) throw BadVariable("empty variable group");

    if (dd->rectype  != NS_TIMESERIES) 
        throw BadVariable("variable group must be NS_TIMESERIES");

    if (_datatype == NS_FLOAT)
        _intFill = 0;
    _nsamples = (int) floor(finterval / _interval + .5);
    if (_nsamples < 1)
        _nsamples = 1;
    if (_interval < NS_NcFile::minInterval)
        _nsamples = 1;

    _ndims = dd->dimensions.dimensions_len + 2;
    _dimnames = vector<string>(_ndims);
    _dimsizes = vector<long>(_ndims);

    _dimnames[0] = "time";
    _dimsizes[0] = NC_UNLIMITED;

    _dimnames[1] = "sample";
    _dimsizes[1] = _nsamples;

    for (i = 2; i < _ndims; i++) {
        _dimnames[i] = string(dd->dimensions.dimensions_val[i - 2].name);
        _dimsizes[i] = dd->dimensions.dimensions_val[i - 2].size;
    }

    // _invars does not include a counts variable
    for (i = 0; i < nv; i++) {
        _invars.push_back(v =
                new Variable(dd->fields.fields_val[i].name));

        // generate name for log messages
        if (i == 0) {
            ostringstream ost;
            ost << "variables[" << nv << "]=" << v->name() <<
                (nv > 1 ? ",..." : "");
            _name = ost.str();
        }

#ifdef DEBUG
        DLOG(("%s: %d", v->name().c_str(), i));
#endif
        const char *cp = dd->fields.fields_val[i].units;

        // if (!strncmp(v->name(),"chksumOK",8))
        // DLOG(("%s units=%s",v->name(),cp ? cp : "none"));

        if (cp && strlen(cp) > 0)
            v->add_att("units", cp);

        n = dd->attrs.attrs_val[i].attrs.attrs_len;

        for (j = 0; j < n; j++) {
            str_attr *a = dd->attrs.attrs_val[i].attrs.attrs_val + j;
#ifdef DEBUG
            DLOG(("%d %s %s", j, a->name, a->value));
#endif
            v->add_att(a->name, a->value);
        }
    }

    create_outvariables();

    try {
        check_counts_variable();
    }
    catch(const BadVariable& e) {
        for (unsigned i = 0; i < _invars.size(); i++)
            delete _invars[i];
        for (unsigned i = 0; i < _outvars.size(); i++)
            delete _outvars[i];
        throw e;
    }

#ifdef DEBUG
    DLOG(("created outvariables"));
#endif
}

VariableGroup::~VariableGroup(void)
{
    unsigned int i;
    for (i = 0; i < _invars.size(); i++)
        delete _invars[i];
    for (i = 0; i < _outvars.size(); i++)
        delete _outvars[i];
}

int VariableGroup::num_dims(void) const
{
    return _ndims;
}

long VariableGroup::dim_size(unsigned int i) const
{
    if (i < _ndims)
        return _dimsizes[i];
    return -1;
}

const string& VariableGroup::dim_name(unsigned int i) const
{
    static string empty;
    if (i < _ndims)
        return _dimnames[i];
    return empty;
}

void VariableGroup::create_outvariables(void)
{
    for (unsigned int i = 0; i < _invars.size(); i++) {
        Variable *var = _invars[i];
        _outvars.push_back(
                new OutVariable(*var, _datatype, _floatFill,_intFill));
    }
}

void VariableGroup::check_counts_variable() throw(BadVariable)
{
    OutVariable *ov;

    OutVariable * cv = 0;

    // check that all counts attributes are the same.
    set<string> cntsNames;

    for (int i = 0; i < num_vars(); i++) {
        ov = get_var(i);
        if (ov->isCnts()) {
            if (cv) throw BadVariable(getName() + ": multiple counts variables");
            cv = ov;
        }
        else if (ov->att_val("counts").length() > 0)
            cntsNames.insert(ov->att_val("counts"));
    }

    if (cntsNames.size() == 0) {
        return;                 // no counts
    }

    if (cntsNames.size() > 1) {
        ostringstream ost;
        ost << ": inconsistent counts attributes: " <<
            *(cntsNames.begin()) << " and " << *(++cntsNames.begin());
        throw BadVariable(getName() + ost.str());
    }

    string cntsName = *(cntsNames.begin());
    if (cv && cv->name() != cntsName) {
        ostringstream ost;
        ost << ": name of counts variable " << cv->name() <<
            " not equal to counts attributes " << cntsName;
        throw BadVariable(getName() + ost.str());
    }

    setCountsName(cntsName);
}

void VariableGroup::setCountsName(const std::string& val)
{
    _countsName = val;
    OutVariable* cv = 0;
    for (int i = 0; i < num_vars(); i++) {
        OutVariable* ov = get_var(i);
        if (!ov->isCnts()) ov->add_att("counts",val);
        else cv = ov;
    }
    if (!cv) createCountsVariable(val);
}

void VariableGroup:: createCountsVariable(const string& cname)
{
    Variable v(cname);
    OutVariable *ov = new OutVariable(v, NS_INT, _floatFill, _intFill);
    ov->isCnts() = true;
    // no short_name attribute
    ov->add_att("short_name", "");
    _outvars.push_back(ov);
}

//
// Was this VariableGroup created from an identical datadef
//
int VariableGroup::same_var_group(const struct datadef *ddp) const
{

    unsigned int i, j;
    unsigned int nv = ddp->fields.fields_len;

    if (_invars.size() != nv)
        return 0;

    if (_interval != ddp->interval)
        return 0;

    // Check that dimensions are the same.  There can be extra
    // rightmost (trailing) dimensions of 1.
    // This does not check the dimension names
    // It also does not check the time or sample dimensions
    //
    for (i = 0, j = 2; i < ddp->dimensions.dimensions_len && j < _ndims;
            i++, j++)
        if (ddp->dimensions.dimensions_val[i].size != _dimsizes[j])
            return 0;
    for (; i < ddp->dimensions.dimensions_len; i++)
        if (ddp->dimensions.dimensions_val[i].size != 1)
            return 0;
    for (; j < _ndims; j++)
        if (_dimsizes[j] != 1)
            return 0;

    if (_rectype != ddp->rectype)
        return 0;
    if (_datatype != ddp->datatype)
        return 0;

    for (i = 0; i < nv; i++)
        if (_invars[i]->name() != string(ddp->fields.fields_val[i].name))
            return 0;
    return 1;
}
void VariableGroup::update_attrs(const struct datadef *ddp)
{
    unsigned int nv = ddp->fields.fields_len;

    for (unsigned int i = 0; i < nv; i++) {
        Variable *v = _invars[i];
        OutVariable *ov = 0;
        if (i < _outvars.size()) ov = _outvars[i];

        char *cp = ddp->fields.fields_val[i].units;
        if (cp) {
            v->add_att("units", cp);
            if (ov) ov->add_att("units", cp);
        }

        unsigned int n = ddp->attrs.attrs_val[i].attrs.attrs_len;

        for (unsigned int j = 0; j < n; j++) {
            str_attr *a = ddp->attrs.attrs_val[i].attrs.attrs_val + j;
#ifdef DEBUG
            DLOG(("%d %s %s", j, a->name, a->value));
#endif
            v->add_att(a->name, a->value);
            if (ov) ov->add_att(a->name, a->value);
        }
    }
}

Variable::Variable(const string& vname): _name(vname),_isCnts(false),
    _strAttrs()
{
}

Variable::Variable(const Variable & v):
    _name(v._name),_isCnts(v._isCnts),_strAttrs(v._strAttrs)
{
}

void Variable::set_name(const string& n)
{
    _name = n;
}

vector<string> Variable::get_attr_names() const
{
    vector<string> names;
    map<string,string>::const_iterator mi = _strAttrs.begin();
    for ( ; mi != _strAttrs.end(); ++mi) names.push_back(mi->first);
    return names;
}

void Variable::add_att(const string& name, const string& val)
{
    map<string,string>::iterator mi = _strAttrs.find(name);
    if (val.length() > 0) _strAttrs[name] = val;
    else if (mi != _strAttrs.end()) _strAttrs.erase(mi);
}

const string& Variable::att_val(const string& name) const
{
    // could just do _return _strAttrs[name], and make
    // this a non-const method, or make _strAttrs mutable.
    static string empty;
    map<string,string>::const_iterator mi = _strAttrs.find(name);
    if (mi != _strAttrs.end()) return mi->second;
    return empty;
}

OutVariable::OutVariable(const Variable& v, NS_datatype dtype,
        float ff, int ll):
    Variable(v),_datatype(dtype), _floatFill(ff), _intFill(ll)
{
    string namestr = name();

    add_att("short_name", namestr);

    string::size_type ic;

    // convert dots, tics, commas, parens to underscores so that
    // it is a legal NetCDL name
    while((ic = namestr.find_first_of(".'(),*",0)) != string::npos)
        namestr[ic] = '_';

#ifdef REDUCE_DOUBLE_UNDERSCORES
    // convert double underscores to one.
    while((ic = namestr.find("__",0)) != string::npos)
        namestr = namestr.substr(0,ic) + namestr.substr(ic+1);

    // remove trailing _, unless name less than 3 chars
    if ((ic = namestr.length()) > 2 && namestr[ic-1] == '_')
        namestr = namestr.substr(0,ic-1);
#endif

    set_name(namestr);
}

const double NS_NcFile::minInterval = 1.e-5;

NS_NcFile::NS_NcFile(const string & fileName, enum FileMode openmode,
        double interval, double fileLength,
        double dtime) throw(NetCDFAccessFailed):
    NcFile(fileName.c_str(), openmode),
    _fileName(fileName), _startTime(0.0),_endTime(0.0),
    _interval(interval), _lengthSecs(fileLength),
    _timeOffset(0.0),_timeOffsetType(ncFloat),_monthLong(false),
    _ttType(FIXED_DELTAT),_timesAreMidpoints(-1),
    _baseTimeVar(0),_timeOffsetVar(0),_vars(),_recdim(0),
    _baseTime(0),_nrecs(0),_dimNames(0),_dimSizes(),_dimIndices(),
    _ndims(0),_dims(),_ndims_req(0),_lastAccess(0),_lastSync(0),
    _historyHeader()
{

    if (!is_valid())
        throw NetCDFAccessFailed(getName(),"open",get_error_string());

    if (_interval < minInterval)
        _ttType = VARIABLE_DELTAT;

    // If file length is 31 days, then align file times on months.
    _monthLong = _lengthSecs == 31 * 86400;

    if (_monthLong) {
        struct tm tm;
        nidas::util::UTime(dtime).toTm(true, &tm);

        tm.tm_sec = tm.tm_min = tm.tm_hour = 0;
        tm.tm_mday = 1;         // first day of month
        tm.tm_yday = 0;

        nidas::util::UTime ut(true, &tm);
        _baseTime = ut.toSecs();
    } else if (_lengthSecs > 0)
        _baseTime = (int) (floor(dtime / _lengthSecs) * _lengthSecs);

    _timeOffset = -_interval * .5;      // _interval may be 0
    _nrecs = 0;

    /*
     * base_time variable id
     */
    if (!(_baseTimeVar = get_var("base_time"))) {
        /* New variable */
        if (!(_baseTimeVar = NcFile::add_var("base_time", ncLong)) ||
                !_baseTimeVar->is_valid())
            throw NetCDFAccessFailed(getName(),
                    string("add_var ") + "base_time",get_error_string());
        string since =
            nidas::util::UTime(0.0).format(true,
                    "seconds since %Y-%m-%d %H:%M:%S 00:00");
        if (!_baseTimeVar->add_att("units", since.c_str()))
            throw NetCDFAccessFailed(getName(),
                    string("add_att units to ") + _baseTimeVar->name(),get_error_string());
    }

    if (!(_recdim = rec_dim())) {
        if (!(_recdim = add_dim("time")) || !_recdim->is_valid()) {
            // the NcFile has been constructed, and so if this exception is
            // thrown, it will be deleted, which will delete all the 
            // created dimensions and variables.  So we don't have to
            // worry about deleting _recdim here or the variables later
            throw NetCDFAccessFailed(getName(),
                    string("add_dim ") + "time",get_error_string());
        }
    } else
        _nrecs = _recdim->size();

    if (!(_timeOffsetVar = get_var("time")) &&
            !(_timeOffsetVar = get_var("time_offset"))) {
        /* New variable */
        if (!(_timeOffsetVar = NcFile::add_var("time", ncDouble, _recdim))
                || !_timeOffsetVar->is_valid())
            throw NetCDFAccessFailed(getName(),
                    string("add_var ") + "time",get_error_string());
        
        _timeOffsetType = _timeOffsetVar->type();
    } else {
        _timeOffsetType = _timeOffsetVar->type();
        if (_nrecs > 0) {
            // Read last available time_offset, double check it
            long nrec = _nrecs - 1;
            NcValues *val;
            _timeOffsetVar->set_rec(nrec);
            if (!(val = _timeOffsetVar->get_rec())) {
                ostringstream ost;
                ost << "get_rec #" << nrec << " of " << _timeOffsetVar->name();
                throw NetCDFAccessFailed(getName(),ost.str(),get_error_string());
            }
            switch (_timeOffsetType) {
            case ncFloat:
                _timeOffset = val->as_float(0L);
                break;
            case ncDouble:
                _timeOffset = val->as_double(0L);
                break;
            default:
                throw NetCDFAccessFailed(getName(),
                        string("get_rec ") + _timeOffsetVar->name() + " unsupported type",get_error_string());
            }
            delete val;

            if (_ttType == FIXED_DELTAT) {
                _timesAreMidpoints = fabs(fmod(_timeOffset, _interval) - _interval * .5) <
                    _interval * 1.e-3;
#ifdef DEBUG
                DLOG(("_timeOffset=") << _timeOffset << " interval=" << _interval <<
                        " timesAreMidpoints=" << _timesAreMidpoints);
#endif
                if (_timesAreMidpoints) {
                    if (fabs(((nrec + .5) * _interval) - _timeOffset) > _interval * 1.e-3) {
                        PLOG(("%s: Invalid timeOffset (NS_NcFile) = %f, nrec=%d,_nrecs=%d,interval=%f",
                                    _fileName.c_str(), _timeOffset, nrec, _nrecs, _interval));
                        // rewrite them all
                        _nrecs = 0;
                        _timeOffset = -_interval * .5;
                    }
                }
                else {
                    if (fabs((nrec * _interval) - _timeOffset) > _interval * 1.e-3) {
                        PLOG(("%s: Invalid timeOffset (NS_NcFile) = %f, nrec=%d,_nrecs=%d,interval=%f",
                                    _fileName.c_str(), _timeOffset, nrec, _nrecs, _interval));
                        // rewrite them all
                        _nrecs = 0;
                        _timeOffset = -_interval;
                    }
                }
            }
        }
    }

    NcAtt *timeOffsetUnitsAtt;
    if (!(timeOffsetUnitsAtt = _timeOffsetVar->get_att("units"))) {
        string since =
            nidas::util::UTime((time_t) _baseTime).format(true,
                    "seconds since %Y-%m-%d %H:%M:%S 00:00");
        if (!_timeOffsetVar->add_att("units", since.c_str()))
            throw NetCDFAccessFailed(getName(),
                    string("add_att units to ") + _timeOffsetVar->name(),get_error_string());
    } else
        delete timeOffsetUnitsAtt;

    if (_ttType == FIXED_DELTAT) {
        NcAtt *intervalAtt;
        if (!(intervalAtt = _timeOffsetVar->get_att("interval(sec)"))) {
            if (!_timeOffsetVar->add_att("interval(sec)",_interval))
                throw NetCDFAccessFailed(getName(),
                        string("add_att interval(sec) to ") +  _timeOffsetVar->name(),get_error_string());
        } else
            delete intervalAtt;
    }

    /* Write base time */
    if (!_baseTimeVar->put(&_baseTime, &_nrecs))
        throw NetCDFAccessFailed(getName(),
                string("put ") + _baseTimeVar->name(),get_error_string());
#ifdef DEBUG
    DLOG(("%s: nrecs=%d, baseTime=%d, timeOffset=%f, length=%f",
                _fileName.c_str(), _nrecs, _baseTime, _timeOffset, _lengthSecs));
#endif

    _lastAccess = _lastSync = time(0);
    //
    // Write Creation/Update time in global history attribute
    //

    // Remember: this history attribute is not deleted on file close!
    // If you throw an exception between here and the end of the
    // constructor, remember to delete historyAtt first.

    NcAtt *historyAtt;
    //
    // If history doesn't exist, add a Created message.
    // Otherwise don't add an Updated message unless the user
    // explicitly makes a history request.  
    // In some applications connections to nc_server will be made
    // frequently to add data, and we don't want a history record
    // every time.
    //
    if (!(historyAtt = get_att("history"))) {
        string tmphist =
            nidas::util::UTime(_lastAccess).format(true, "Created: %c\n");
        put_history(tmphist);
    } else {
        _historyHeader =
            nidas::util::UTime(_lastAccess).format(true, "Updated: %c\n");
        delete historyAtt;
    }

    ILOG(("%s: %s", (openmode == Write ? "Opened" : "Created"),
                _fileName.c_str()));

    _startTime = _baseTime;

    if (_monthLong) {
        _endTime = _baseTime + 32 * 86400;

        struct tm tm;
        nidas::util::UTime(_endTime).toTm(true, &tm);

        tm.tm_sec = tm.tm_min = tm.tm_hour = 0;
        tm.tm_mday = 1;         // first day of month
        tm.tm_yday = 0;

        nidas::util::UTime ut(true, &tm);
        _endTime = ut.toDoubleSecs();
    } else if (_lengthSecs > 0)
        _endTime = _baseTime + _lengthSecs;
    else
        _endTime = 1.e37;       // Somewhat far off in the future
}

NS_NcFile::~NS_NcFile(void)
{
    ILOG(("Closing: %s", _fileName.c_str()));
    map<int,vector<NS_NcVar*> >::iterator vi = _vars.begin();
    for ( ; vi != _vars.end(); ++vi) {
        vector<NS_NcVar*>& vars = vi->second;
        for (unsigned int j = 0; j < vars.size(); j++)
            delete vars[j];
    }
}

const string & NS_NcFile::getName() const
{
    return _fileName;
}

NcBool NS_NcFile::sync()
{
    _lastSync = time(0);
    NcBool res = NcFile::sync();
    if (!res)
        PLOG(("%s: sync: %s",
                    getName().c_str(),
                    get_error_string().c_str()));
    else DLOG(("%s: sync'd",getName().c_str()));
    return res;
}

bool NS_NcFile::checkCountsVariableName(const string& name,VariableGroup * vgroup)
            throw(NetCDFAccessFailed)
{
    // check if variable exists
    // Y: correct dimension?  If not return false;
    // Check if any variables in file, not in vgroup, have it's name
    // as a counts attribute:
    //  Y: return false
    //  N: return true
    //
    NcVar *ncv;
    if ((ncv = get_var(name.c_str()))) {
        // Check the dimensions. We're not being picky about the type.
        if (!check_var_dims(ncv)) return false;
    }
    const vector<NS_NcVar*>& gvars = get_vars(vgroup);

    // loop over all variables in the file
    for (int i = 0; i < num_vars(); i++) {
        ncv = get_var(i);
        // check that it is a time series variable
        if (ncv->num_dims() == 0 || !ncv->get_dim(0)->is_unlimited()) continue;
        unsigned int j;
        for (j = 0; j < gvars.size(); j++) {
            if (gvars[j] && gvars[j]->var() == ncv) break;
        }
        // check counts attributes of time series variables not in this group
        if (j == gvars.size()) {
            auto_ptr<NcAtt> att(ncv->get_att("counts"));
            if (att.get()) {
                const char* attString = 0;
                if (att->type() == ncChar && att->num_vals() > 0 &&
                        (attString = att->as_string(0)) &&
                        !strcmp(attString, name.c_str())) {
                    delete [] attString;
                    return false;   // string match, this is not a good counts name
                }
                delete [] attString;
            }
        }
    }
    return true;
}

string NS_NcFile::resolveCountsName(
        const set<string>& fileCntsAttrs,VariableGroup* vgroup)
{
    // non-empty requested counts name
    //     no attributes in file
    //         while (!checkCountsVariableName(req name,vg)) createNewName()
    //     file attributes size 1, equal to requested
    //         while (!checkCountsVariableName(req name,vg)) createNewName()
    //     file attributes size 1, not equal to requested
    //         warn
    //         cntsName = file name
    //         if (!checkCountsVariableName(cntsName,vg))
    //             cntsName = req name
    //             while (!checkCountsVariableName(cntsName,vg)) createNewName();
    //     file attributes size > 1
    //         warn
    //         while (!checkCountsVariableName(req name,vg)) createNewName()
    // empty requested name
    //     no attributes in file: do nothing
    //     file attributes size 1
    //         while (!checkCountsVariableName(file name,vg)) createNewName()
    //         update the requested counts name to the attribute from the file
    //     file attributes size > 1
    //         warn
    //         what to do? Work with first one.

    if (fileCntsAttrs.size() > 1)
        WLOG(("%s: %s: multiple counts attributes: ",
                getName().c_str(),vgroup->getName().c_str()) <<
                *(fileCntsAttrs.begin()) << " and " << *(++fileCntsAttrs.begin()));

    string fileCntsName;
    if (fileCntsAttrs.size() > 0)
        fileCntsName = *(fileCntsAttrs.begin());

    string cntsName = vgroup->getCountsName();
    int isuffix = 1;

    if (vgroup->getCountsName().length() > 0) {
        if (fileCntsName.length() > 0) {
            if (fileCntsName == vgroup->getCountsName()) {
                while (!checkCountsVariableName(cntsName,vgroup))
                    cntsName = createNewName(fileCntsName,isuffix);
            }
            else {
                // mismatch of what was requested and what's in the file
                WLOG(("%s: %s: counts attribute: ",
                        getName().c_str(),vgroup->getName().c_str()) <<
                        vgroup->getCountsName() <<
                        " does not match attributes in file: " <<
                        fileCntsName);
                cntsName = fileCntsName;
                // try what's in file
                if (!checkCountsVariableName(cntsName,vgroup)) {
                    // didn't work, try what's requested
                    cntsName = vgroup->getCountsName();
                    while (!checkCountsVariableName(cntsName,vgroup))
                        cntsName = createNewName(vgroup->getCountsName(),isuffix);
                }
            }
        }
        else {
            while (!checkCountsVariableName(cntsName,vgroup))
                cntsName = createNewName(vgroup->getCountsName(),isuffix);
        }
    }
    else {
        // no requested counts name, but counts attributes occur in file
        if (fileCntsName.length() > 0) {
            cntsName = fileCntsName;
            if (checkCountsVariableName(cntsName,vgroup)) {
                // no counts attributes in requested variables, but
                // consistent counts attributes found in file.
                // Set counts attribute for this group from file.
                if (fileCntsAttrs.size() == 1) vgroup->setCountsName(cntsName);
            }
            else {
                do {
                    cntsName = createNewName(fileCntsName,isuffix);
                } while (!checkCountsVariableName(cntsName,vgroup));
            }
        }
    }
    return cntsName;
}

string NS_NcFile::createNewName(const string& name,int & i)
{
    ostringstream ost;
    ost << name << '_' << i++;
    return ost.str();
}

const vector<NS_NcVar*>& NS_NcFile::get_vars(VariableGroup * vgroup)
            throw(NetCDFAccessFailed)
{

    int groupid = vgroup->getId();

    // variables have been initialzed for this VariableGroup and file.
    map<int,vector<NS_NcVar*> >::iterator vi = _vars.find(groupid);
    if (vi != _vars.end()) return vi->second;

    _ndims_req = vgroup->num_dims();

    if (_ndims_req > (signed)_dims.size()) {
        _dimSizes = vector<long>(_ndims_req);
        _dimIndices = vector<int>(_ndims_req);
        _dimNames = vector<string>(_ndims_req);
        _dims = vector<const NcDim *>(_ndims_req);
    }

    for (int i = 0; i < _ndims_req; i++) {
        _dimSizes[i] = vgroup->dim_size(i);
        _dimIndices[i] = -1;
        _dimNames[i] = vgroup->dim_name(i);
    }

    //
    // get dimensions for those group dimensions that are > 1
    //
    for (int i = _ndims = 0; i < _ndims_req; i++) {
#ifdef DEBUG
        DLOG(("VariableGroup %d dimension number %d %s, size=%d",
                    igroup,i,_dimNames[i].c_str(), _dimSizes[i]));
#endif

        // The _dims array is only used when creating a new variable
        // don't create dimensions of size 1
        // Unlimited dimension must be first one.
        if ((i == 0 && _dimSizes[i] == NC_UNLIMITED) || _dimSizes[i] > 1) {
            _dims[_ndims] = get_dim(_dimNames[i].c_str(), _dimSizes[i]);
            if (!_dims[_ndims] || !_dims[_ndims]->is_valid())
                throw NetCDFAccessFailed(getName(),string("get_dim ") + _dimNames[i],get_error_string());
            _ndims++;
        }
    }
#ifdef DEBUG
    for (unsigned int i = 0; i < _ndims; i++)
        DLOG(("%s: dimension %s, size=%d",
                    getName().c_str(),_dims[i]->name(), _dims[i]->size()));

    DLOG(("creating outvariables"));
#endif

    int nv = vgroup->num_vars();    // number of variables in group

    _vars[groupid] = vector<NS_NcVar*>(nv);
    vector<NS_NcVar*>& vars = _vars[groupid];

    set<string> fileCntsNames;

    // Initialize any variables that are currently in this file,
    // Check for validity. Accumulate counts attributes.
    for (int iv = 0; iv < nv; iv++) {
        OutVariable *ov = vgroup->get_var(iv);
        vars[iv] = 0;
        if (!ov->isCnts()) {
            NcVar* ncv = find_var(ov);
            if (ncv) {
                NS_NcVar* nsv =
                    new NS_NcVar(ncv, &_dimIndices.front(), _ndims_req, ov->floatFill(),
                        ov->intFill(), ov->isCnts());
                vars[iv] = nsv;

                NcAtt *att = nsv->get_att("counts");
                if (att) {
                    // accumulate counts attributes of all variables in the file
                    if (att->type() == ncChar && att->num_vals() > 0) {
                        const char *cname = att->as_string(0);
                        if (cname) {
                            fileCntsNames.insert(cname);
                            delete [] cname;
                        }
                    }
                    delete att;
                }
            }
        }
    }
    string cntsName = resolveCountsName(fileCntsNames,vgroup);

    bool doSync = false;
    if (cntsName != vgroup->getCountsName())
        ILOG(("%s: %s: new name for counts variable: %s",
                   getName().c_str(),vgroup->getName().c_str(),
                   cntsName.c_str()));

    for (int iv = 0; iv < nv; iv++) {
        OutVariable *ov = vgroup->get_var(iv);
        if (ov->isCnts() && ov->name() != cntsName) ov->set_name(cntsName);

        NS_NcVar* nsv = vars[iv];
        if (!nsv) vars[iv] = nsv = add_var(ov,doSync);
        if (add_attrs(ov,nsv,cntsName)) doSync = true;
    }

#ifdef DEBUG
    DLOG(("added vars"));
#endif
    if (doSync) sync();
    return vars;
}

NS_NcVar *NS_NcFile::add_var(OutVariable * v, bool& modified)
            throw(NetCDFAccessFailed)
{
    NcVar *var;
    NS_NcVar *fsv;
    bool isCnts = v->isCnts();

    const string& varName = v->name();

    // No matching variables found, create new one
    if (!(var = find_var(v))) {
        modified = true;
        if (!(var =
                    NcFile::add_var(varName.c_str(), (NcType) v->data_type(), _ndims,
                        &_dims.front())) || !var->is_valid()) {
#ifdef DEBUG
            for (unsigned int i = 0; i < _ndims; i++)
                DLOG(("dims=%d id=%d size=%d", i, _dims[i]->id(),
                            _dims[i]->size()));
#endif
            throw NetCDFAccessFailed(getName(),string("add_var ") + varName,get_error_string());
        }
    }

    // double check ourselves
    if (!check_var_dims(var))
        throw NetCDFAccessFailed(getName(),string("check dimensions ") + varName,"wrong dimensions");

    fsv = new NS_NcVar(var, &_dimIndices.front(), _ndims_req, v->floatFill(),
            v->intFill(), isCnts);
    return fsv;
}

bool NS_NcFile::add_attrs(OutVariable * v, NS_NcVar * var,const string& cntsName)
                throw(NetCDFAccessFailed)
{
    // add attributes if they don't exist in file, otherwise leave them alone

    // bug in netcdf, if units is empty string "", result in 
    // netcdf file is some arbitrary character.
    //

    bool modified = false;

    auto_ptr<NcAtt> nca(var->get_att("_FillValue"));
    if (!nca.get()) {
        modified = true;
        switch (v->data_type()) {
        case NS_INT:
            if (!var->add_att("_FillValue",v->intFill()))
                throw NetCDFAccessFailed(getName(),string("add_att _FillValue to ") + var->name(),get_error_string());
            break;
        case NS_FLOAT:
            if (!var->add_att("_FillValue", v->floatFill()))
                throw NetCDFAccessFailed(getName(),string("add_att _FillValue to ") + var->name(),get_error_string());
            break;
        }
    }
    nca.reset();

    try {
#ifdef DO_UNITS_SEPARATELY
        if (v->units().length() > 0) {
            if (var->set_att("units",v->units())) modified = true;
        }
#endif
        // all string attributes
        vector<string> attrNames = v->get_attr_names();
        for (unsigned int i = 0; i < attrNames.size(); i++) {
            string aname = attrNames[i];
            string aval = v->att_val(aname);
            // do counts below
            if (aname == "counts") continue;
            if (aval.length() > 0) {
                if (var->set_att(aname,aval)) modified = true;
            }
        }
        // if cntsName is non-empty, set it in the file, even if it
        // isn't an attribute of the OutVariable
        // This logic will not change a counts attribute to an empty string
        if (!v->isCnts() && cntsName.length() > 0) {
            if (var->set_att("counts",cntsName)) modified = true;
        }
    }
    catch(const NetCDFAccessFailed& e) {
        // add file name to message
        throw NetCDFAccessFailed(getName() + ": " + e.toString());
    }
    return modified;
}

//
// Find the variable.
// First lookup by the NetCDF variable name we've created for it.
// If a variable is found by that name, check that the short_name
// attribute is correct.  If it isn't, then someone has been
// renaming variables and we have to create a new variable name.
//
NcVar *NS_NcFile::find_var(OutVariable * v) throw(NetCDFAccessFailed)
{
    int i;
    NcVar *var;
    const string& varName = v->name();
    const string& shortName = v->att_val("short_name");

    bool nameExists = false;

    if ((var = get_var(varName.c_str()))) {
        nameExists = 1;
        // Check its short_name attribute
        if (shortName.length() > 0) {
            NcAtt *att;
            if ((att = var->get_att("short_name"))) {
                char *attString = 0;
                if (att->type() != ncChar || att->num_vals() == 0 ||
                        !(attString = att->as_string(0)) ||
                        strcmp(attString, shortName.c_str()))
                    var = 0;
                delete [] attString;
                delete att;
            }
        }
    }
    //
    // If we can't find a variable with the same NetCDF variable name,
    // and a matching short_name, look through all other variables for
    // one with a matching short_name
    //
    for (i = 0; !var && shortName.length() > 0 && i < num_vars(); i++) {
        var = get_var(i);
        // Check its short_name attribute
        NcAtt *att;
        if ((att = var->get_att("short_name"))) {
            char* attString = 0;
            if (att->type() == ncChar && att->num_vals() > 0 &&
                    (attString = att->as_string(0)) &&
                    !strcmp(attString, shortName.c_str())) {
                delete att;
                delete [] attString;
                break;          // match
            }
            delete att;
            delete [] attString;
        }
        var = 0;
    }

    if (var && var->type() != (NcType) v->data_type()) {
        // we'll just warn about this at the moment.
        WLOG(("%s: variable %s is of wrong type",
                    _fileName.c_str(), var->name()));
    }

    if (var && !check_var_dims(var)) {
        WLOG(("%s: variable %s has incorrect dimensions",
                    _fileName.c_str(), var->name()));
        ostringstream ost;

        ost << varName << '(';
        for (i = 0; i < _ndims_req; i++) {
            if (i > 0) ost << ',';
            ost << _dimNames[i] << '=' << _dimSizes[i];
        }
        ost << ')';
        WLOG(("%s: should be declared %s", _fileName.c_str(), ost.str().c_str()));

        if (shortName.length() > 0) {
            // Variable with matching short_name, but wrong dimensions
            // We'll change the short_name attribute of the offending
            // variable to "name_old" and create a new variable.
            string tmpString = shortName + "_old";
            if (!var->add_att("short_name", tmpString.c_str()))
                throw NetCDFAccessFailed(getName(),
                        string("add_att short_name to ") + var->name(),get_error_string());
        }
        var = 0;
    }

    if (!var && nameExists) {
        //
        // !var && nameExists means there was a variable with the same name,
        // but differing short_name, dimensions or type.  So we need to
        // change our name.
        //
        string newname;

        int nunique = 1;
        for (;; nunique++) {
            ostringstream ost;
            ost << varName << '_' << nunique;
            newname = ost.str();
            if (!(get_var(newname.c_str())))
                break;
        }
#ifdef DEBUG
        DLOG(("%s: %s new name= %s\n",
                    _fileName.c_str(), var->name(), newname.c_str()));
#endif
        v->set_name(newname.c_str());
    }
    return var;
}

long NS_NcFile::put_time(double timeoffset) throw(NetCDFAccessFailed)
{
    float floatOffset;
    long nrec;

    /*
     * nrec is the record number to be written.
     * _nrecs is one more than the last record written,
     *    or if we're at the end of the file, the number of
     *    records in the file.
     */
    if (_ttType == VARIABLE_DELTAT)
        nrec = _nrecs;
    else if (_timesAreMidpoints)
        nrec = (long) floor(timeoffset / _interval);
    else
        nrec = (long) rint(timeoffset / _interval);

#ifdef DEBUG
    DLOG(("timeoffset=%f, _timeOffset=%f,nrec=%d, nrecs=%d,interval=%f",
                (double) timeoffset, (double) _timeOffset, nrec, _nrecs,
                _interval));
#endif

#ifdef DOUBLE_CHECK
    if (nrec < _nrecs - 1) {
        // time for this record has been written
        // double check time of record
        sync();
        double tmpOffset;
        NcValues *val;
        _timeOffsetVar->set_rec(nrec);
        if (!(val = _timeOffsetVar->get_rec()))
            throw NetCDFAccessFailed(getName(),string("get_rec ") + _timeOffsetVar->getName(),get_error_string());

        switch (_timeOffsetType) {
        case ncFloat:
            tmpOffset = val->as_float(0L);
            break;
        case ncDouble:
            tmpOffset = val->as_double(0L);
            break;
        }
        delete val;

        if (fabs((double) tmpOffset - (double) timeoffset) >
                _interval * 1.e-3) {
            PLOG(("Invalid timeoffset=%f, file timeOffset=%f, nrec=%d, _nrecs=%d, _interval=%f", (double) timeoffset, (double) tmpOffset, nrec, _nrecs, _interval));
        }
    }
#endif

    // Write time to previous records and the current record
    for (; _nrecs <= nrec; _nrecs++) {
        if (_ttType == VARIABLE_DELTAT)
            _timeOffset = timeoffset;
        else
            _timeOffset += _interval;
        int i = 0;
        switch (_timeOffsetType) {
        case ncFloat:
            floatOffset = _timeOffset;
            i = _timeOffsetVar->put_rec(&floatOffset, _nrecs);
            break;
        case ncDouble:
            i = _timeOffsetVar->put_rec(&_timeOffset, _nrecs);
            break;
        default:
            break;
        }
        if (!i)
            throw NetCDFAccessFailed(getName(),string("put_rec ") + _timeOffsetVar->name(),get_error_string());
    }
#ifdef DEBUG
    DLOG(("after fill timeoffset = %f, timeOffset=%f,nrec=%d, _nrecs=%d,interval=%f",
                (double)timeoffset,(double)_timeOffset, nrec, _nrecs, (double)_interval));
#endif

#ifdef LAST_TIME_CHECK
    if (_ttType == FIXED_DELTAT && nrec == _nrecs - 1) {
        if (fabs((double) _timeOffset - (double) timeoffset) >
                _interval * 1.e-3) {
            PLOG(("Invalid timeoffset = %f, file timeOffset=%f,nrec=%d, _nrecs=%d, interval=%f", timeoffset, _timeOffset, nrec, _nrecs, _interval));
        }
    }
#endif
    return nrec;
}

int NS_NcFile::put_history(string val)
{
    if (val.length() == 0)
        return 0;

    string history;

    NcAtt *historyAtt = get_att("history");
    if (historyAtt) {
        char *htmp = historyAtt->as_string(0);
        history = htmp;
        delete [] htmp;
#ifdef DEBUG
        DLOG(("history=%.40s", history.c_str()));
#endif
        delete historyAtt;
    }

    string::size_type i1, i2 = 0;

    // check \n delimited records in h and history
    for (i1 = 0; i1 < val.length(); i1 = i2) {

        i2 = val.find('\n', i1);
        if (i2 == string::npos)
            i2 = val.length();
        else
            i2++;

        string::size_type j1, j2 = 0;
        for (j1 = 0; j1 < history.length(); j1 = j2) {
            j2 = history.find('\n', j1);
            if (j2 == string::npos)
                j2 = history.length();
            else
                j2++;

            if (history.substr(j1, j2 - j2).
                    find(val.substr(i1, i2 - i1)) != string::npos) {
                // match of a line in val with the existing history
                val = val.substr(0, i1) + val.substr(i2);
                i2 = i1;
            }
        }

        if (val.length() > 0) {
            history += _historyHeader + val;
            if (!add_att("history", history.c_str()))
                PLOG(("add history att: %s: %s", _fileName.c_str(),
                            get_error_string().c_str()));
        }
    }

    _lastAccess = time(0);

#ifdef DEBUG
    DLOG(("NS_NcFile::put_history"));
#endif

    // Don't sync;
    return 0;
}

NcBool NS_NcFile::check_var_dims(NcVar * var)
{

    //
    // Example:
    //
    //   NetCDF file
    //    dimensions:
    //            time=99;
    //            sample = 5;
    //            station = 8;
    //            sample_10 = 10;
    //            station_2 = 2;
    //    variables:
    //            x(time,sample,station);
    //            y(time);
    //            z(time,station_2);
    //            zz(time,sample_10,station_2);
    //   Input:
    //     variable: x
    //     dimnames:  time, sample, station
    //     sizes:    any     5        8
    //     _ndims_req: 3
    //    Returned:
    //            function value: 1 (OK)
    //            indices = 0,1,2 sample is dimension 1 of variable,
    //                            station is dimension 2
    //    Input:
    //     variable: y
    //     dimnames:  time, sample, station
    //     sizes:    any      1       1
    //     _ndims_req: 3
    //    Returned:
    //            function value: 1 (OK)
    //            indices = 0,-1,-1 (variable has no sample or station dim)
    //    Input:
    //     variable: z
    //     dimnames:  time, sample, station
    //     sizes:    any   1          2
    //     ndimin: 3
    //    Returned:
    //            function value: 1 (OK)
    //            indices = 0,-1,1 variable has no sample dim,
    //                            station_2 dim is dimension 1 (station_2)
    //                            Note that the dimension name can
    //                            have a _N suffix.  That way
    //                            a more than one value for a station
    //                            dimension can exist in the file -
    //                            perhaps a variable was sampled
    //                            by a subset of the stations.
    //                            
    //    Input:
    //     variable: zz
    //     dimnames:  time, sample, station
    //     sizes:      any    10      2
    //     ndimin: 3
    //    Returned:
    //            function value: 1 (OK)
    //            indices = 0,1,2 sample_10 is dimension 1 of variable,
    //                            station_2 is dimension 2
    //    Input:
    //     variable: zz
    //     dimnames:  time,sample, station
    //     sizes:   any   1           9
    //     ndimin: 3
    //    Returned:
    //            function value: 0 (No match, no station dimension
    //                            of value 9)

    int ndims;
    NcDim *dim;
    int i, ireq;

    ndims = var->num_dims();
    if (ndims < 1) {
        PLOG(("%s: variable %s has no dimensions",
                    _fileName.c_str(), var->name()));
        return 0;
    }

    // do the dimension checks only for time series variables
    if (!var->get_dim(0)->is_unlimited())
        return 0;

    for (ireq = i = 0; i < ndims && ireq < _ndims_req;) {
        dim = var->get_dim(i);

#ifdef DEBUG
        DLOG(("%s: dim[%d] = %s, size=%d ndims=%d",
                    var->name(),i, dim->name(), dim->size(), ndims));
        DLOG(("%s: req dim[%d] = %s, size=%d ndimin=%d",
                    getName().c_str(),ireq, _dimNames[ireq].c_str(), _dimSizes[ireq], _ndims_req));
#endif
        if (_dimSizes[ireq] == NC_UNLIMITED) {
            if (dim->is_unlimited())
                _dimIndices[ireq++] = i++;
            else
                return 0;
        } else if (!strncmp
                (dim->name(),_dimNames[ireq].c_str(),_dimNames[ireq].length())) {
            // name match
            if (dim->size() != _dimSizes[ireq]) {
#ifdef DEBUG
                DLOG(("dimension size mismatch for var=%s, dim %s=%d, expected size=%d",
                            var->name(),dim->name(),dim->size(),_dimSizes[ireq]));
#endif
                return 0;
            }
            _dimIndices[ireq++] = i++;
        }
        // If no name match, then the requested dimension must be 1.
        else {
            if (_dimSizes[ireq] != 1) {
#ifdef DEBUG
                DLOG(("no dimension name match for var=%s, dim %s=%d",
                            var->name(),dim->name(),dim->size()));
#endif
                return 0;
            }
            _dimIndices[ireq++] = -1;
        }
    }
#ifdef DEBUG
    DLOG(("ireq=%d _ndims_req=%d, i=%d, ndims=%d",
                ireq, _ndims_req, i, ndims));
#endif
    // remaining requested or existing dimensions should be 1
    for (; ireq < _ndims_req; ireq++)
        if (_dimSizes[ireq] > 1)
            return 0;
    for (; i < ndims; i++) {
        dim = var->get_dim(i);
        if (dim->size() != 1)
            return 0;
    }
    return 1;
}

const NcDim *NS_NcFile::get_dim(NcToken prefix, long size)
{
    const NcDim *dim;
    int i, l;

    if (size == NC_UNLIMITED)
        return _recdim;
    if ((dim = NcFile::get_dim(prefix)) && dim->size() == size)
        return dim;

    int ndims = num_dims();

    l = strlen(prefix);

    // Look for a dimension whose name starts with prefix and with correct size
    //
    for (i = 0; i < ndims; i++) {
        dim = NcFile::get_dim(i);
#ifdef DEBUG
        DLOG(("dim[%d]=%s, size %d", i, dim->name(), dim->size()));
#endif
        if (!strncmp(dim->name(), prefix, l) && dim->size() == size)
            return dim;
    }

    // At this point:
    //    there are no dimensions starting with "prefix"
    //    or if there are, they don't have the correct size
    //

    char tmpString[64];

    for (i = 0;; i++) {
        if (!i)
            strcpy(tmpString, prefix);
        else
            sprintf(tmpString + strlen(tmpString), "_%ld", size);
        if (!(dim = NcFile::get_dim(tmpString)))
            break;
    }
    // found a unique dimension name, starting with prefix

    dim = add_dim(tmpString, size);
#ifdef DEBUG
    DLOG(("new dimension %s, size %d", tmpString, size));
#endif
    return dim;
}

NS_NcVar::NS_NcVar(NcVar * var, int *dimIndices, int ndimIndices, float ffill, int ifill, bool iscnts):
    _var(var),_dimIndices(0),_ndimIndices(ndimIndices),
    _start(0),_count(0), _isCnts(iscnts), _floatFill(ffill),
    _intFill(ifill)
{
    int i;
    _dimIndices = new int[_ndimIndices];
    _start = new long[_ndimIndices];
    _count = new long[_ndimIndices];
    for (i = 0; i < _ndimIndices; i++)
        _dimIndices[i] = dimIndices[i];
}


NS_NcVar::~NS_NcVar()
{
    delete [] _dimIndices;
    delete [] _start;
    delete [] _count;
}

bool NS_NcVar::set_att(const string& aname, const string& aval)
    throw(NetCDFAccessFailed)
{
    bool modified = false;
    const char* faval = 0;
    auto_ptr<NcAtt> nca(get_att(aname.c_str()));
    if (nca.get()) {
        auto_ptr<NcValues> uvals(nca->values());
        if (uvals.get() && nca->num_vals() >= 1 && nca->type() == ncChar) {
            faval = uvals->as_string(0);
            if (!faval || string(faval) != aval) {
                modified = true;
                delete [] faval;
                if (!add_att(aname.c_str(), aval.c_str()))
                    throw NetCDFAccessFailed(string("add_att ") +
                            aname + " to " + name() + ": " + get_error_string());
            }
            else delete [] faval;
        }
    }
    else {
        modified = true;
        if (!add_att(aname.c_str(), aval.c_str()))
        throw NetCDFAccessFailed(string("add_att ") +
                aname + " to " + name() + ": " + get_error_string());
    }
    return modified;
}

NcBool NS_NcVar::set_cur(long nrec, int nsample, const long *start)
{
    int i, j, k;
    _start[0] = nrec;
    if ((k = _dimIndices[1]) > 0) _start[k] = nsample;

    for (i = 0, j = 2; j < _ndimIndices; i++, j++)
        if ((k = _dimIndices[j]) > 0)
            _start[k] = start[i];

    return _var->set_cur(_start);
}

int NS_NcVar::put(const float *d, const long *counts)
{
    int nout = put_len(counts); // this sets _count
    // type conversion of one data value
    if (nout == 1 && _var->type() == ncLong) {
        int dl = (int) d[0];
        if (d[0] == _floatFill)
            dl = _intFill;
        return (_var->put(&dl, _count) ? nout : 0);
    }
    int i = _var->put(d, _count);
#ifdef DEBUG
    DLOG(("_var->put of %s, i=%d, nout=%d", name(), i, nout));
#endif
    return (i ? nout : 0);
    // return (_var->put(d,_count) ? nout : 0);
}

int NS_NcVar::put(const int * d, const long *counts)
{
    int nout = put_len(counts); // this sets _count
    // type conversion of one data value
    if (nout == 1 && _var->type() == ncFloat) {
        float df = d[0];
        return (_var->put(&df, _count) ? nout : 0);
    }
    return (_var->put(d, _count) ? nout : 0);
}

int NS_NcVar::put_len(const long *counts)
{
    int i, j, k;
    int nout = 1;
    _count[0] = 1;
    if ((k = _dimIndices[1]) > 0)
        _count[k] = 1;
#ifdef DEBUG
    DLOG(("%s: _dimIndices[1]=%d", name(), _dimIndices[1]));
#endif

    for (i = 0, j = 2; j < _ndimIndices; i++, j++)
        if ((k = _dimIndices[j]) > 0) {
            _count[k] = counts[i];
#ifdef DEBUG
            DLOG(("%s: _dimIndices[%d]=%d,counts[%d]=%d",
                        name(), j, _dimIndices[j], i, counts[i]));
#endif
            nout *= _count[k];
        }
    return nout;
}

NcServerApp::NcServerApp(): 
    _username(), _userid(0),_groupname(),_groupid(0),
    _suppGroupNames(),_suppGroupIds(),
    _daemon(true),_logLevel(defaultLogLevel)
{
}

void NcServerApp::usage(const char *argv0)
{
    cerr << "******************************************************************\n\
        nc_server is a program that supports writing to NetCDF files via RPC calls.\n\
        Multiple programs can write to the same file at one time, or the programs can\n\
        write to separate collections of files.  Currently nc_server supports writing\n\
        to version 3 NetCDF files which follow the time series conventions of the NCAR/EOL ISFS.\n\
        nc_server is part of the nc_server package.\n" << 
        "******************************************************************\n" << endl;

    cerr << "Usage: " << argv0 << " [-d] [-l loglevel] [-u username] [ -g groupname -g ... ] [-z]\n\
        -d: debug, run in foreground and send messages to stderr with log level of debug\n\
        Otherwise run in the background, cd to /, and log messages to syslog\n\
        Specify a -l option after -d to change the log level from debug\n\
        -l loglevel: set logging level, 7=debug,6=info,5=notice,4=warning,3=err,...\n\
        The default level if no -d option is " << defaultLogLevel << "\n\
        -u name: change user id of the process to given user name and their default group\n\
        after opening RPC portmap socket\n\
        -g name: add name to the list of supplementary group ids of the process.\n\
        More than one -g option can be specified so that the process can belong to more\n\
        than one group, if necessary, for write permissions on multiple directories\n\
        -z: run in background as a daemon. Either the -d or -z options must be specified" << endl;
}

int NcServerApp::parseRunstring(int argc, char **argv)
{
    int c;
    int daemonOrforeground = -1;
    while ((c = getopt(argc, argv, "dl:g:u:z")) != -1) {
        switch (c) {
        case 'd':
            daemonOrforeground = 0;
            _daemon = false;
            _logLevel = nidas::util::LOGGER_DEBUG;
            break;
        case 'g':
            {
                struct group groupinfo;
                struct group *gptr;
                long nb = sysconf(_SC_GETGR_R_SIZE_MAX);
                if (nb < 0) nb = 4096;
                vector<char> strbuf(nb);
                int res;
                if ((res = getgrnam_r(optarg,&groupinfo,&strbuf.front(),strbuf.size(),&gptr)) != 0) {
                    cerr << "getgrnam_r: " << nidas::util::Exception::errnoToString(res) << endl;
                    return 1;
                }
                else if (!gptr) {
                    cerr << "cannot find group " << optarg << endl;
                    return 1;
                }
                _suppGroupIds.push_back(groupinfo.gr_gid);
                _suppGroupNames.push_back(optarg);
            }
            break;
        case 'l':
            _logLevel = atoi(optarg);
            break;
        case 'u':
            {
                _username = optarg;
                struct passwd pwdbuf;
                struct passwd *result;
                long nb = sysconf(_SC_GETPW_R_SIZE_MAX);
                if (nb < 0) nb = 4096;
                vector < char >strbuf(nb);
                int res;
                if ((res = getpwnam_r(optarg, &pwdbuf, &strbuf.front(), nb, &result)) != 0) {
                    cerr << "getpwnam_r: " << nidas::util::Exception::errnoToString(res) << endl;
                    return 1;
                }
                else if (!result) {
                    cerr << "Unknown user: " << optarg << endl;
                    return 1;
                }
                _userid = pwdbuf.pw_uid;
                _groupid = pwdbuf.pw_gid;
                struct group groupinfo;
                struct group *gptr;
                nb = sysconf(_SC_GETGR_R_SIZE_MAX);
                if (nb < 0) nb = 4096;
                strbuf.resize(nb);
                if ((res = getgrgid_r(_groupid,&groupinfo,&strbuf.front(),strbuf.size(),&gptr)) != 0) {
                    cerr << "getgrgid_r: " << nidas::util::Exception::errnoToString(res) << endl;
                    return 1;
                }
                else if (!gptr) {
                    cerr << "Unknown group id for user " << optarg << endl;
                    return 1;
                }
                _groupname = groupinfo.gr_name;
            }
            break;
        case 'z':
            daemonOrforeground = 1;
            _daemon = true;
            break;
        default:
            usage(argv[0]);
            return 1;
        }
    }
    if (daemonOrforeground < 0) {
        usage(argv[0]);
        return 1;
    }
    return 0;
}

void NcServerApp::setup()
{
    if (getuid() != 0) {
        struct passwd *pwent = getpwuid(getuid());
        WLOG(("Warning: userid=%s (%d) is not root. Calls to rpcbind may fail since we can't use a restricted port number", (pwent == NULL ? "unknown" : pwent->pw_name), getuid()));
    }

    nidas::util::Logger * logger = 0;
    nidas::util::LogScheme logscheme("nc_server");

    nidas::util::LogConfig lc;
    lc.level = _logLevel;

    if (_daemon) {
        // fork to background
        if (daemon(0, 0) < 0) {
            nidas::util::IOException e("nc_server", "daemon", errno);
            cerr << "Warning: " << e.toString() << endl;
        }
        logger =
            nidas::util::Logger::createInstance("nc_server",
                    LOG_PID, LOG_LOCAL5);
        logscheme.setShowFields("level,message");
    } else
        logger = nidas::util::Logger::createInstance(&cerr);

    logscheme.addConfig(lc);
    logger->setScheme(logscheme);
}

int NcServerApp::run(void)
{
    ILOG(("nc_server starting"));
    SVCXPRT *transp;

    (void) pmap_unset(NETCDFSERVERPROG, NETCDFSERVERVERS);

    transp = svctcp_create(RPC_ANYSOCK, 0, 0);
    if (transp == NULL) {
        PLOG(("cannot create tcp service."));
        return 1;
    }
    if (!svc_register(transp, NETCDFSERVERPROG, NETCDFSERVERVERS,
                netcdfserverprog_1, IPPROTO_TCP)) {
        PLOG(("Unable to register (NETCDFSERVERPROG=%x, NETCDFSERVERVERS, tcp): %m", NETCDFSERVERPROG));
        return 1;
    }

#ifdef HAS_CAPABILITY_H 
    /* man 7 capabilities:
     * If a thread that has a 0 value for one or more of its user IDs wants to
     * prevent its permitted capability set being cleared when it  resets  all
     * of  its  user  IDs  to  non-zero values, it can do so using the prctl()
     * PR_SET_KEEPCAPS operation.
     *
     * If we are started as uid=0 from sudo, and then setuid(x) below
     * we want to keep our permitted capabilities.
     */
    try {
        if (prctl(PR_SET_KEEPCAPS,1,0,0,0) < 0)
            throw nidas::util::Exception("prctl(PR_SET_KEEPCAPS,1)",errno);
    }
    catch (const nidas::util::Exception& e) {
        WLOG(("%s: %s. Will not be able to add supplement group ids","nc_server",e.what()));
    }
#endif

    if (_groupid != 0 && getegid() != _groupid) {
        if (setgid(_groupid) < 0)
            WLOG(("%s: cannot change group id to %d (%s): %m","nc_server",
                        _groupid,_groupname.c_str()));
    }

    if (_userid != 0 && geteuid() != _userid) {
        if (setuid(_userid) < 0)
            WLOG(("%s: cannot change userid to %d (%s): %m", "nc_server",
                        _userid,_username.c_str()));
    }

    // add CAP_SETGID capability so that we can add to our supplmental group ids
    try {
        nidas::util::Process::addEffectiveCapability(CAP_SETGID);
        DLOG(("CAP_SETGID = ") << nidas::util::Process::getEffectiveCapability(CAP_SETGID));
    }
    catch (const nidas::util::Exception& e) {
        WLOG(("%s: %s","nc_server",e.what()));
    }

    // even if user didn't specify -g and _suppGroupIds.size() is 0
    // we want to do a setgroups to reset them, otherwise the
    // root group is still in the list of supplemental group ids.
    for (unsigned int i = 0; i < _suppGroupIds.size(); i++) {
        DLOG(("%s: groupid=%d","nc_server",_suppGroupIds[i]));
    }
    if (setgroups(_suppGroupIds.size(),&_suppGroupIds.front()) < 0) {
        WLOG(("%s: failure in setgroups system call, ngroup=%d: %m",
                    "nc_server",_suppGroupIds.size()));
    }

    // create files with group write
    umask(S_IWOTH);

    svc_run();
    PLOG(("svc_run returned: %m"));
    return 1;
    /* NOTREACHED */
}

int main(int argc, char **argv)
{
    NcServerApp ncserver;
    int res;
    if ((res = ncserver.parseRunstring(argc, argv)))
        return res;
    ncserver.setup();
    return ncserver.run();
}
