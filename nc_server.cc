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

Connections::Connections(void): _connectionId(0)
{
    nidas::util::UTime::setTZ("GMT");
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
    std::map <int, Connection * >::iterator ci = _connections.begin();
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
    if (_connectionId < 0) _connectionId = 0;
    try {
        cp = new Connection(conn,_connectionId);
    }
    catch(Connection::InvalidOutputDir) {
        delete cp;
        return -1;
    }
    _connections[_connectionId] = cp;
    ILOG(("Opened connection, id=%d, #connections=%zd, heap=%zd",
            _connectionId,_connections.size(), heap()));
    return _connectionId++;
}

int Connections::closeConnection(int id)
{
    std::map <int, Connection * >::iterator ci = _connections.find(id);
    if (ci != _connections.end()) {
        int id = ci->first;
        delete ci->second;
        _connections.erase(ci);
        ILOG(("Closed connection, id=%d, #connections=%zd",
            id,_connections.size()));
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

    std::map<int, Connection*>::iterator ci = _connections.begin();
    for ( ; ci != _connections.end(); ) {
        int id = ci->first;
        Connection* co = ci->second;
        if (utime - co->LastRequest() > CONNECTIONTIMEOUT) {
            delete co;
            // map::erase doesn't return an iterator, unless
            // __GXX_EXPERIMENTAL_CXX0X__ is defined
            _connections.erase(ci++);
            ILOG(("Timeout, closed connection, id=%d, #connections=%zd",
                id,_connections.size()));
        }
        else ++ci;
    }
}

unsigned int Connections::num() const
{
    return _connections.size();
}

Connection::Connection(const connection * conn, int id)
:  _filegroup(0),_lastf(0),_id(id)
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

int Connection::add_var_group(const struct datadef *dd)
{
    _lastRequest = time(0);
    return _filegroup->add_var_group(dd);
}

Connection *Connections::operator[] (int i) const
{
#ifdef DEBUG
    DLOG(("i=%d,_connections.size()=%d", i, _connections.size()));
#endif
    std::map <int, Connection*>::const_iterator ci = _connections.find(i);
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

int Connection::put_rec(const datarec_float * writerec)
{
    NS_NcFile *f;
    _lastRequest = time(0);
    if (!(f = _filegroup->put_rec<datarec_float,float>(writerec, _lastf)))
        return -1;
    _lastf = f;

    return 0;
}

int Connection::put_rec(const datarec_int * writerec)
{
    NS_NcFile *f;
    _lastRequest = time(0);
    if (!(f = _filegroup->put_rec<datarec_int,int>(writerec, _lastf)))
        return -1;
    _lastf = f;

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

AllFiles::AllFiles(void)
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

FileGroup::FileGroup(const struct connection *conn):
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
    std::map<int,VariableGroup*>::iterator vi = _vargroups.begin();
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
    std::list < NS_NcFile * >::const_iterator ni;

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
    std::list < NS_NcFile * >::const_iterator ni;
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

    std::list < NS_NcFile * >::const_iterator ni;
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


NS_NcFile *FileGroup::get_file(double dtime)
{
    NS_NcFile *f = 0;
    std::list < NS_NcFile * >::iterator ni;
    std::list < NS_NcFile * >::const_iterator nie;

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

NS_NcFile *FileGroup::open_file(double dtime)
{
    int fileExists = 0;

    string fileName =
        build_name(_outputDir, _fileNameFormat, _fileLength, dtime);

    struct stat statBuf;
    if (!access(fileName.c_str(), F_OK)) {
        if (stat(fileName.c_str(), &statBuf) < 0) {
            PLOG(("%s: %m", fileName.c_str()));
            throw NS_NcFile::NetCDFAccessFailed();
        }
        if (statBuf.st_size > 0)
            fileExists = 1;
    }
#ifdef DEBUG
    DLOG(("NetCDF fileName =%s, exists=%d", fileName.c_str(), fileExists));
#endif

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
    DLOG(("NetCDF fileName =%s, CDLName=%s", fileName.c_str(),
                _CDLFileName.c_str()));
#endif

    // If file doesn't exist (or is size 0) then
    //   if a CDL file was specified, try to ncgen it.
    // If the file doesn't exist (or size 0) and it was not ncgen'd
    //   then do a create/replace.
    if (!fileExists &&
            !(_CDLFileName.length() > 0 && !access(_CDLFileName.c_str(), F_OK)
                && !ncgen_file(_CDLFileName, fileName)))
        openmode = NcFile::Replace;

    NS_NcFile *f = 0;
    try {
        f = new NS_NcFile(fileName.c_str(), openmode, _interval,
                _fileLength, dtime);
    }
    catch(NS_NcFile::NetCDFAccessFailed) {
        delete f;
        f = 0;
    }
    return f;
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
            errmsg += string(buf,0,l);
            if (errmsg.length() > 1024) break;
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
            errmsg += string(buf,0,l);
            if (errmsg.length() > 1024) break;
        }
        proc.wait(true, &status);
        if (WIFEXITED(status)) {
            if (WEXITSTATUS(status))
                WLOG(("ncgen exited with status=%d, err output=",
                            WEXITSTATUS(status)) << errmsg);
        } else if (WIFSIGNALED(status))
            WLOG(("ncgen received signal=%d, err output=",
                        WTERMSIG(status)) << errmsg);
    }
    catch(const nidas::util::IOException & e)
    {
        WLOG(("%s", e.what()));
    }
    return 0;
}

void FileGroup::close_old_files(void)
{
    NS_NcFile *f;
    time_t now = time(0);
    vector < Connection * >::iterator ic;
    Connection *cp;

    std::list < NS_NcFile * >::iterator ni;

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

    std::list < NS_NcFile * >::const_iterator ni;

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

    std::list < NS_NcFile * >::iterator ni, oldest;

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

int FileGroup::add_var_group(const struct datadef *dd)
{

    // check to see if this variable group is equivalent to
    // one we've received before.

    std::map<int,VariableGroup*>::iterator vi = _vargroups.begin();
    for ( ; vi != _vargroups.end(); ++vi) {
        int id = vi->first;
        VariableGroup* vg = vi->second;
        if (vg->same_var_group(dd))
            return id;
    }

    if (_vargroupId < 0) _vargroupId = 0;
    VariableGroup *vg = new VariableGroup(dd, _vargroupId, _interval);
    _vargroups[_vargroupId] = vg;

#ifdef DEBUG
    ILOG(("Created variable group %d", _vargroupId));
#endif

    return _vargroupId++;
}

VariableGroup::VariableGroup(const struct datadef * dd, int id, double finterval):
    _id(id)
{
    unsigned int i, j, n;
    Variable *v;
    unsigned int nv;

    nv = dd->fields.fields_len;
    _rectype = dd->rectype;
    _datatype = dd->datatype;
    _fillMissing = dd->fillmissingrecords;
    _floatFill = dd->floatFill;
    _intFill = dd->intFill;
    if (_datatype == NS_FLOAT)
        _intFill = 0;
    _interval = dd->interval;
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

    try {
        create_outvariables();
        check_counts_variable();
    }
    catch(BadVariableName & bvn) {
        PLOG(("Illegal variable name: %s", bvn.toString().c_str()));
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

void VariableGroup::create_outvariables(void) throw(BadVariableName)
{
    int nv = _invars.size();
    int i;
    Variable *v1;
    OutVariable *ov;
    string cntsattr;
    enum NS_datatype dtype;

    for (i = 0; i < nv; i++) {
        v1 = _invars[i];
        dtype = _datatype;
        _outvars.push_back(ov =
                new OutVariable(*v1, dtype, _floatFill,
                    _intFill));
    }
}

OutVariable *VariableGroup::
createCountsVariable(const string& cname) throw(BadVariableName)
{
    Variable v(cname);
    OutVariable *ov = new OutVariable(v, NS_INT, _floatFill, _intFill);
    ov->isCnts() = 1;
    // no short_name attribute
    ov->add_att("short_name", "");
    _outvars.push_back(ov);
    return ov;
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

    // looks to be the same, lets copy any missing attributes

    for (i = 0; i < nv; i++) {
        Variable *v = _invars[i];

        char *cp = ddp->fields.fields_val[i].units;
        if (cp) v->add_att("units", cp);

        unsigned int n = ddp->attrs.attrs_val[i].attrs.attrs_len;

        for (j = 0; j < n; j++) {
            str_attr *a = ddp->attrs.attrs_val[i].attrs.attrs_val + j;
#ifdef DEBUG
            DLOG(("%d %s %s", j, a->name, a->value));
#endif
            v->add_att(a->name, a->value);
        }
    }
    return 1;
}

Variable::Variable(const string& vname): _name(vname),_isCnts(0)
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

std::vector<std::string> Variable::get_attr_names() const
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
        float ff,
        int ll) throw(BadVariableName):
    Variable(v),_datatype(dtype), _countsvar(0),
    _floatFill(ff), _intFill(ll)
{
    string namestr = name();

    add_att("short_name", namestr);

    string::size_type ic;

    // convert dots, tics, commas, parens to underscores so that
    // it is a legal NetCDL name
    while((ic = namestr.find_first_of(".'(),*",0)) != string::npos)
        namestr[ic] = '_';

    // convert double underscores to one
    while((ic = namestr.find("__",0)) != string::npos)
        namestr = namestr.substr(0,ic) + namestr.substr(ic+1);

    // remove trailing _, unless name less than 3 chars
    if ((ic = namestr.length()) > 2 && namestr[ic-1] == '_')
        namestr = namestr.substr(0,ic-1);

    set_name(namestr);
}

const double NS_NcFile::minInterval = 1.e-5;

NS_NcFile::NS_NcFile(const string & fileName, enum FileMode openmode,
        double interval, double fileLength,
        double dtime):NcFile(fileName.c_str(), openmode),
    _fileName(fileName), _interval(interval), _lengthSecs(fileLength),
    _ttType(FIXED_DELTAT),_timesAreMidpoints(-1)
{

    if (!is_valid()) {
        PLOG(("NcFile %s: %s", _fileName.c_str(),
                    get_error_string().c_str()));
        throw NS_NcFile::NetCDFAccessFailed();
    }

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
                !_baseTimeVar->is_valid()) {
            PLOG(("add_var %s: %s %s", _fileName.c_str(), "base_time",
                        get_error_string().c_str()));
            throw NS_NcFile::NetCDFAccessFailed();
        }
        string since =
            nidas::util::UTime(0.0).format(true,
                    "seconds since %Y-%m-%d %H:%M:%S 00:00");
        if (!_baseTimeVar->add_att("units", since.c_str())) {
            PLOG(("add_att %s: %s %s %s", _fileName.c_str(),
                        _baseTimeVar->name(), "units",
                        get_error_string().c_str()));
            throw NS_NcFile::NetCDFAccessFailed();
        }
    }

    if (!(_recdim = rec_dim())) {
        if (!(_recdim = add_dim("time")) || !_recdim->is_valid()) {
            PLOG(("add_dim %s: %s %s", _fileName.c_str(), "time",
                        get_error_string().c_str()));
            // the NcFile has been constructed, and so if this exception is
            // thrown, it will be deleted, which will delete all the 
            // created dimensions and variables.  So we don't have to
            // worry about deleting _recdim here or the variables later
            throw NS_NcFile::NetCDFAccessFailed();
        }
    } else
        _nrecs = _recdim->size();

    if (!(_timeOffsetVar = get_var("time")) &&
            !(_timeOffsetVar = get_var("time_offset"))) {
        /* New variable */
        if (!(_timeOffsetVar = NcFile::add_var("time", ncDouble, _recdim))
                || !_timeOffsetVar->is_valid()) {
            PLOG(("add_var %s: %s %s", _fileName.c_str(), "time",
                        get_error_string().c_str()));
            throw NS_NcFile::NetCDFAccessFailed();
        }
        _timeOffsetType = _timeOffsetVar->type();
    } else {
        _timeOffsetType = _timeOffsetVar->type();
        if (_nrecs > 0) {
            // Read last available time_offset, double check it
            long nrec = _nrecs - 1;
            NcValues *val;
            _timeOffsetVar->set_rec(nrec);
            if (!(val = _timeOffsetVar->get_rec())) {
                PLOG(("get_rec(%d) %s: %s %s",
                            nrec, _fileName.c_str(), _timeOffsetVar->name(),
                            get_error_string().c_str()));
                throw NS_NcFile::NetCDFAccessFailed();
            }
            switch (_timeOffsetType) {
            case ncFloat:
                _timeOffset = val->as_float(0L);
                break;
            case ncDouble:
                _timeOffset = val->as_double(0L);
                break;
            default:
                PLOG(("%s: unsupported type for time variable",
                            _fileName.c_str()));
                throw NS_NcFile::NetCDFAccessFailed();
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
        if (!_timeOffsetVar->add_att("units", since.c_str())) {
            PLOG(("add_att %s: %s %s %s", _fileName.c_str(),
                        _timeOffsetVar->name(), "units",
                        get_error_string().c_str()));
            throw NS_NcFile::NetCDFAccessFailed();
        }
    } else
        delete timeOffsetUnitsAtt;

    if (_ttType == FIXED_DELTAT) {
        NcAtt *intervalAtt;
        if (!(intervalAtt = _timeOffsetVar->get_att("interval(sec)"))) {
            if (!_timeOffsetVar->add_att("interval(sec)",_interval)) {
                PLOG(("add_att %s: %s %s %s", _fileName.c_str(),
                            _timeOffsetVar->name(), "interval(sec)",
                            get_error_string().c_str()));
                throw NS_NcFile::NetCDFAccessFailed();
            }
        } else
            delete intervalAtt;
    }

    /* Write base time */
    if (!_baseTimeVar->put(&_baseTime, &_nrecs)) {
        PLOG(("put basetime %s: %s", _fileName.c_str(),
                    get_error_string().c_str()));
        throw NS_NcFile::NetCDFAccessFailed();
    }
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
    int j;

    ILOG(("Closing: %s", _fileName.c_str()));
    std::map<int,NS_NcVar**>::iterator vi = _vars.begin();
    for ( ; vi != _vars.end(); ) {
        int id = vi->first;
        NS_NcVar** vars = vi->second;
        for (j = 0; j < _nvars[id]; j++)
            delete vars[j];
        delete [] vars;
        _vars.erase(vi++);
    }
}

const std::string & NS_NcFile::getName() const
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

NS_NcVar **NS_NcFile::get_vars(VariableGroup * vgroup)
{

    NS_NcVar **vars;

    int groupid = vgroup->getId();

    // variables have been initialzed for this VariableGroup
    std::map<int,NS_NcVar**>::iterator vi = _vars.find(groupid);
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
            if (!_dims[_ndims] || !_dims[_ndims]->is_valid()) {
                PLOG(("get_dim %s: %s %s", _fileName.c_str(), _dimNames[i].c_str(),
                            get_error_string().c_str()));
                return 0;
            }
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
    vars = new NS_NcVar *[nv];

    int numCounts = 0;
    int countsIndex = -1;
    string countsAttrNameFromFile;
    string countsAttrNameFromOVs;
    bool countsAttrNameMisMatch = false;

    for (int iv = 0; iv < nv; iv++) {
        vars[iv] = 0;
        OutVariable *ov = vgroup->get_var(iv);
#ifdef DEBUG
        DLOG(("checking %s", ov->name().c_str()));
#endif
        if (!ov->isCnts()) {
            string cntsAttr;
            if ((cntsAttr = ov->att_val("counts")).length() > 0) {
                if (countsAttrNameFromOVs.length() > 0) {
                    if (countsAttrNameFromOVs != cntsAttr)
                        PLOG(("var %s: counts attribute=%s, no match with %s",
                                    ov->name().c_str(),cntsAttr.c_str(),countsAttrNameFromOVs.c_str()));
                } else
                    countsAttrNameFromOVs = cntsAttr;
            }
            NS_NcVar *nsv;
            if ((nsv = add_var(ov)) == 0) {
                PLOG(("Failed to add variable %s to %s",
                            ov->name().c_str(),_fileName.c_str()));
                continue;
            }

            NcAtt *att = nsv->get_att("counts");
            if (att) {
                if (att->type() == ncChar && att->num_vals() > 0) {
                    const char *cname = att->as_string(0);
                    if (countsAttrNameFromFile.length() == 0)
                        countsAttrNameFromFile = cname;
                    else {
                        // this shouldn't happen - there should be one counts var per grp
                        if (countsAttrNameFromFile != string(cname)) {
                            PLOG(("%s: var %s counts attribute=%s, no match with %s",
                                        getName().c_str(),nsv->name(),cname,countsAttrNameFromFile.c_str()));
                            countsAttrNameMisMatch = true;
                        }
                    }
                    delete [] cname;
                }
                delete att;
            }

            vars[iv] = nsv;
        } else {
            // a counts variable
            if (numCounts++ == 0)
                countsIndex = iv;
        }
    }

    //
    // Scenarios:
    //    no variables in file, so no attributes, likely a new file
    //        create counts variable
    //    variables, and counts attributes
    //      counts var exists in file
    //        does file counts variable match OutVariable counts var name?
    //            yes: no problem
    //            no: change name of OutVariable
    //      counts var doesn't exist in file
    //            typically shouldn't happen, since attributes existed
    //            

    if (numCounts > 0) {
        if (numCounts > 1)
            PLOG(("multiple counts variables in variable group"));

#ifdef DEBUG
        DLOG(("countsIndex=%d", countsIndex));
#endif
        OutVariable *cv = vgroup->get_var(countsIndex);
        assert(cv->isCnts());
#ifdef DEBUG
        cerr << "countsAttrNameFromFile=" << countsAttrNameFromFile <<
            endl;
        cerr << "counts var=" << cv->name() << endl;
#endif

        if (countsAttrNameFromFile.length() == 0 || countsAttrNameMisMatch) {
            // no counts attributes found in file for variables in group
            // check that OutVariable counts attributes
            // matches OutVariable name
            if (cv->name() != countsAttrNameFromOVs)
                PLOG(("counts attributes don't match counts variable name for group %s: %s and %s",
                            cv->name().c_str(), countsAttrNameFromOVs.c_str(), cv->name().c_str()));
            NcVar *ncv;
            if ((ncv = get_var(cv->name().c_str()))) {
                // counts variable exists in file, it must be for another group
                // create another counts variable
                ostringstream ost;
                for (int j = 1; get_var(cv->name().c_str()); j++) {
                    ost.str("");
                    ost << cv->name() << '_' << j;
                }
                string countsname = ost.str();

                // new name
                ILOG(("new name for counts variable %s: %s\n",
                            cv->name().c_str(), countsname.c_str()));
                if (countsname != cv->name())
                    cv->set_name(countsname);

                // set our counts attributes (on the file too)
                for (int iv = 0; iv < nv; iv++) {
                    OutVariable *ov = vgroup->get_var(iv);
                    if (!ov->isCnts()) {
                        ov->add_att("counts", cv->name());
                        NS_NcVar *nsv = vars[iv];
                        // set in file too
                        if (nsv)
                            nsv->add_att("counts", cv->name().c_str());
                    }
                }
                vars[countsIndex] = add_var(cv);
            } else {
                // counts variable doesn't exist
                if ((vars[countsIndex] = add_var(cv)) == 0)
                    PLOG(("failed to create counts variable %s",
                                cv->name().c_str()));
                for (int iv = 0; iv < nv; iv++) {
                    OutVariable *ov = vgroup->get_var(iv);
                    if (!ov->isCnts()) {
                        ov->add_att("counts", cv->name());
                        NS_NcVar *nsv = vars[iv];
                        // set in file too
                        nsv->add_att("counts", cv->name().c_str());
                    }
                }
            }
        } else {
            // counts attributes found in file
            if (cv->name() != countsAttrNameFromFile)
                cv->set_name(countsAttrNameFromFile.c_str());
            if ((vars[countsIndex] = add_var(cv)) == 0)
                PLOG(("failed to create counts variable %s", cv->name().c_str()));
            for (int iv = 0; iv < nv; iv++) {
                OutVariable *ov = vgroup->get_var(iv);
                if (!ov->isCnts()) {
                    ov->add_att("counts", cv->name());
                    NS_NcVar *nsv = vars[iv];
                    // set in file too
                    nsv->add_att("counts", cv->name().c_str());
                }
            }
        }
    }
#ifdef DEBUG
    DLOG(("added vars"));
#endif
    _vars[groupid] = vars;
    _nvars[groupid] = nv;
    sync();
    return vars;
}

void VariableGroup::check_counts_variable() throw(BadVariableName)
{
    OutVariable *ov;

    // scan over all counts variables
    set < OutVariable * >cntsVars;
    set < string > cntsNames;

    for (int i = 0; i < num_vars(); i++) {
        ov = get_var(i);
        if (ov->isCnts())
            cntsVars.insert(ov);
        else if (ov->att_val("counts").length() > 0)
            cntsNames.insert(ov->att_val("counts"));
    }

    if (cntsNames.size() == 0)
        return;                 // no counts

    // add a counts OutVariable for each cntsName that doesn't
    // exist in cntsVars. Update counts_variable() member.
    set < string >::const_iterator ni = cntsNames.begin();

    for (; ni != cntsNames.end(); ++ni) {
        set < OutVariable * >::const_iterator ci = cntsVars.begin();
        for (; ci != cntsVars.end(); ++ci)
            if ((*ci)->name() == *ni) break;

        if (ci == cntsVars.end()) {
            OutVariable *cv = createCountsVariable(*ni);
            // update counts_variable() 
            for (int i = 0; i < num_vars(); i++) {
                ov = get_var(i);
                if (!ov->isCnts() && ov->att_val("counts").length() > 0
                        && ov->att_val("counts") == *ni)
                    ov->counts_variable() = cv;
            }
        }
    }
}

NS_NcVar *NS_NcFile::add_var(OutVariable * v)
{
    NcVar *var;
    NS_NcVar *fsv;
    int isCnts = v->isCnts();

    const string& varName = v->name();
    const string& shortName = v->att_val("short_name");

    // No matching variables found, create new one
    if (!(var = find_var(v))) {
        if (!(var =
                    NcFile::add_var(varName.c_str(), (NcType) v->data_type(), _ndims,
                        &_dims.front())) || !var->is_valid()) {
            PLOG(("add_var %s: %s %s", _fileName.c_str(), varName.c_str(),
                        get_error_string().c_str()));
            PLOG(("shortName=%s", shortName.c_str()));
            PLOG(("define_mode=%d", define_mode()));
            PLOG(("data_type=%d", v->data_type()));
            PLOG(("ndims=%d", _ndims));
#ifdef DEBUG
            for (unsigned int i = 0; i < _ndims; i++)
                DLOG(("dims=%d id=%d size=%d", i, _dims[i]->id(),
                            _dims[i]->size()));
#endif
            goto error;
        }
    }

    if (add_attrs(v, var) < 0)
        goto error;

    // double check ourselves
    if (!check_var_dims(var)) {
        PLOG(("%s: wrong dimensions for variable %s",
                    getName().c_str(),varName.c_str()));
        goto error;
    }
    fsv = new NS_NcVar(var, &_dimIndices.front(), _ndims_req, v->floatFill(),
            v->intFill(), isCnts);
    // sync();
    return fsv;
error:
    delete var;
    return 0;
}

/*
 * return 0: OK
 * return -1: error
 */
int NS_NcFile::add_attrs(OutVariable * v, NcVar * var)
{
    // add attributes if they don't exist in file, otherwise leave them alone

    // bug in netcdf, if units is empty string "", result in 
    // netcdf file is some arbitrary character.

    NcAtt *nca;
    if (!(nca = var->get_att("_FillValue"))) {
        switch (v->data_type()) {
        case NS_INT:
            if (!var->add_att("_FillValue",v->intFill())) {
                PLOG(("%s: %s: add_att %s: %s %s",
                            getName().c_str(),var->name(), "_FillValue",
                            get_error_string().c_str()));
                delete nca;
                return -1;
            }
            break;
        case NS_FLOAT:
            if (!var->add_att("_FillValue", v->floatFill())) {
                PLOG(("%s: %s: add_att %s: %s",
                            getName().c_str(),var->name(), "_FillValue",
                            get_error_string().c_str()));
                delete nca;
                return -1;
            }
            break;
        }
    } else
        delete nca;

#ifdef DO_UNITS_SEPARATELY
    if (v->units().length() > 0) {
        NcValues *uvals = 0;
        char *units = 0;
        NcBool res = 1;
        nca = var->get_att("units");
        if (nca) {
            uvals = nca->values();
            if (uvals && nca->num_vals() >= 1 && nca->type() == ncChar)
                units = uvals->as_string(0);
        }
        if (!units || strcmp(units, v->units().c_str())) {
#ifdef DEBUG
            DLOG(("new units=%s, old units=%s,len=%d",
                        v->units().c_str(),
                        (units ? units : "none"), (nca ? nca->num_vals() : 0)));
#endif
            if (!(res = var->add_att("units", v->units().c_str()))) {
                PLOG(("%s: %s: add_att: %s=%s %s",
                            getName().c_str(),var->name(),
                            "units",v->units().c_str(),
                            get_error_string().c_str()));
            }
        }
        delete nca;
        delete uvals;
        delete [] units;
        if (!res) return -1;
    }
#endif

    // all string attributes
    vector<string> attrNames = v->get_attr_names();
    for (unsigned int i = 0; i < attrNames.size(); i++) {
        string aname = attrNames[i];
        string aval = v->att_val(aname);
        if (aval.length() > 0) {
            NcValues *uvals = 0;
            char* faval = 0;
            NcBool res = 1;
            nca = var->get_att(aname.c_str());
            if (nca) {
                uvals = nca->values();
                if (uvals && nca->num_vals() >= 1 && nca->type() == ncChar)
                    faval = uvals->as_string(0);
            }
            if (!faval || string(faval) != aval) {
                if (!(res = var->add_att(aname.c_str(), aval.c_str()))) {
                    PLOG(("%s: %s: add_att: %s=%s: %s",
                                getName().c_str(),var->name(),
                                aname.c_str(),aval.c_str(),
                                get_error_string().c_str()));
                }
            }
            delete nca;
            delete uvals;
            delete [] faval;
            if (!res) return -1;
        }
    }

    return 0;
}

//
// Find the variable.
// First lookup by the NetCDF variable name we've created for it.
// If a variable is found by that name, check that the short_name
// attribute is correct.  If it isn't, then someone has been
// renaming variables and we have to create a new variable name.
//
NcVar *NS_NcFile::find_var(OutVariable * v)
{
    int i;
    NcVar *var;
    const string& varName = v->name();
    const string& shortName = v->att_val("short_name");

    int varFound = 0;
    bool nameExists = false;

    NcAtt *att;
    char *attString;

    if ((var = get_var(varName.c_str()))) {
        nameExists = 1;
        if (shortName.length() == 0)
            varFound = 1;
        // Check its short_name attribute
        else if ((att = var->get_att("short_name"))) {
            attString = 0;
            if (att->type() == ncChar && att->num_vals() > 0 &&
                    (attString = att->as_string(0)) &&
                    !strcmp(attString, shortName.c_str()))
                varFound = 1;
            delete att;
            delete [] attString;
        }

        if (!varFound)
            var = 0;
    }
    //
    // If we can't find a variable with the same NetCDF variable name,
    // and a matching short_name, look through all other variables for
    // one with a matching short_name
    //
    for (i = 0; shortName.length() > 0 && !var && i < num_vars(); i++) {
        var = get_var(i);
        // Check its short_name attribute
        if ((att = var->get_att("short_name"))) {
            attString = 0;
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
        WLOG(("%s: variable %s is of wrong type",
                    _fileName.c_str(), var->name()));
        var = 0;
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
            //
            // Variable with matching short_name, but wrong dimensions
            // We'll change the short_name attribute of the offending
            // variable to "name_old" and create a new variable.
            string tmpString = shortName + "_old";
            if (!var->add_att("short_name", tmpString.c_str())) {
                PLOG(("add_att %s: %s %s %s", _fileName.c_str(),
                            var->name(), "short_name",
                            get_error_string().c_str()));
            }
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

long NS_NcFile::put_time(double timeoffset, const char *varname)
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
        if (!(val = _timeOffsetVar->get_rec())) {
            PLOG(("get_rec(%d) %s: %s %s",
                        _nrecs, _fileName.c_str(), _timeOffsetVar->name(),
                        get_error_string().c_str()));
            throw NS_NcFile::NetCDFAccessFailed();
        }
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
            PLOG(("Invalid timeoffset=%f, file timeOffset=%f, nrec=%d, _nrecs=%d, _interval=%f, var=%s", (double) timeoffset, (double) tmpOffset, nrec, _nrecs, _interval, varname));
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
        if (!i) {
            PLOG(("time_offset put_rec %s: %s", _fileName.c_str(),
                        get_error_string().c_str()));
            return -1;
        }
    }
#ifdef DEBUG
    DLOG(("after fill timeoffset = %f, timeOffset=%f,nrec=%d, _nrecs=%d,interval=%f",
                (double)timeoffset,(double)_timeOffset, nrec, _nrecs, (double)_interval));
#endif

#ifdef LAST_TIME_CHECK
    if (_ttType == FIXED_DELTAT && nrec == _nrecs - 1) {
        if (fabs((double) _timeOffset - (double) timeoffset) >
                _interval * 1.e-3) {
            PLOG(("Invalid timeoffset = %f, file timeOffset=%f,nrec=%d, _nrecs=%d, interval=%f, var=%s", timeoffset, _timeOffset, nrec, _nrecs, _interval, varname));
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

NS_NcVar::NS_NcVar(NcVar * var, int *dimIndices, int ndimIndices, float ffill, int ifill, int iscnts):
    _var(var), _ndimIndices(ndimIndices), _isCnts(iscnts), _floatFill(ffill),
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
    _userid(0),_groupid(0),_daemon(true),_logLevel(defaultLogLevel)
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
                if (getgrnam_r(optarg,&groupinfo,&strbuf.front(),strbuf.size(),&gptr) < 0) {
                    cerr << "cannot determine group id for " << optarg << ": " << strerror(errno) << endl;
                    usage(argv[0]);
                    return 1;
                }
                else if (!gptr) {
                    cerr << "cannot find group " << optarg << endl;
                    usage(argv[0]);
                    return 1;
                }
                else if (gptr != 0) {
                    _suppGroupIds.push_back(groupinfo.gr_gid);
                    _suppGroupNames.push_back(optarg);
                }
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
                if (getpwnam_r
                        (optarg, &pwdbuf, &strbuf.front(), nb, &result) < 0) {
                    cerr << "Unknown user: " << optarg << endl;
                    usage(argv[0]);
                    return 1;
                }
                _userid = pwdbuf.pw_uid;
                _groupid = pwdbuf.pw_gid;
                struct group groupinfo;
                struct group *gptr;
                nb = sysconf(_SC_GETGR_R_SIZE_MAX);
                if (nb < 0) nb = 4096;
                strbuf.resize(nb);
                if (getgrgid_r(_groupid,&groupinfo,&strbuf.front(),strbuf.size(),&gptr) < 0) {
                    cerr << "cannot determine group for gid " << _groupid << ": " << strerror(errno) << endl;
                }
                else if (gptr != 0) _groupname = groupinfo.gr_name;
                else _groupname = "unknown";
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
                    LOG_CONS | LOG_PID,
                    LOG_LOCAL5);
    } else
        logger = nidas::util::Logger::createInstance(&std::cerr);

    logger->setScheme(nidas::util::LogScheme("nc_server").addConfig(lc));
}

void NcServerApp::run(void)
{
    ILOG(("nc_server starting"));
    SVCXPRT *transp;

    (void) pmap_unset(NETCDFSERVERPROG, NETCDFSERVERVERS);

    transp = svctcp_create(RPC_ANYSOCK, 0, 0);
    if (transp == NULL) {
        PLOG(("cannot create tcp service."));
        exit(1);
    }
    if (!svc_register(transp, NETCDFSERVERPROG, NETCDFSERVERVERS,
                netcdfserverprog_1, IPPROTO_TCP)) {
        PLOG(("Unable to register (NETCDFSERVERPROG=%x, NETCDFSERVERVERS, tcp): %m", NETCDFSERVERPROG));
        exit(1);
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
    PLOG(("svc_run returned"));
    exit(1);
    /* NOTREACHED */
}

int main(int argc, char **argv)
{
    NcServerApp ncserver;
    int res;
    if ((res = ncserver.parseRunstring(argc, argv)))
        return res;
    ncserver.setup();
    ncserver.run();
}
