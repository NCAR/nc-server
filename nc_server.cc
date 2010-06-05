//
// Todo:
//	When NS_NcFile::_interval==0, must do binary search to
//		figure out what record to write to.  One should
//		save the last written time to avoid reading/searching,
//		and perhaps read the next time if appropriate.
//
//              Copyright (C) by UCAR

/*
 * NetCDF file server.  Implemented with remote procedure calls (RPC).
 * Right now it only supports file writing, not reading.
 *
 * A theoretically unlimited number of clients can request a connection
 * to this server.  Upon connection, the server returns a unique integer
 * connectionId to each client, which the client must use in each
 * RPC call to the server to indicate which client is calling.
 *
 * After connection, the clients make one or more RPC
 * calls to tell the server what kind of data records they will be
 * sending, what the time resolution is, what the names of the
 * data variables are, and strings indicating the data units,
 * what format to use for the NetCDF file names, and the time length
 * of the NetCDF files.
 *
 * Much of the complication in this program is connected with
 * supporting time domain statistics products, like covariances,
 * trivariances, and third and fourth moments.  
 *
 * For example, if a client requests the server write trivariance
 * data records, it only has to pass the names of the individual
 * data variables.  The server then generates NetCDF variable names for all the
 * trivariance combinations of these variables.
 *
 * Preferably the variable names should follow the ASTER syntax,
 * as a series of words separated by periods ".", with the first
 * word being the common notation for the data quantity, e.g.
 * u,v,w for wind components,  t for temperature, rh, etc.
 * The other words in the variable name can further identify
 * the  variable, the instrument name, location, tower name, etc.
 *
 * NetCDF names compatible with the NetCDL syntax will be generated
 * for the statistics products.  NetCDL does not allow periods
 * in the names.  In general, the periods are replaced by underscores.
 * When creating NetCDF names for covariance, trivariance, and
 * higher moments, the first word is repeated to indicate the
 * moment.
 *
 * For example, a NetCDF variable name for a a trivariance of three
 * data variable: u.sonic.ht.tower, w.sonic.ht.tower, and t.inst.ht.tower,
 * would be something like "u_w_t_ht_tower".  For the data variable
 * in a statistics group, an attempt is made to determine a common
 * suffix, in this case "ht.tower", which is then used in the
 * NetCDF variable names of all variables in the group.
 *
 * Once the NetCDF variables are created, the client simply makes
 * successive RPC calls to write data records.  One thing the
 * client and server must agree upon is the sequence for the
 * data variable in the statistics products.
 */

#define _REENTRANT

#include <unistd.h>
#include <limits.h>
#include <stdlib.h>
#include <assert.h>

#include <stdio.h>
#include <errno.h>
#include <math.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <sys/signal.h>

#include <netcdf.h>

#include <iostream>
#include <string>
#include <vector>
#include <set>

using namespace std;

#include "nc_server.h"

#include <nidas/util/Process.h>

const int Connections::CONNECTIONTIMEOUT = 43200;
const int Component::NWORDALLOC = 5;
const int FileGroup::FILEACCESSTIMEOUT = 900;
const int FileGroup::MAX_FILES_OPEN = 16;

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

  sprintf(procname,"/proc/%d/stat",pid);
  FILE *fp = fopen(procname,"r");

  // do man proc on Linus
  if (fscanf(fp,"%*d (%*[^)]) %*c%*d%*d%*d%*d%*d%*u%*u%*u%*u%*u%*d%*d%*d%*d%*d%*d%*u%*u%*d%u%*u%*u%*u%*u%*u%*u%*u%*d%*d%*d%*d%*u",&vsize) != 1) vsize = 0;
  fclose(fp);
  return vsize;
#else

  int fd;

  sprintf(procname,"/proc/%d",pid);
  if ((fd = open(procname,O_RDONLY)) < 0) return 0;

  // do "man -s 4 proc" on Solaris
  struct prstatus pstatus;
  if (ioctl(fd,PIOCSTATUS,&pstatus) < 0) { close(fd); return 0; }

  // struct prpsinfo pinfo;
  // if (ioctl(fd,PIOCPSINFO,&pinfo) < 0) { close(fd); return 0; }

//  cout << pinfo.pr_pid << ' ' << pinfo.pr_size << ' ' << pinfo.pr_rssize << ' ' << pinfo.pr_bysize << ' ' << pinfo.pr_byrssize << ' ' << pstatus.pr_brksize << ' ' << pstatus.pr_stksize << endl;

  close(fd);
  return pstatus.pr_brksize;
#endif
}

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


NcError ncerror(NcError::silent_nonfatal);

#ifndef SVR4
// attempt at supporting strtok_r on systems that don't have it.
char *strtok_r(char *s1, const char *s2, char ** lasts)
{

  char *cp;
  if (!s1) s1 = *lasts;
  if (s1) {
    if ((cp = strpbrk(s1,s2))) {
      *cp = '\0';
      *lasts = cp + 1;
    }
    else *lasts = 0;
  }
  return s1;
}
#endif

void nc_shutdown(int i) {
  if (i) PLOG(("nc_server exiting abnormally: exit(%d)",i));
  else ILOG(("nc_server normal exit",i));
  exit(i);
}

Connections::Connections(void)
{
  nidas::util::UTime::setTZ("GMT");
}
Connections *Connections::_instance = 0;

Connections *Connections::Instance ()
{
  if (_instance == 0) _instance = new Connections;
  return _instance;
}

Connections::~Connections(void)
{
  for (int i=0; i < _connections.size(); i++) delete _connections[i];
}
int Connections::OpenConnection(const struct connection *conn)
{
  int iconn;
  Connection *cp=0;

#ifdef DEBUG
  DLOG(("_connections.size()=%d",_connections.size()));
#endif

  CloseOldConnections();

  try {
    cp = new Connection(conn);
  }
  catch (Connection::InvalidOutputDir) {
    delete cp;
    return -1;
  }

  for (iconn=0; iconn < _connections.size(); iconn++)
    /* found available entry */
    if (!_connections[iconn]) break;

  if (iconn == _connections.size()) _connections.push_back(cp);
  else _connections[iconn] = cp;
  
  ILOG(("Cpened Connection, Id=%d, heap=%d",iconn,heap()));
  return iconn;
}
int Connections::CloseOldConnections()
{
  time_t utime;
  int i;
  /*
   * Release timed-out connections.
   */
  utime = time(0);
  for (i=0; i < _connections.size(); i++)
    if (_connections[i] && (utime - _connections[i]->LastRequest() > CONNECTIONTIMEOUT)) {
      ILOG(("Connection Timeout, Id=%d",i));
      CloseConnection(i);
    }

  return i;
}
int Connections::CloseConnection(int connectionId)
{
  Connection *conn;

  if ((conn = _connections[connectionId]) == 0) {
    PLOG(("Invalid Connection Id=%d",connectionId));
    return -1;
  }
  ILOG(("Closing Connection, Id=%d, heap=%d",connectionId,heap()));
  delete conn;
  _connections[connectionId] = 0;
  return 0;
}
int Connections::num() const
{
  int i,n;
  for (i=n=0; i< _connections.size(); i++) if (_connections[i]) n++;
  return n;
}

Connection::Connection(const connection *conn)
	: _lastf(0),_filegroup(0)
{

  AllFiles *allfiles = AllFiles::Instance();
  _lastRequest = time(0);

  _filegroup = allfiles->get_file_group(conn);

  _filegroup->add_connection(this);

}
Connection::~Connection(void)
{
  if (_lastf) _lastf->sync();
  _filegroup->remove_connection(this);

  AllFiles* allfiles = AllFiles::Instance();
  allfiles->close_old_files();
}

int Connection::add_var_group(const struct datadef *dd)
{
  _lastRequest = time(0);
  return _filegroup->add_var_group(dd);
}
  
Connection *& Connections::operator[](int i)
{
#ifdef DEBUG
  DLOG(("i=%d,_connections.size()=%d",i,_connections.size()));
#endif
  static Connection *conn = 0;
  if (i < _connections.size()) return _connections[i];
  else return conn;
}

NS_NcFile* Connection::last_file() const
{
  return _lastf;
}


void Connection::unset_last_file()
{
  _lastf = 0;
}

int Connection::put_rec(const datarec_float *writerec)
{
  NS_NcFile *f;
  _lastRequest = time(0);
  if (!(f = _filegroup->put_rec(writerec,_lastf)))
    return -1;
  _lastf = f;

  return 0;
}

int Connection::put_rec(const datarec_long *writerec)
{
  NS_NcFile *f;
  _lastRequest = time(0);
  if (!(f = _filegroup->put_rec(writerec,_lastf)))
    return -1;
  _lastf = f;

  return 0;
}
//
// Cache the history records, to be written when we close files.
//
int Connection::put_history(const string& h)
{
  _history += h;
  if (_history.length() > 0 && _history[_history.length() - 1] != '\n')
    _history += '\n';
  _lastRequest = time(0);
  return 0;
}

AllFiles::AllFiles(void)
{
#ifdef SVR4
  (void) signal(SIGHUP,(SIG_PF)hangup);
  (void) signal(SIGTERM,(SIG_PF)shutdown);
  (void) signal(SIGINT,(SIG_PF)shutdown);
#else
  (void) signal(SIGHUP,hangup);
  (void) signal(SIGTERM,shutdown);
  (void) signal(SIGINT,shutdown);
#endif
}
AllFiles::~AllFiles()
{
  int i;
  for (i = 0; i < _filegroups.size(); i++) delete _filegroups[i];
}

AllFiles *AllFiles::_instance = 0;

AllFiles *AllFiles::Instance ()
{
  if (_instance == 0) _instance = new AllFiles;
  return _instance;
}

// Close all open files
void AllFiles::hangup(int sig)
{
  ILOG(("Hangup signal received, closing all open files and old connections."));

  AllFiles* allfiles = AllFiles::Instance();
  allfiles->close();

  Connections* connections = Connections::Instance();
  connections->CloseOldConnections();
  ILOG(("%d current connections",connections->num()));

#ifdef SVR4
  (void) signal(SIGHUP,(SIG_PF)hangup);
#else
  (void) signal(SIGHUP,hangup);
#endif
}

void AllFiles::shutdown(int sig)
{
  ILOG(("Signal %d received, shutting down."));

  AllFiles* allfiles = AllFiles::Instance();
  allfiles->close();
  Connections* connections = Connections::Instance();
  connections->CloseOldConnections();
  nc_shutdown(0);
}

//
// Look through existing file groups to find one with
// same output directory and file name format
// If not found, allocate a new group
//
FileGroup * AllFiles::get_file_group(const struct connection *conn)
{
  FileGroup *p;
  vector<FileGroup *>::iterator ip;

#ifdef DEBUG
  DLOG(("filegroups.size=%d",_filegroups.size()));
#endif

  for (ip = _filegroups.begin(); ip < _filegroups.end(); ++ip) {
    p = *ip;
    if (p) {
      if (!p->match(conn->outputdir,conn->filenamefmt)) continue;
      if (conn->interval != p->interval()) throw FileGroup::InvalidInterval();
      if (conn->filelength != p->length()) throw FileGroup::InvalidFileLength();
      return p;
    }
  }

  // Create new file group
  p = new FileGroup(conn);

  if (ip < _filegroups.end()) *ip = p;
  else _filegroups.push_back(p);

  return p;
}
int AllFiles::num_files() const
{
  int i,n;
  for (i = n = 0; i < _filegroups.size(); i++)
    if (_filegroups[i]) n += _filegroups[i]->num_files();
  return n;
}
void AllFiles::close()
{
  int i,n=0;

  // close all filegroups.  If a filegroup is not active, delete it
  for (i = 0; i < _filegroups.size(); i++) {
    if (_filegroups[i]) {
      _filegroups[i]->close();
      if (!_filegroups[i]->active()) {
	delete _filegroups[i];
	_filegroups[i] = 0;
      }
      else {
	DLOG(("filegroup %d has %d open files, %d var groups",
		n,_filegroups[i]->num_files(),_filegroups[i]->num_var_groups()));
	n++;
      }
    }
  }
  DLOG(("%d current file groups, heap=%d",n,heap()));
}

void AllFiles::sync()
{
  // sync all filegroups.
  for (int i = 0; i < _filegroups.size(); i++) {
    if (_filegroups[i]) _filegroups[i]->sync();
  }
}

void AllFiles::close_old_files(void)
{
  int i,n=0;

  for (i = 0; i < _filegroups.size(); i++) {
    if (_filegroups[i]) {
      _filegroups[i]->close_old_files();
      if (!_filegroups[i]->active()) {
	delete _filegroups[i];
	_filegroups[i] = 0;
      }
      else {
	DLOG(("filegroup %d has %d open files, %d var groups",
		n,_filegroups[i]->num_files(),_filegroups[i]->num_var_groups()));
	n++;
      }
    }
  }
  DLOG(("%d current file groups, heap=%d",n,heap()));
}

void AllFiles::close_oldest_file(void)
{
  int i;
  time_t lastaccess = time(0) - 10;
  time_t accesst;
  FileGroup *fg;
  int oldgroup=-1;

  for (i = 0; i < _filegroups.size(); i++)
    if ((fg = _filegroups[i]) && (accesst = fg->oldest_file()) < lastaccess) {
      lastaccess = accesst;
      oldgroup = i;
    }
  if (oldgroup >= 0) _filegroups[oldgroup]->close_oldest_file();
  
}

FileGroup::FileGroup(const struct connection *conn):
	_interval(conn->interval),_fileLength(conn->filelength)
{

#ifdef DEBUG
  DLOG(("creating FileGroup, dir=%s,file=%s",
  	conn->outputdir,conn->filenamefmt));
#endif

  if (access(conn->outputdir,F_OK)) {
    PLOG(("%s: %m",conn->outputdir));
    throw Connection::InvalidOutputDir();
  }

  _outputDir = conn->outputdir;

  _fileNameFormat = conn->filenamefmt;

  _CDLFileName = conn->cdlfile;

  _uniqueSuffixChar = 'A';

#ifdef DEBUG
  DLOG(("created FileGroup, dir=%s,file=%s",
  	_outputDir.c_str(),_fileNameFormat.c_str()));
#endif
}
FileGroup::~FileGroup(void)
{
  close();
  for (int i = 0; i < _vargroups.size(); i++) delete _vargroups[i];
}
int FileGroup::match(const string& dir, const string& file)
{
  /*
   * We want to avoid having nc_server having the same file
   * open twice - which causes major problems.
   */
  if (_fileNameFormat != file) return 0;	// file formats don't match
  if (_outputDir == dir) return 1;	// file formats and directory
                                        // names match, must be same

  /* If directory names don't match they could still point
   * to the same directory.  Do an inode comparison.
   */
  struct stat sbuf1,sbuf2;
  if (::stat(_outputDir.c_str(),&sbuf1) < 0) {
    PLOG(("Cannot stat %s: %m",_outputDir.c_str()));
    return 0;
  }
  if (::stat(dir.c_str(),&sbuf2) < 0) {
    PLOG(("Cannot stat %s: %m",dir.c_str()));
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
  if (sbuf1.st_ino != sbuf2.st_ino) return 0;

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
  vector<char> tmpname(tmpfile.begin(),tmpfile.end());
  tmpname.push_back('\0');

  int fd;
  if ((fd = ::mkstemp(&tmpname.front())) < 0) {
    PLOG(("Cannot create tmpfile on %s: %m",_outputDir.c_str()));
    return match;
  }
  ::close(fd);

  tmpfile = dir + string(tmpname.begin() + _outputDir.length(),
    tmpname.end()-1);
  
  fd = ::open(tmpfile.c_str(), O_RDONLY);
  // if file successfully opens, then same directory
  if ((match = (fd >= 0))) ::close(fd);
  if (::unlink(&tmpname.front()) < 0) {
    PLOG(("Cannot delete %s: %m",&tmpname.front()));
  }
  return match;
}

// delete all NS_NcFile objects
void FileGroup::close()
{
  int i;
  std::list<NS_NcFile*>::const_iterator ni;

  for (i = 0; i < _connections.size(); i++) {
    _connections[i]->unset_last_file();
    // write history
    for (ni = _files.begin(); ni != _files.end(); ni++)
       (*ni)->put_history(_connections[i]->get_history());
  }

  while (_files.size() > 0) _files.pop_back();
}

// sync all NS_NcFile objects
void FileGroup::sync()
{
  std::list<NS_NcFile*>::const_iterator ni;
  for (ni = _files.begin(); ni != _files.end(); ni++) (*ni)->sync();
}
void FileGroup::add_connection(Connection *cp)
{
  _connections.push_back(cp);
}
void FileGroup::remove_connection(Connection *cp)
{
  vector<Connection *>::iterator ic;
  Connection *p;

  std::list<NS_NcFile*>::const_iterator ni;
  // write history
  for (ni = _files.begin(); ni != _files.end(); ni++)
     (*ni)->put_history(cp->get_history());

  for (ic = _connections.begin(); ic < _connections.end(); ic++) {
    p = *ic;
    if (p == cp) {
      _connections.erase(ic);	// warning ic is invalid after erase
      break;
    }
  }
}


NS_NcFile * FileGroup::get_file(double dtime)
{
  NS_NcFile *f=0;
  std::list<NS_NcFile*>::iterator ni;
  std::list<NS_NcFile*>::const_iterator nie;

  nie = _files.end();

  for (ni = _files.begin(); ni != nie; ni++)
    if ((*ni)->EndTimeGT(dtime)) break;

  /*
   * At this point:
   *	ni == nie, no files open
   *		   no file with an endTime > dtime, ie all earlier
   *	or *ni->endTime > dtime
   */

  AllFiles *allfiles = AllFiles::Instance();
  if (ni == nie) {
#ifdef DEBUG
    DLOG(("new NS_NcFile: %s %s",_outputDir.c_str(),_fileNameFormat.c_str()));
#endif
    if ((f = open_file(dtime))) _files.push_back(f);
    close_old_files();
    if (allfiles->num_files() > MAX_FILES_OPEN) allfiles->close_oldest_file();
  }
  else if (!(f=*ni)->StartTimeLE(dtime)) {
#ifdef DEBUG
    DLOG(("new NS_NcFile: %s %s",_outputDir.c_str(),_fileNameFormat.c_str()));
#endif

    // If the file length is less than 0, then the file has "infinite"
    // length.  We will have problems here, because you can't
    // insert records, only overwrite or append.
    // if (length() < 0) return(0);

    if ((f = open_file(dtime))) _files.insert(ni,f);
    close_old_files();
    if (allfiles->num_files() > MAX_FILES_OPEN) allfiles->close_oldest_file();
  }
  return f;
}

NS_NcFile* FileGroup::open_file(double dtime)
{
  int fileExists = 0;

  string fileName = build_name(_outputDir,_fileNameFormat,_fileLength,dtime);

  struct stat statBuf;
  if (!access(fileName.c_str(),F_OK)) {
    if (stat(fileName.c_str(),&statBuf) < 0) {
      PLOG(("%s: %m",fileName.c_str()));
      throw NS_NcFile::NetCDFAccessFailed();
    }
    if (statBuf.st_size > 0) fileExists = 1;
  }

#ifdef DEBUG
  DLOG(("NetCDF fileName =%s, exists=%d",fileName.c_str(),fileExists));
#endif

  if (fileExists) {
    if (!check_file(fileName)) {
      string badName = fileName + ".bad";
      DLOG(("Renaming corrupt file: %s to %s",fileName.c_str(),badName.c_str()));
      if (!access(badName.c_str(),F_OK) && unlink(badName.c_str()) < 0)
	      PLOG(("unlink %s, %m",badName.c_str()));
      if (link(fileName.c_str(),badName.c_str()) < 0)
	      PLOG(("link %s %s, %m",fileName.c_str(),badName.c_str()));
      if (unlink(fileName.c_str()) < 0) PLOG(("unlink %s, %m",fileName.c_str()));
      fileExists = 0;
    }
  }

#ifdef __GNUC__
  enum NcFile::FileMode openmode = NcFile::Write;
#else
  enum FileMode openmode = NcFile::Write;
#endif

#ifdef DEBUG
  DLOG(("NetCDF fileName =%s, CDLName=%s",fileName.c_str(),_CDLFileName.c_str()));
#endif

  // If file doesn't exist (or is size 0) then
  //   if a CDL file was specified, try to ncgen it.
  // If the file doesn't exist (or size 0) and it was not ncgen'd
  //   then do a create/replace.
  if (! fileExists &&
    !(_CDLFileName.length() > 0 && !access(_CDLFileName.c_str(),F_OK) &&
      !ncgen_file(_CDLFileName.c_str(),fileName.c_str()))) openmode = NcFile::Replace;

  NS_NcFile* f = 0;
  try {
    f = new NS_NcFile(fileName.c_str(),openmode,_interval,_fileLength, dtime);
  }
  catch (NS_NcFile::NetCDFAccessFailed) {
    delete f;
    f = 0;
  }
  return f;
}

string FileGroup::build_name(const string& outputDir, const string& nameFormat,double fileLength,double dtime) const
{

  // If file length is 31 days, then align file times on months.
  int monthLong = fileLength == 31 * 86400;

  if (monthLong) {
    struct tm tm;
    nidas::util::UTime(dtime).toTm(true,&tm);

    tm.tm_sec = tm.tm_min = tm.tm_hour = 0;
    tm.tm_mday = 1;	// first day of month
    tm.tm_yday = 0;

    nidas::util::UTime ut(true,&tm);
    dtime = ut.toDoubleSecs();
  }
  else if (fileLength > 0) dtime = floor(dtime/fileLength) * fileLength;

  nidas::util::UTime utime(dtime);

  return outputDir + '/' + utime.format(true,nameFormat);
}

int FileGroup::check_file(const string& fileName) const
{
  bool fileok = false;

  try {
      vector<string> args;
      args.push_back("nccheck");
      args.push_back(fileName);
      nidas::util::Process nccheck = nidas::util::Process::spawn("nccheck",args);
      int status;
      string errmsg;
      nccheck.errStream() >> errmsg;
      nccheck.wait(true,&status);
      if (WIFEXITED(status)) {
          if (WEXITSTATUS(status))
              WLOG(("nccheck exited with status=%d, err output=",
                WEXITSTATUS(status)) << errmsg);
          else fileok = true;
      }
      else if (WIFSIGNALED(status))
          WLOG(("nccheck received signal=%d, err output=",
                WTERMSIG(status)) << errmsg);
  }
  catch(const nidas::util::IOException& e) {
      WLOG(("%s",e.what()));
  }

  return fileok;
}

int FileGroup::ncgen_file(const string& CDLFileName, const string& fileName) const
{
  try {
      vector<string> args;
      args.push_back("ncgen");
      args.push_back("-o");
      args.push_back(fileName);
      args.push_back(CDLFileName);
      nidas::util::Process ncgen = nidas::util::Process::spawn("ncgen",args);
      int status;
      string errmsg;
      ncgen.errStream() >> errmsg;
      ncgen.wait(true,&status);
      if (WIFEXITED(status)) {
          if (WEXITSTATUS(status))
              WLOG(("ncgen exited with status=%d, err output=",
                WEXITSTATUS(status)) << errmsg);
      }
      else if (WIFSIGNALED(status))
          WLOG(("ncgen received signal=%d, err output=",
                WTERMSIG(status)) << errmsg);
  }
  catch(const nidas::util::IOException& e) {
      WLOG(("%s",e.what()));
  }
  return 0;
}

void FileGroup::close_old_files(void)
{
  NS_NcFile *f;
  time_t now = time(0);
  vector<Connection *>::iterator ic;
  Connection *cp;

  std::list<NS_NcFile*>::iterator ni;

  for (ni = _files.begin(); ni != _files.end(); ) {
    f = *ni;
    if (now - f->LastAccess() > FILEACCESSTIMEOUT) {
      for (ic = _connections.begin(); ic < _connections.end(); ic++) {
	cp = *ic;
	if (cp->last_file() == f) cp->unset_last_file();
	// write history
        f->put_history(cp->get_history());
      }
      ni = _files.erase(ni);
      delete f;
    }
    else ++ni;
  }
}

time_t FileGroup::oldest_file(void)
{
  NS_NcFile *f;
  time_t lastaccess = time(0);

  std::list<NS_NcFile*>::const_iterator ni;

  for (ni = _files.begin(); ni != _files.end(); ) {
    f = *ni;
    if (f->LastAccess() < lastaccess) lastaccess = f->LastAccess();
    ++ni;
  }
  return lastaccess;
}
void FileGroup::close_oldest_file(void)
{
  NS_NcFile *f;
  time_t lastaccess = time(0);
  vector<Connection *>::iterator ic;
  Connection *cp;

  std::list<NS_NcFile*>::iterator ni,oldest;

  oldest = _files.end();

  for (ni = _files.begin(); ni != _files.end(); ) {
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
      if (cp->last_file() == f) cp->unset_last_file();
      // write history
      f->put_history(cp->get_history());
    }
    _files.erase(oldest);
    delete f;
  }
}

int FileGroup::add_var_group(const struct datadef *dd)
{

  int i;
  int n = _vargroups.size();

  // check to see if this variable group is equivalent to
  // one we've received before.

  for (i = 0; i < n; i++) if (_vargroups[i]->same_var_group(dd)) return i;
  
  VariableGroup * vg = new VariableGroup(dd,n,_interval);

  _vargroups.push_back(vg);

#ifdef DEBUG
  DLOG(("Created variable group %d",n));
#endif

  return n;
}


NS_NcFile* FileGroup::put_rec(const datarec_float *writerec,NS_NcFile *f)
{
  int ngroup = writerec->datarecId;
  double dtime = writerec->time;

  if (ngroup >= _vargroups.size()) {
    PLOG(("Invalid variable group number: %d",ngroup));
    return 0;
  }

  /* Check if last file is still current */
  if ( !(f && (f->StartTimeLE(dtime) && f->EndTimeGT(dtime) ))) {
#ifdef DEBUG
    DLOG(("time not contained in current file"));
#endif
    if (f) f->sync();
    if (!(f = get_file(dtime)) && ncerror.get_err() == NC_ENFILE) {
      // Too many files open
      AllFiles::Instance()->close_oldest_file();
      f = get_file(dtime);
    }
    if (!f) return 0;
  }

#ifdef DEBUG
  DLOG(("Writing Record, ngroup=%d,f=%x",ngroup,f));
#endif

  if (!f->put_rec(writerec,_vargroups[ngroup],dtime)) f = 0;
  return f;
}

NS_NcFile* FileGroup::put_rec(const datarec_long *writerec,NS_NcFile *f)
{
  int ngroup = writerec->datarecId;
  double dtime = writerec->time;

  if (ngroup >= _vargroups.size()) {
    PLOG(("Invalid variable group number: %d",ngroup));
    return 0;
  }

  /* Check if last file is still current */
  if ( !(f && (f->StartTimeLE(dtime) && f->EndTimeGT(dtime) ))) {
#ifdef DEBUG
    DLOG(("time not contained in current file"));
#endif
    if (f) f->sync();
    if (!(f = get_file(dtime)) && ncerror.get_err() == NC_ENFILE) {
      // Too many files open
      AllFiles::Instance()->close_oldest_file();
      f = get_file(dtime);
    }
    if (!f) return 0;
  }

#ifdef DEBUG
  DLOG(("Writing Record, ngroup=%d,f=%x",ngroup,f));
#endif

  if (!f->put_rec(writerec,_vargroups[ngroup],dtime)) f = 0;
  return f;
}


VariableGroup::VariableGroup(const struct datadef *dd,int igroup,
	double finterval):_ngroup(igroup)
{
  Component **nextmatches;
  int currentComp,nmatch;
  int suffixlen,suffixok;
  int i,j,k,l,n;
  InVariable *v;
  int nv;

  nv = dd->fields.fields_len;
  _rectype = dd->rectype;
  _datatype = dd->datatype;
  _fillMissing = dd->fillmissingrecords;
  _floatFill = dd->floatFill;
  _longFill = dd->longFill;
  if (_datatype == NS_FLOAT) _longFill = 0;
  _interval = dd->interval;
  _nsamples = (int) floor (finterval / _interval + .5);
  if (_nsamples < 1) _nsamples = 1;
  if (_interval < NS_NcFile::minInterval) _nsamples = 1;

  _ndims = dd->dimensions.dimensions_len + 2;
  _dimnames = new char*[_ndims];
  _dimsizes = new long[_ndims];

  _dimnames[0] = new char[5];
  strcpy(_dimnames[0],"time");
  _dimsizes[0] = NC_UNLIMITED;

  _dimnames[1] = new char[7];
  strcpy(_dimnames[1],"sample");
  _dimsizes[1] = _nsamples;

  for (i=2; i < _ndims; i++) {
    l = strlen(dd->dimensions.dimensions_val[i-2].name) + 1;
    _dimnames[i] = new char[l];
    strcpy(_dimnames[i],dd->dimensions.dimensions_val[i-2].name);
    _dimsizes[i] = dd->dimensions.dimensions_val[i-2].size;
  }

  _ncomponents = 0;
  for (i = 0; i < nv; i++) {
    _invars.push_back(v = new InVariable(dd->fields.fields_val[i].name));
#ifdef DEBUG
      DLOG(("%s: %d",v->name(),i));
#endif
    char *cp = dd->fields.fields_val[i].units;

    // if (!strncmp(v->name(),"chksumOK",8))
      // DLOG(("%s units=%s",v->name(),cp ? cp : "none"));

    if (cp && strlen(cp) > 0) v->add_att("units",cp); 
    v->add_att("short_name","");	// dummy short_name attribute
    _ncomponents += v->ncomponents();

    n = dd->attrs.attrs_val[i].attrs.attrs_len;

#ifdef DEBUG
    DLOG(("%d %s ncomponents=%d",i,v->name(),v->ncomponents()));
#endif

    for (j = 0; j < n; j++) {
      str_attr *a = dd->attrs.attrs_val[i].attrs.attrs_val + j;
#ifdef DEBUG
      DLOG(("%d %s %s",j,a->name,a->value));
#endif
      v->add_att(a->name,a->value);
    }
  }
  _components = new Component *[_ncomponents];
  k = 0;
  for (i = 0; i < nv; i++)
    for (j = 0; j < _invars[i]->ncomponents(); j++)
      _components[k++] = _invars[i]->component(j);

  /*
   * Find longest common suffix.
   */
  suffixlen = _components[0]->nwords() - 1;
  for (i=1; i < _ncomponents; i++) {
    if ((l = (_components[i]->nwords() - 1)) < suffixlen) suffixlen = l;
#ifdef VARNAME_DEBUG
      DLOG(("suffixlen=%d",suffixlen));
#endif
  }
  while(suffixlen > 0) {
    suffixok = 1;
    k = _components[0]->nwords();
    for(i=1; suffixok && i < _ncomponents; i++) {
      n = _components[i]->nwords();
      for (j=1; suffixok && j <= suffixlen; j++)
	if (strcmp(_components[0]->word(k-j),_components[i]->word(n-j)))
		suffixok = 0;
    }
    if (suffixok) break;
    suffixlen--;
  }
  if (suffixlen == 0) {
    // No common suffix, set it to an empty string
    for (i=0; i < _ncomponents; i++)
      _components[i]->nwordsuffix() = 0;
    _suffix = new char[1];
    _suffix[0] = 0;
  }
  else {
    for (i=0; i < _ncomponents; i++) _components[i]->nwordsuffix() = suffixlen;
    l = 0;
    for (j = _components[0]->nwords() - suffixlen;
	j < _components[0]->nwords(); j++)
      		l += strlen(_components[0]->word(j)) + 1;
    _suffix = new char[l];
    _suffix[0] = 0;
    for (j = _components[0]->nwords() - suffixlen;
	j < _components[0]->nwords(); j++) {
      if (_suffix[0]) strcat(_suffix,".");
      strcat(_suffix,_components[0]->word(j));
    }
  }
#ifdef DEBUG
  DLOG(("VariableGroup: suffix=%s, ncomponents=%d, interval=%f, nsamples=%d",
	_suffix,_ncomponents,_interval,_nsamples));
  for (i=0; i < _ndims; i++)
    DLOG(("group dim %s, size=%d",
    _dimnames[i], _dimsizes[i]));
#endif

  for (i=0; i < _ncomponents; i++) {
    _components[i]->make_prefix();
    _components[i]->make_middle();
    _components[i]->set_suffix(_suffix);
  }

  try {
    create_outvariables();
  }
  catch (BadVariableName &bvn) {
    PLOG(("Illegal variable name: %s", bvn.toString().c_str()));
  }

#ifdef DEBUG
  DLOG(("created outvariables"));
#endif

#ifdef DEBUG
  DLOG(("adding vars"));
#endif

  check_counts_variable();
}

VariableGroup::~VariableGroup(void)
{
  int i;
  for (i = 0; i < _invars.size(); i++) delete _invars[i];
  for (i = 0; i < _outvars.size(); i++) delete _outvars[i];

  delete [] _components;

  for (i = 0; i < _ndims; i++) delete [] _dimnames[i];
  delete [] _dimnames;
  delete [] _dimsizes;

  delete [] _suffix;

}
int VariableGroup::num_dims(void) const
{
  return _ndims;
}
long VariableGroup::dim_size(int i) const
{
  if (i < _ndims) return _dimsizes[i];
  return -1;
}
const char *VariableGroup::dim_name(int i) const
{
  if (i < _ndims) return _dimnames[i];
  return 0;
}

void VariableGroup::create_outvariables(void) throw(BadVariableName)
{
  int nCheck;
  int nv = _invars.size();
  int needcnts = 0;

  switch (_rectype) {
  case NS_TRIVAR:
    needcnts = 1;
    _ncombs = 	nv +				// means
		((nv + 1) * nv) / 2 +		// covariances
		NumCombTrivar(nv) +		// trivariances
		nv;				// 4th moments


    CreateMomentsVariables(nv,1);
    CreateCovarianceVariables(nv);
    CreateTrivarianceVariables(nv);
    CreateMomentsVariables(nv,4);
    break;
  case NS_PRUNEDTRIVAR:
    needcnts = 1;
    _ncombs =	nv +				// means
		((nv + 1) * nv) / 2 +		// covariances
	 	nv * 5 - 7 +			// trivariances
		nv;				// 4th moments
    CreateMomentsVariables(nv,1);
    CreateCovarianceVariables(nv);
    CreatePrunedTrivarianceVariables(nv);
    CreateMomentsVariables(nv,4);
    break;

  case NS_COVN:
    needcnts = 1;
  case NS_COV:
    _ncombs =	nv +				// means
		((nv + 1) * nv) / 2 +		// covariances
		2 * nv;				// 3rd & 4th moments

    CreateMomentsVariables(nv,1);
    CreateCovarianceVariables(nv);
    CreateMomentsVariables(nv,3);
    CreateMomentsVariables(nv,4);
    break;
  case NS_FLUX:
    needcnts = 1;
    /*
    _ncombs =	nv +				// means
		((nv + 1) * nv) / 2 -		// covariances
		((nv-4) * (nv-3)) / 2 +		// except scalarXscalar cross
		2 * nv;				// 3rd & 4th moments
    */
    _ncombs = 7 * nv - 6;

    CreateMomentsVariables(nv,1);
    CreateFluxVariables(nv);
    CreateMomentsVariables(nv,3);
    CreateMomentsVariables(nv,4);
    break;
  case NS_RFLUX:
    needcnts = 1;
    _ncombs = 3 * nv + 6;

    // means of wind components only
    CreateMomentsVariables(3,1);
    CreateRFluxVariables(nv);
    CreateMomentsVariables(3,3);
    CreateMomentsVariables(3,4);
    break;
  case NS_SFLUX:
    needcnts = 1;
    _ncombs = nv + 3;

    CreateMomentsVariables(1,1);	// means of first variable only
    CreateSFluxVariables(nv);
    CreateMomentsVariables(1,3);
    CreateMomentsVariables(1,4);
    break;

  case NS_MEANN:
    needcnts = 1;
  case NS_MEAN:		// means
    _ncombs = nv;
    CreateMomentsVariables(nv,1);
    break;
  case NS_MINIMUM:	// minimums
    _ncombs = nv;
    CreateMinimumVariables(nv);
    break;
  case NS_MAXIMUM:	// maximums
    _ncombs = nv;
    CreateMaximumVariables(nv);
    break;
  case NS_TIMESERIES:	// means
    _ncombs = nv;
    CreateTimeSeriesVariables(nv);
    break;
  }

  nCheck = _outvars.size();
  if (nCheck != _ncombs) {
    PLOG((
      "Programming error: nCheck (%d) is not equal to ncombs (%d)",
      nCheck,_ncombs));
    nc_shutdown(1);
  }
  if (needcnts) {
    char tmpname[64];
    OutVariable *ov = CreateCountsVariable(counts_name(tmpname));
    for (int i = 0; i < _ncombs; i++) {
      if (!_outvars[i]->isCnts()) {
	_outvars[i]->add_att("counts",ov->name());
	_outvars[i]->counts_variable() = ov;
      }
    }
  }
}

OutVariable* VariableGroup::CreateCountsVariable(const char *cname) throw(BadVariableName)
{
  InVariable v(cname);
  OutVariable *ov = new OutVariable(&v,NS_LONG,_floatFill,_longFill);
  _outvars.push_back(ov);
  ov->isCnts() = 1;
  return ov;
}
void VariableGroup::CreateTimeSeriesVariables(int nv) throw(BadVariableName)
{
  int i,j;
  InVariable *v1,*v2;
  OutVariable *ov,*ov2;
  const char *cntsattr;
  enum NS_datatype dtype;

  for (i=0; i < nv; i++) {
    int iscnts = 0;
    v1 = _invars[i];
    dtype = _datatype;

    // Check each variable to see if it is actually a counts variable
    for (j=0; j < nv; j++) {
      if (j != i) {
	v2 = _invars[j];
	if ((cntsattr = v2->att_val("counts")) &&
		!strcmp(cntsattr,v1->name())) {
	  iscnts = 1;
	  dtype = NS_LONG;
	  break;
	}
      }
    }
    _outvars.push_back(ov = new OutVariable(v1,dtype,_floatFill,_longFill));
    if (iscnts) {
      ov->isCnts() = iscnts;
      // delete short_name attr if it exists
      ov->add_att("short_name",NULL);
    }
  }
  //
  // Create pointers to counts variables
  for (i=0; i < nv; i++) {
    ov = _outvars[i];
    if (ov->isCnts()) {
      v1 = _invars[i];
      for (j=0; j < nv; j++) {
	if (j != i) {
	  ov2 = _outvars[j];
	  // compare against original (InVariable) name of counts var
	  if ((cntsattr = ov2->att_val("counts")) &&
		  !strcmp(cntsattr,v1->name()))
	    ov2->counts_variable() = ov;
	}
      }
    }
  }
}
void VariableGroup::CreateMinimumVariables(int nv) throw(BadVariableName)
{
  CreateMomentsVariables(nv,1);
  for (int i=0; i < nv; i++)
    _outvars[i]->append_name("_min");
}

void VariableGroup::CreateMaximumVariables(int nv) throw(BadVariableName)
{
  CreateMomentsVariables(nv,1);
  for (int i=0; i < nv; i++)
    _outvars[i]->append_name("_max");
}

void VariableGroup::CreateCovarianceVariables(int nv) throw(BadVariableName)
{
  int i,j;
  InVariable *v1,*v2;
  OutVariable *v;

  for (i=0; i < nv; i++) {
    v1 = _invars[i];
    for (j=i; j < nv; j++) {
      v2 = _invars[j];
      _outvars.push_back(v = new OutVariable(v1,v2,_datatype,_floatFill,_longFill));
    }
  }
}

void VariableGroup::CreateTrivarianceVariables(int nv) throw(BadVariableName)
{
  int i,j,k;
  InVariable *v1,*v2,*v3;
  OutVariable *v;

  for (i=0; i < nv; i++) {
    v1 = _invars[i];
    for (j=i; j < nv; j++) {
      v2 = _invars[j];
      for (k=j; k < nv; k++) {
	v3 = _invars[k];
	_outvars.push_back(v = new OutVariable(v1,v2,v3,_datatype,_floatFill,_longFill));
      }
    }
  }
}

void VariableGroup::CreatePrunedTrivarianceVariables(int nv) throw(BadVariableName)
{
  int i,j,k;
  InVariable *v1,*v2,*v3;
  OutVariable *v;

  /* A pruned trivariance.
   *  u,v,w are wind components
   *  s       is a scalar
   *  x       is any of the above
   * 
   * These combinations are used:
   *                ncomb
   *  xxx 3rd mom   nv
   *  wss           nv-3
   *  [uv][uvw]w    5       (uvw, vuw are duplicates)
   *  [uvw]ws       3 * (nv - 3)
   * 
   *  total:         5 * nv - 7
   * 
   *  all covariances and all means
   */

  // 3rd moments
  for (i=0; i < nv; i++) {
    v1 = _invars[i];
    _outvars.push_back(v = new OutVariable(v1,v1,v1,_datatype,_floatFill,_longFill));
  }

  // ws^2 trivariances
  for (i=2,j = 3; j < nv; j++) {
    v1 = _invars[i];
    v2 = _invars[j];
    _outvars.push_back(v = new OutVariable(v1,v2,v2,_datatype,_floatFill,_longFill));
  }

  // [uv][uvw]w trivariances
  for (i=0,k=2; i < 2; i++) {
    v1 = _invars[i];
    v3 = _invars[k];
    for (j = i; j < 3; j++) {
      v2 = _invars[j];
      _outvars.push_back(v = new OutVariable(v1,v2,v3,_datatype,_floatFill,_longFill));
    }
  }
  // uws, vws and wws trivariances
  for (i=0,j=2; i < 3; i++) {
    v1 = _invars[i];
    v2 = _invars[j];
    for (k = 3; k < nv; k++) {
      v3 = _invars[k];
      _outvars.push_back(v = new OutVariable(v1,v2,v3,_datatype,_floatFill,_longFill));
    }
  }
}

void VariableGroup::CreateMomentsVariables(int nv,int nmom) throw(BadVariableName)
{
  int i;
  InVariable *v1;
  OutVariable *v;

  for (i=0; i < nv; i++) {
    v1 = _invars[i];
    switch (nmom) {
      case 1:
	_outvars.push_back(v = new OutVariable(v1,_datatype,_floatFill,_longFill));
	break;
      case 2:
	_outvars.push_back(v = new OutVariable(v1,v1,_datatype,_floatFill,_longFill));
	break;
      case 3:
	_outvars.push_back(v = new OutVariable(v1,v1,v1,_datatype,_floatFill,_longFill));
	break;
      case 4:
	_outvars.push_back(v = new OutVariable(v1,v1,v1,v1,_datatype,_floatFill,_longFill));
	break;
    }
  }
}
void VariableGroup::CreateFluxVariables(int nv) throw(BadVariableName)
{
  int i,j;
  InVariable *v1,*v2;
  OutVariable *v;

  // covariances (including scalar variances), but no scalar cross-covariances
  for (i=0; i < nv; i++) {
    v1 = _invars[i];
    for (j = i; j < (i > 2 ? i+1 : nv); j++) {
      v2 = _invars[j];
      _outvars.push_back(v = new OutVariable(v1,v2,_datatype,_floatFill,_longFill));
    }
  }
}
void VariableGroup::CreateRFluxVariables(int nv) throw(BadVariableName)
{
  int i,j;
  InVariable *v1,*v2;
  OutVariable *v;

  // covariances, but no scalar cross-covariances or variances
  for (i=0; i < nv && i < 3; i++) {
    v1 = _invars[i];
    for (j = i; j < nv; j++) {
      v2 = _invars[j];
      _outvars.push_back(v = new OutVariable(v1,v2,_datatype,_floatFill,_longFill));
    }
  }

}
void VariableGroup::CreateSFluxVariables(int nv) throw(BadVariableName)
{
  int i,j;
  InVariable *v1,*v2;
  OutVariable *v;

  // covariance combinations of first component with itself and others
  // flip names, so that  c'u' is called u'c'
  i = 0;
    v1 = _invars[i];
    for (j = i; j < nv; j++) {
      v2 = _invars[j];
      _outvars.push_back(v = new OutVariable(v2,v1,_datatype,_floatFill,_longFill));
    }
}

/*
 * A challenge to anyone listening:
 * how to do this without looping, using some expression in n?
 */
int VariableGroup::NumCombTrivar(int n) const
{
  int i,j,k,nc;

  for (i=nc=0; i < n; i++)
    for (j=i; j < n; j++)
      for (k=j; k < n; k++) nc++;
  return nc;
}

char *VariableGroup::counts_name(char *name)
{
  if (strlen(_suffix) > 0) sprintf(name,"counts_%s",_suffix);
  else sprintf(name,"counts");
  return name;
}
//
// Was this VariableGroup created from an identical datadef
//
int VariableGroup::same_var_group(const struct datadef *ddp) const
{

  int i,j;
  int nv = ddp->fields.fields_len;

  if (_invars.size() != nv) return 0;

  if (_interval != ddp->interval) return 0;

  // Check that dimensions are the same.  There can be extra
  // rightmost (trailing) dimensions of 1.
  // This does not check the dimension names
  // It also does not check the time or sample dimensions
  //
  for (i = 0,j=2; i < ddp->dimensions.dimensions_len && j < _ndims; i++,j++)
    if (ddp->dimensions.dimensions_val[i].size != _dimsizes[j]) return 0;
  for (; i < ddp->dimensions.dimensions_len; i++)
    if (ddp->dimensions.dimensions_val[i].size != 1) return 0;
  for (; j < _ndims; j++)
    if (_dimsizes[j] != 1) return 0;

  if (_rectype != ddp->rectype) return 0;
  if (_datatype != ddp->datatype) return 0;

  for (i = 0; i < nv; i++)
      if (strcmp(_invars[i]->name(),ddp->fields.fields_val[i].name)) return 0;

  // looks to be the same, lets copy any missing attributes

  for (i = 0; i < nv; i++) {
    InVariable *v = _invars[i];

    char *cp = ddp->fields.fields_val[i].units;
    const char *cpe = v->att_val("units");
    if (cp && (!cpe || strcmp(cp,cpe))) v->add_att("units",cp); 

    int n = ddp->attrs.attrs_val[i].attrs.attrs_len;

    for (j = 0; j < n; j++) {
      str_attr *a = ddp->attrs.attrs_val[i].attrs.attrs_val + j;
#ifdef DEBUG
      DLOG(("%d %s %s",j,a->name,a->value));
#endif
      if (v->att_val(a->name) == 0) 
	  v->add_att(a->name,a->value);
    }
  }
  return 1;
}

#ifdef MESS_WITH_SUFFIX
//
// Is this VariableGroup similar to a datadef.
// Similar means:
//	same data interval
//	same dimensions 
//	if either one is a NS_TIMESERIES and they have the same suffix
//	if they are the same record type and data type, and is the first
//		variable the same?
//
int VariableGroup::similar_var_group(const VariableGroup *vgp) const
{

  int i;
  if (_interval != vgp->_interval) return 0;

  // Check that dimensions are the same.  There can be extra
  // rightmost (trailing) dimensions of 1.
  // This does not check the dimension names
  for (i = 0; i < vgp->_ndims && i < _ndims; i++)
    if (vgp->_dimsizes[i] != _dimsizes[i]) return 0;
  for (; i < vgp->_ndims; i++)
    if (vgp->_dimsizes[i] != 1) return 0;
  for (; i < _ndims; i++)
    if (_dimsizes[i] != 1) return 0;

  if (_datatype != vgp->_datatype) return 0;

  // either one a timeseries and same suffix
  if ((_rectype == NS_TIMESERIES || vgp->_rectype == NS_TIMESERIES ) &&
	!strcmp(_suffix,vgp->_suffix)) return 1;

  if (_rectype != vgp->_rectype) return 0;
  if (strcmp(_invars[0]->name(),vgp->_invars[0]->name())) return 0;
  return 1;
}
#endif

VariableBase::VariableBase(const char *vname):
	_isCnts(0)
{
  _name = new char[strlen(vname) + 1];
  strcpy(_name,vname);
}

VariableBase::VariableBase(const VariableBase *v): _isCnts(v->_isCnts)
{
  _name = new char[strlen(v->_name) + 1];
  strcpy(_name,v->_name);

  copy_atts(v);
}

VariableBase::~VariableBase(void)
{
  int i;
  for (i = 0; i < _strAttrs.size(); i++) delete _strAttrs[i];
  delete [] _name;
}

void VariableBase::set_name(const char *n)
{
  delete [] _name;
  _name = new char[strlen(n) + 1];
  strcpy(_name,n);
}


void VariableBase::add_att(const char *name, const char *val)
{
  int i;
  for (i = 0; i < _strAttrs.size(); i++)
    if (!strcmp(_strAttrs[i]->name(),name)) break;
  if (i < _strAttrs.size()) {
    delete _strAttrs[i];
    _strAttrs[i] = new VarAttr<char *>(name,(char *)val);
  }
  else _strAttrs.push_back(new VarAttr<char *>(name,(char *)val));
}

const char *VariableBase::att_val(const char *name) const
{
  int i;
  for (i = 0; i < _strAttrs.size(); i++)
    if (!strcmp(_strAttrs[i]->name(),name)) break;
  if (i < _strAttrs.size()) return _strAttrs[i]->val();
  return 0;
}
void VariableBase::copy_atts(const VariableBase *v)
{
  for (int i = 0; i < v->num_atts(); i++) {
    const VarAttr<char *> *att = v->get_att(i);
    add_att(att->name(),att->val());
  }
}

InVariable::InVariable(const char *vname):
	VariableBase(vname), _ncomponents(0),_components(0)
{
  // names are alphanumeric. Periods are used to separate portions
  // of a name.
  //
  //	P
  //	P.2m
  //
  // Special characters are single quotes "'", parenthesis and periods.
  //
  // quotes are used to indicate variances, covariances, higher moments:
  //
  //	w'w'
  //	w't'
  //	w't'.9m
  //	w't'.(ati,air).9m	covariance of w.ati.9m and t.air.9m
  //	

  char *cp;
  char *cplast;
  char *cname;
  int i;

  char **suffixes=0,**tmpsuffixes;
  int nsuffixes=0,ns;

  char *tmpstring = new char[strlen(_name) + 1];
  strcpy(tmpstring,_name);

  // look after last single quote
  char *trailing_junk = strrchr(tmpstring,'\'');
  char *paren_string;
  int ltj = 0;

  if (trailing_junk) {
    *trailing_junk++ = 0;	// write null at last quote

    // break apart   (a,b,c).xxx syntax
    if ((paren_string = strchr(trailing_junk,'('))) {
      *paren_string++ = 0;	// null out open paren
      if ((trailing_junk = strchr(paren_string,')'))) *trailing_junk++ = 0;
				// null out close paren
      // strtok_r will return  "a" and "b" when parsing ",a,b,"
      // we want "", "a", "b", ""
      for(;;) {
	tmpsuffixes = new char*[nsuffixes+1];
	for (i=0; i < nsuffixes; i++) tmpsuffixes[i] = suffixes[i];
	delete [] suffixes;
	suffixes = tmpsuffixes;
	suffixes[nsuffixes++] = paren_string;
	if (!(paren_string = strchr(paren_string,','))) break;
	*paren_string++ = 0;
      }
    }
  }
  if (trailing_junk) ltj = strlen(trailing_junk);

  // Split Field at colons or tics
  for (cp=tmpstring;; cp=0,_ncomponents++) {

    if ((cname = strtok_r(cp,"':",&cplast)) == 0) break;

    if (strlen(cname) > 0) {
      Component **tmpcomponents;
      tmpcomponents = new Component*[_ncomponents+1];
      for (i = 0; i < _ncomponents; i++) tmpcomponents[i] = _components[i];
      delete [] _components; _components = tmpcomponents;
      int ls = 0;
      if (nsuffixes > 0) {
	ns = _ncomponents >= nsuffixes ? nsuffixes - 1 : _ncomponents;
	ls = strlen(suffixes[ns]);
      }
      char *np = new char[strlen(cname)+ltj+ls+2];
      strcpy(np,cname);
      if (ls > 0) {
	strcat(np,".");
	strcat(np,suffixes[ns]);
      }
      if (trailing_junk) strcat(np,trailing_junk);

      _components[_ncomponents] = new Component(np);
      delete [] np;
    }
  }
  delete [] tmpstring;
  delete [] suffixes;
#ifdef VARNAME_DEBUG
  DLOG(("Field=%s,ncomponents=%d",_name,_ncomponents));
#endif
}

InVariable::~InVariable(void)
{
  int i;
  for (i = 0; i < _ncomponents; i++) delete _components[i];
  delete [] _components;
}


OutVariable::OutVariable(const InVariable *v,NS_datatype dtype,
	float ff,NSlong ll) throw(BadVariableName) :
	VariableBase(v),_datatype(dtype),_countsvar(0),
	_floatFill(ff),_longFill(ll)
{
  int i;
  vector<const Component *> comps;


  for (i = 0; i < v->ncomponents(); i++)
    comps.push_back((const Component *)v->component(i));

  make_name(comps);

  vector<const VariableBase *> vars;
  vars.push_back((const VariableBase*) v);
  make_units(vars);
}

OutVariable::OutVariable(const InVariable *v,const InVariable *v2,
	NS_datatype dtype,float ff,NSlong ll) throw(BadVariableName) :
	VariableBase(v),_datatype(dtype),_countsvar(0),
	_floatFill(ff),_longFill(ll)
{
  int i;
  vector<const Component *> comps;

  for (i = 0; i < v->ncomponents(); i++)
    comps.push_back((const Component *)v->component(i));

  for (i = 0; i < v2->ncomponents(); i++)
    comps.push_back((const Component *)v2->component(i));

  make_name(comps);

  vector<const VariableBase *> vars;
  vars.push_back((const VariableBase*) v);
  vars.push_back((const VariableBase*) v2);

  make_units(vars);
}

OutVariable::OutVariable(const InVariable *v,const InVariable *v2,
	const InVariable *v3,NS_datatype dtype,
	float ff,NSlong ll) throw(BadVariableName) :
	VariableBase(v),_datatype(dtype),_countsvar(0),
	_floatFill(ff),_longFill(ll)
{
  int i;
  vector<const Component *> comps;

  for (i = 0; i < v->ncomponents(); i++)
    comps.push_back((const Component *)v->component(i));
  for (i = 0; i < v2->ncomponents(); i++)
    comps.push_back((const Component *)v2->component(i));
  for (i = 0; i < v3->ncomponents(); i++)
    comps.push_back((const Component *)v3->component(i));
  make_name(comps);

  vector<const VariableBase *> vars;
  vars.push_back((const VariableBase*) v);
  vars.push_back((const VariableBase*) v2);
  vars.push_back((const VariableBase*) v3);

  make_units(vars);
}

OutVariable::OutVariable(const InVariable *v,const InVariable *v2,
	const InVariable *v3, const InVariable *v4,NS_datatype dtype,
	float ff,NSlong ll) throw(BadVariableName) :
	VariableBase(v),_datatype(dtype),_countsvar(0),
	_floatFill(ff),_longFill(ll)
{
  int i;
  vector<const Component *> comps;

  for (i = 0; i < v->ncomponents(); i++)
    comps.push_back((const Component *)v->component(i));
  for (i = 0; i < v2->ncomponents(); i++)
    comps.push_back((const Component *)v2->component(i));
  for (i = 0; i < v3->ncomponents(); i++)
    comps.push_back((const Component *)v3->component(i));
  for (i = 0; i < v4->ncomponents(); i++)
    comps.push_back((const Component *)v4->component(i));
  make_name(comps);

  vector<const VariableBase *> vars;
  vars.push_back((const VariableBase*) v);
  vars.push_back((const VariableBase*) v2);
  vars.push_back((const VariableBase*) v3);
  vars.push_back((const VariableBase*) v4);

  make_units(vars);
}

//
// this builds a name of the following format:
//
//	prefix1'prefex2'...'.(middle1,middle2,...).suffix
//
//	prefixN: the prefix of component N
//		All prefixes of a list of components are unique.
//	middleN: the middle portion of component N
//		The middle part of a component name can be
//		null, if the prefix and suffix fully name it.
//	suffix:	common suffix of all components.
//		This can be null if the prefix and middle fully name
//		a component.
// if there is only one component, it just rebuilds the name;
//	prefix.middle.suffix
//
void OutVariable::make_name(const vector<const Component*> &cs)
  throw(BadVariableName) {
  int i,l;

  int n = cs.size();
  if (!n) throw BadVariableName(name());

  l = (cs[0]->suffix() ? strlen(cs[0]->suffix()) : 0) + 2;
  for (i = 0; i < n; i++) l += strlen(cs[i]->prefix()) + 1;

  int same_mid=1;
  for (i = 1; i < n; i++)
    if (strcmp(cs[0]->middle(),cs[i]->middle())) same_mid = 0;

  int lmid;
  if (same_mid) lmid = strlen(cs[0]->middle()) + 1;
  else {
    lmid = 0;
    for (i = 0; i < n; i++) lmid += strlen(cs[i]->middle());
    if (lmid) lmid += n + 2;
  }

  l += lmid;

  delete [] _name;
  _name = new char[l];
  _name[0] = 0;

  for (i = 0; i < n; i++) {
    strcat(_name,cs[i]->prefix());
    if (n > 1) strcat(_name,"'");
  }
  if (same_mid) {
    if (lmid > 1) {
      strcat(_name,".");
      strcat(_name,cs[0]->middle());
    }
  }
  else {
    if (lmid > 0) {
      if (n > 1) strcat(_name,".(");
      else strcat(_name,".");
      for (i = 0; i < n; i++) {
	if (i) strcat(_name,",");
	strcat(_name,cs[i]->middle());
      }
      if (n > 1) strcat(_name,")");
    }
  }

  if (cs[0]->suffix() && strlen(cs[0]->suffix()) > 0) {
    strcat(_name,".");
    strcat(_name,cs[0]->suffix());
  }
  switch_names();
  return;
}
void OutVariable::make_units(const vector<const VariableBase *>&vars)
{
  int i,j,l,n;
  n = vars.size();

  const char **uar = new const char *[n];
				// list of unit names, to keep track of
				// repeated ones
  int *iar = new int[n];	// number of times this units is repeated

  for (i = 0; i < n; i++) {
    uar[i] = vars[i]->units();
    iar[i] = 1;
  }
  for (i = 0; i < n; i++) {
    if (uar[i])
      for (j = i + 1;j < n; j++)
	if (uar[j] && !strcmp(uar[i],uar[j])) {
	  uar[j] = 0;
	  iar[i]++;
	}
  }

  l = 0;
  for (i = 0; i < n; i++)
    if (uar[i]) l += strlen(uar[i]) + 5;

  if (l > 0) {
    char *un;
    un = new char[l+1];
    for (i = j = 0; i < n; i++)
      if (uar[i]) {
	if (i > 0) strcat(un+j++," ");
	if (iar[i] > 1) sprintf(un+j,"(%s)^%d",uar[i],iar[i]);
	else sprintf(un+j,"%s",uar[i]);
	j = strlen(un);
      }
      
    add_att("units",un);
    delete [] un;
  }
  delete [] uar;
  delete [] iar;
}

void OutVariable::switch_names()
{
  // If this variable has a "short_name" attribute,
  // replace it with the new made-up name
  if (att_val("short_name")) add_att("short_name",_name);

  // convert dots, tics, commas, parens to underscores so that
  // it is a legal NetCDL name
  char *cp;
  for (cp = _name; cp = strpbrk(cp,".'(),*"); cp++) *cp = '_';

  // while (cp = strstr(_name,"__")) strcpy(cp,cp+1);
  while (cp = strstr(_name,"__")) memmove(cp,cp+1,strlen(cp+1)+1);

  // remove trailing _, unless name less than 3 chars
  if (strlen(_name) > 2) {
    cp = _name + strlen(_name) - 1;
    if (*cp == '_') *cp = '\0';
  }
}
void OutVariable::append_name(const char *n)
{
  char *newname;
  int l = strlen(_name) + strlen(n) + 1;
  newname = new char[l];
  strcpy(newname,_name);
  strcat(newname,n);
  delete [] _name;
  _name = newname;

  const char *shortname;
  if ((shortname = att_val("short_name"))) {
    l = strlen(shortname) + strlen(n) + 1;
    newname = new char[l];
    strcpy(newname,shortname);
    strcat(newname,n);
    add_att("short_name",newname);
    delete [] newname;
  }
}

Component::Component(const char *cname): _prefix(0),_middle(0),_suffix(0),
	_words(0),_nwords(0),_nwordsuffix(0)
{
  int i,l;
  int nalloc;
  char *cp,*wp,*cplast;

  l = strlen(cname) + 1;
  _name = new char[l];
  strcpy(_name,cname);

  _string = new char[l];
  strcpy(_string,cname);

  /* Split Component at periods */
  for (cp=_string,_nwords=nalloc=0;; cp=0,_nwords++) {
    if ((wp = strtok_r(cp,".",&cplast)) == 0) break;
    if (_nwords == nalloc) {
      char **tmpwords;
      nalloc += NWORDALLOC;
      tmpwords = new char*[nalloc];
      for (i = 0; i < _nwords; i++) tmpwords[i] = _words[i];
      delete [] _words;
      _words = tmpwords;
    }
    if (_nwords == 0) {
      _prefix = new char[strlen(wp) + 1];
      strcpy(_prefix,wp);
    }
    _words[_nwords] = wp;
  }
  if (!_prefix) {
    _prefix = new char[1];
    _prefix[0] = '\0';
  }
  _middle = new char[1];
  _middle[0] = '\0';
#ifdef VARNAME_DEBUG
    DLOG(("Component=%s",_name));
    for (i = 0; i < _nwords; i++)
      DLOG(("word[%d]=%s",i,word(i)));
#endif
  
}
Component::~Component(void)
{
  delete [] _name;
  delete [] _string;
  delete [] _words;
  delete [] _prefix;
  delete [] _middle;
  delete [] _suffix;
}


void Component::make_prefix()
{
  delete [] _prefix;
  int l = strlen(_words[0]) + 1;
  _prefix = new char[l];
  strcpy(_prefix,_words[0]);
}

void Component::make_middle()
{
  delete [] _middle;
  int j,l=1;
  for(j = 1; j < _nwords - _nwordsuffix; j++)
    l += strlen(_words[j]) + 1;

  _middle = new char[l];
  _middle[0] = 0;

  for(j = 1; j < _nwords - _nwordsuffix; j++) {
    if (_middle[0] != '\0') strcat(_middle,".");
    strcat(_middle,_words[j]);
  }
#ifdef VDEBUG
  DLOG(("middle=%s",middle));
#endif
}

void Component::set_suffix(const char *suff)
{
  delete [] _suffix;
  _suffix = new char[strlen(suff)+1];
  strcpy(_suffix,suff);
}

const double NS_NcFile::minInterval = 1.e-5;

NS_NcFile::NS_NcFile(const string& fileName, enum FileMode openmode,
	double interval, double fileLength, double dtime):
	NcFile(fileName.c_str(),openmode),
	_fileName(fileName),
	_interval(interval),_lengthSecs(fileLength),
	_ndimalloc(0),_dimSizes(0),_dimNames(0),_dimIndices(0),_dims(0),
	refCount(0),_ttType(FIXED_DELTAT)
{

  if (!is_valid()) {
    PLOG(("NcFile %s: %s",_fileName.c_str(),nc_strerror(ncerror.get_err())));
    throw NS_NcFile::NetCDFAccessFailed();
  }

  if (_interval < minInterval) _ttType = VARIABLE_DELTAT;

  // If file length is 31 days, then align file times on months.
  _monthLong = _lengthSecs == 31 * 86400;

  if (_monthLong) {
    struct tm tm;
    nidas::util::UTime(dtime).toTm(true,&tm);

    tm.tm_sec = tm.tm_min = tm.tm_hour = 0;
    tm.tm_mday = 1;	// first day of month
    tm.tm_yday = 0;

    nidas::util::UTime ut(true,&tm);
    _baseTime = ut.toSecs();
  }
  else if (_lengthSecs > 0)
	_baseTime = (nclong) (floor(dtime / _lengthSecs) * _lengthSecs);

  _timeOffset = -_interval * .5; 	// _interval may be 0

  _nrecs = 0;

  /*
   * base_time variable id
   */
  if (!(_baseTimeVar = get_var("base_time"))) {
    /* New variable */
    if (!(_baseTimeVar = NcFile::add_var("base_time",ncLong)) ||
	!_baseTimeVar->is_valid()) {
      PLOG(("add_var %s: %s %s",_fileName.c_str(),"base_time",nc_strerror(ncerror.get_err())));
      throw NS_NcFile::NetCDFAccessFailed();
    }
    string since = nidas::util::UTime(0.0).format(true,"seconds since %Y-%m-%d %H:%M:%S 00:00");
    if (!_baseTimeVar->add_att("units",since.c_str())) {
      PLOG(("add_att %s: %s %s %s",_fileName.c_str(),_baseTimeVar->name(),"units",
	nc_strerror(ncerror.get_err())));
      throw NS_NcFile::NetCDFAccessFailed();
    }
  }

  if (!(_recdim = rec_dim())) {
    if (!(_recdim = add_dim("time")) || !_recdim->is_valid()) {
      PLOG(("add_dim %s: %s %s",_fileName.c_str(),"time",nc_strerror(ncerror.get_err())));
      // the NcFile has been constructed, and so if this exception is
      // thrown, it will be deleted, which will delete all the 
      // created dimensions and variables.  So we don't have to
      // worry about deleting _recdim here or the variables later
      throw NS_NcFile::NetCDFAccessFailed();
    }
  }
  else _nrecs = _recdim->size();

  if (!(_timeOffsetVar = get_var("time")) &&
    !(_timeOffsetVar = get_var("time_offset"))) {
    /* New variable */
    if (!(_timeOffsetVar = NcFile::add_var("time",ncDouble,_recdim)) ||
	!_timeOffsetVar->is_valid()) {
      PLOG(("add_var %s: %s %s",_fileName.c_str(),"time",nc_strerror(ncerror.get_err())));
      throw NS_NcFile::NetCDFAccessFailed();
    }
    _timeOffsetType = _timeOffsetVar->type();
  }
  else {
    _timeOffsetType = _timeOffsetVar->type();
    if (_nrecs > 0) {
      // Read last available time_offset, double check it
      long nrec = _nrecs - 1;
      NcValues *val;
      _timeOffsetVar->set_rec(nrec);
      if (!(val = _timeOffsetVar->get_rec())) {
	PLOG(("get_rec(%d) %s: %s %s",
		nrec,_fileName.c_str(),_timeOffsetVar->name(),nc_strerror(ncerror.get_err())));
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
	PLOG(("%s: unsupported type for time variable",_fileName.c_str()));
	throw NS_NcFile::NetCDFAccessFailed();
      }
      delete val;
      if (_ttType == FIXED_DELTAT &&
	fabs( ( (nrec+.5) * _interval) - _timeOffset) > _interval / 1000.) {
	PLOG((
	    "%s: Invalid timeOffset (NS_NcFile) = %f, nrec=%d,_nrecs=%d,interval=%f",
		_fileName.c_str(),_timeOffset, nrec,_nrecs,_interval));
	// rewrite them all
	_nrecs = 0;
	_timeOffset = -_interval * .5; 
      }

    }
  }

  NcAtt* timeOffsetUnitsAtt;
  if (!(timeOffsetUnitsAtt = _timeOffsetVar->get_att("units"))) {
    string since = nidas::util::UTime((time_t)_baseTime).format(true,"seconds since %Y-%m-%d %H:%M:%S 00:00");
    if (!_timeOffsetVar->add_att("units",since.c_str())) {
      PLOG(("add_att %s: %s %s %s",_fileName.c_str(),_timeOffsetVar->name(),"units",
	nc_strerror(ncerror.get_err())));
      throw NS_NcFile::NetCDFAccessFailed();
    }
  }
  else delete timeOffsetUnitsAtt;

  /* Write base time */
  if (!_baseTimeVar->put(&_baseTime,&_nrecs)) {
    PLOG(("put basetime %s: %s",_fileName.c_str(),nc_strerror(ncerror.get_err())));
    throw NS_NcFile::NetCDFAccessFailed();
  }

#ifdef DEBUG
  DLOG(("%s: nrecs=%d, baseTime=%d, timeOffset=%f, length=%f",
	_fileName.c_str(),_nrecs,_baseTime,_timeOffset,_lengthSecs));
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
    string tmphist = nidas::util::UTime(_lastAccess).format(true,"Created: %c\n");
    put_history(tmphist);
  }
  else {
    _historyHeader = nidas::util::UTime(_lastAccess).format(true,"Updated: %c\n");
    delete historyAtt;
  }

  ILOG(("%s: %s",(openmode==Write ? "Opened" : "Created"),_fileName.c_str()));

  _startTime = _baseTime;

  if (_monthLong) {
    _endTime = _baseTime + 32 * 86400;

    struct tm tm;
    nidas::util::UTime(_endTime).toTm(true,&tm);

    tm.tm_sec = tm.tm_min = tm.tm_hour = 0;
    tm.tm_mday = 1;	// first day of month
    tm.tm_yday = 0;

    nidas::util::UTime ut(true,&tm);
    _endTime = ut.toDoubleSecs();
  }
  else if (_lengthSecs > 0) _endTime = _baseTime + _lengthSecs;
  else _endTime = 1.e37;	// Somewhat far off in the future


}
NS_NcFile::~NS_NcFile(void)
{
  int i,j;

  ILOG(("Closing: %s",_fileName.c_str()));
  for (i = 0; i < _vars.size(); i++) {
    for (j = 0; j < _nvars[i]; j++) delete _vars[i][j];
    delete [] _vars[i];
  }
  delete [] _dimSizes;
  delete [] _dimIndices;
  delete [] _dimNames;
  delete [] _dims;
}

const std::string& NS_NcFile::getName() const { return _fileName; }

NcBool NS_NcFile::sync()
{
  _lastSync = time(0);
  int res = NcFile::sync();
  if (!res)
    PLOG(("sync %s: %s",_fileName.c_str(),nc_strerror(ncerror.get_err())));
  return res;
}

NS_NcVar** NS_NcFile::get_vars(VariableGroup *vgroup)
{

  int nv;
  NS_NcVar **vars;

  int igroup = vgroup->ngroup();

  // Check if this is a new VariableGroup for this file
  // Add empty vectors for missing VariableGroups
  //
  for (int i = _vars.size(); i <= igroup; i++) {
    _vars.push_back((NS_NcVar**)0);
    _nvars.push_back(0);
  }

  // variables have been initialzed for this VariableGroup
  if ((vars = _vars[igroup])) return vars;

#ifdef DEBUG
  DLOG(("calling get_vars"));
#endif

  _ndims_req = vgroup->num_dims();

  if (_ndims_req > _ndimalloc) {
    delete [] _dimSizes;
    delete [] _dimIndices;
    delete [] _dimNames;
    delete [] _dims;

    _dimSizes = new long[_ndims_req];
    _dimIndices = new int[_ndims_req];
    _dimNames = new const char *[_ndims_req];
    _dims = new const NcDim*[_ndims_req];
    _ndimalloc = _ndims_req;
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
    DLOG(("prior group dimension number %d %s, size=%d",
      i,_dimNames[i],_dimSizes[i]));
#endif

    // The _dims array is only used when creating a new variable
    // don't create dimensions of size 1
    // Unlimited dimension must be first one.
    if ((i == 0 && _dimSizes[i] == NC_UNLIMITED) || _dimSizes[i] > 1) {
      _dims[_ndims] = get_dim(_dimNames[i],_dimSizes[i]);
      if (!_dims[_ndims] || !_dims[_ndims]->is_valid()) {
	PLOG(("get_dim %s: %s %s",_fileName.c_str(),_dimNames[i],nc_strerror(ncerror.get_err())));
	return 0;
      }
      _ndims++;
    }
  }
#ifdef DEBUG
  for (int i = 0; i < _ndims; i++)
    DLOG(("file dimension %s, size=%d",
      _dims[i]->name(),_dims[i]->size()));

  DLOG(("creating outvariables"));
#endif

  nv = vgroup->num_vars();	// number of variables in group
  vars = new NS_NcVar*[nv];

  int numCounts = 0;
  int countsIndex = -1;
  string countsAttrNameFromFile;
  string countsAttrNameFromOVs;

  for (int iv = 0; iv < nv; iv++) {
    vars[iv] = 0;
    OutVariable *ov = vgroup->get_var(iv);
#ifdef DEBUG
    DLOG(("checking %s",ov->name()));
#endif
    if (!ov->isCnts()) {
      const char* cntsAttr;
      if ((cntsAttr = ov->att_val("counts"))) {
	if (countsAttrNameFromOVs.length() > 0) {
	  if (strcmp(countsAttrNameFromOVs.c_str(),cntsAttr))
	    PLOG((
		"multiple counts attributes for group with %s: %s and %s",
		      ov->name(),countsAttrNameFromOVs.c_str(),cntsAttr));
	}
	else countsAttrNameFromOVs = cntsAttr;
      }
      NcVar* ncv;
      if ((ncv = find_var(ov))) {
	NcAtt* att = ncv->get_att("counts");
	if (att) {
	  if (att->type() == ncChar && att->num_vals() > 0) {
	    const char *cname = att->as_string(0);
	    if (countsAttrNameFromFile.length() == 0)
		  countsAttrNameFromFile = cname;
	    else {
	      // this shouldn't happen - there should be one counts var per grp
	      if (strcmp(countsAttrNameFromFile.c_str(),cname))
		PLOG((
		  "multiple counts variables for group with %s: %s and %s",
			ov->name(),countsAttrNameFromFile.c_str(),cname));
	    }
	    delete [] cname;
	  }
	  delete att;
	}
      }

      NS_NcVar* nsv;
      if ((nsv = add_var(ov)) == 0) {
	PLOG(("Failed to add variable %s to %s",
	      ov->name(),_fileName.c_str()));
	continue;
      }
      vars[iv] = nsv;
    }
    else {
      // a counts variable
      if (numCounts++ == 0) countsIndex = iv;
    }
  }

  //
  // Scenarios:
  //	no variables in file, so no attributes, likely a new file
  //	  does counts variable exist in file
  //	    yes  create a new counts name, update var attrs, create var
  //	    no, create var
  //	variables, and counts attributes
  //	  counts var exists in file
  //	    does file counts variable match OutVariable counts var name?
  //		yes: no problem
  //		no: change name of OutVariable
  //	  counts var doesn't exist in file
  //		typically shouldn't happen, since attributes existed
  //		

  if (numCounts > 0) {
    if (numCounts > 1)
      PLOG(("multiple counts variables in variable group"));

#ifdef DEBUG
    DLOG(("countsIndex=%d",countsIndex));
#endif
    OutVariable *cv = vgroup->get_var(countsIndex);
    assert(cv->isCnts());
#ifdef DEBUG
    cerr << "countsAttrNameFromFile=" << countsAttrNameFromFile << endl;
    cerr << "counts var=" << cv->name() << endl;
#endif

    if (countsAttrNameFromFile.length() == 0) {
      // no counts attributes found in file
      // check that OutVariable counts attributes
      // matches OutVariable name
      if (strcmp(cv->name(),countsAttrNameFromOVs.c_str()))
	  PLOG((
	    "counts attributes don't match counts variable name for group %s: %s and %s",
		  cv->name(),countsAttrNameFromOVs.c_str(),cv->name()));
      NcVar* ncv;
      if ((ncv = get_var(cv->name()))) {
	// counts variable exists in file, it must be for another group
	// create another counts variable
	int l = strlen(cv->name());
	char* countsname = new char[l + 5];
	strcpy(countsname,cv->name());
	for (int j=1; get_var(countsname); j++)
	  sprintf(countsname+l,"_%d",j);
	// new name
	ILOG(("new name for counts variable %s: %s\n",
	      cv->name(),countsname));
	if (strcmp(countsname,cv->name())) cv->set_name(countsname);
	delete [] countsname;
	// set our counts attributes (on the file too)
	for (int iv = 0; iv < nv; iv++) {
	  OutVariable* ov = vgroup->get_var(iv);
	  if (!ov->isCnts()) {
	    ov->add_att("counts",cv->name());
	    NS_NcVar* nsv = vars[iv];
	    // set in file too
	    if (nsv) nsv->add_att("counts",cv->name());
	  }
	}
	vars[countsIndex] = add_var(cv);
      }
      else {
	// counts variable doesn't exist
	if ((vars[countsIndex] = add_var(cv)) == 0)
	  PLOG(("failed to create counts variable %s",
		cv->name()));
	for (int iv = 0; iv < nv; iv++) {
	  OutVariable* ov = vgroup->get_var(iv);
	  if (!ov->isCnts()) {
	    ov->add_att("counts",cv->name());
	    NS_NcVar* nsv = vars[iv];
	    // set in file too
	    nsv->add_att("counts",cv->name());
	  }
	}
      }
    }
    else {
      // counts attributes found in file
      cv->set_name(countsAttrNameFromFile.c_str());
      if ((vars[countsIndex] = add_var(cv)) == 0)
	  PLOG(("failed to create counts variable %s",
		cv->name()));
      for (int iv = 0; iv < nv; iv++) {
	OutVariable* ov = vgroup->get_var(iv);
	if (!ov->isCnts()) {
	  ov->add_att("counts",cv->name());
	  NS_NcVar* nsv = vars[iv];
	  // set in file too
	  nsv->add_att("counts",cv->name());
	}
      }
    }
  }

#ifdef DEBUG
  DLOG(("added vars"));
#endif

  _vars[igroup] = vars;	
  _nvars[igroup] = nv;
#ifdef DEBUG
  DLOG(("doing sync"));
#endif
  sync();
#ifdef DEBUG
  DLOG(("did sync"));
#endif
  return vars;
}

void VariableGroup::check_counts_variable() 
{
  OutVariable *ov,*cv;
  const char *countsname;

  // scan over all counts variables
  set<OutVariable*> cntsVars;
  set<string> cntsNames;
  for (int i = 0; i < num_vars(); i++) {
    ov = get_var(i);
    if (ov->isCnts()) cntsVars.insert(ov);
    else if ((countsname = ov->att_val("counts")))
      cntsNames.insert(countsname);
  }

  if (cntsNames.size() == 0) return;	// no counts

  // add a counts OutVariable for each cntsName that doesn't
  // exist in cntsVars. Update counts_variable() member.
  set<string>::const_iterator ni = cntsNames.begin();
  for (; ni != cntsNames.end(); ++ni) {
    set<OutVariable*>::const_iterator ci = cntsVars.begin();
    for (; ci != cntsVars.end(); ++ci)
	if (!strcmp((*ci)->name(),ni->c_str())) break;
    if (ci == cntsVars.end()) {
      OutVariable *cv = CreateCountsVariable(ni->c_str());
      // update counts_variable() 
      for (int i = 0; i < num_vars(); i++) {
	ov = get_var(i);
	if (!ov->isCnts() && ((countsname = ov->att_val("counts")))
		&& !strcmp(countsname,ni->c_str()))
			ov->counts_variable() = cv;
      }
    }
  }
}

NS_NcVar* NS_NcFile::add_var(OutVariable *v)
{
  int i;
  NcVar *var;
  NS_NcVar* fsv;
  int isCnts = v->isCnts();

  const char *varName = v->name();
  const char *shortName = v->att_val("short_name");

  // No matching variables found, create new one
  if (!(var = find_var(v))) {

    if (!(var = NcFile::add_var(varName,(NcType)v->data_type(),_ndims,_dims)) ||
	!var->is_valid()) {
      PLOG(("add_var %s: %s %s",_fileName.c_str(),varName,nc_strerror(ncerror.get_err())));
      PLOG(("shortName=%s",shortName));
      PLOG(("define_mode=%d",define_mode()));
      PLOG(("data_type=%d",v->data_type()));
      PLOG(("ndims=%d",_ndims));
      for (i = 0; i < _ndims; i++) 
	DLOG(("dims=%d id=%d size=%d",i,_dims[i]->id(),_dims[i]->size()));
      goto error;
    }
    if (shortName && !var->add_att("short_name",shortName)) {
      DLOG(("add_att %s: %s %s %s",_fileName.c_str(),varName,"short_name",
	nc_strerror(ncerror.get_err())));
      goto error;
    }
  }

  if (add_attrs(v,var) < 0) goto error;

#ifdef DEBUG
  DLOG(("var %s has %d dimensions",varName,var->num_dims()));
  for (i = 0; i < var->num_dims(); i++) {
    const NcDim *dim = var->get_dim(i);
    DLOG(("dim name=%s,size=%d",
      dim->name(),dim->size()));
  }
#endif
  // double check ourselves
  if (!check_var_dims(var)) {
    PLOG(("wrong dimensions for variable %s: %s",_fileName.c_str(),varName));
    goto error;
  }
  fsv = new NS_NcVar(var,_dimIndices,_ndims_req,v->floatFill(),
  	v->longFill(),isCnts);
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
int NS_NcFile::add_attrs(OutVariable *v,NcVar* var)
{
  // add attributes if they don't exist in file, otherwise leave them alone

  // bug in netcdf, if units is empty string "", result in 
  // netcdf file is some arbitrary character.

  NcAtt *nca;
  if (!(nca = var->get_att("_FillValue"))) {
    switch(v->data_type()) {
    case NS_LONG:
      if (!var->add_att("_FillValue",(long)v->longFill())) {
	PLOG(("add_att %s: %s %s %s",_fileName.c_str(),var->name(),
		"_FillValue", nc_strerror(ncerror.get_err())));
	return -1;
      }
      break;
    case NS_FLOAT:
      if (!var->add_att("_FillValue",v->floatFill())) {
	PLOG(("add_att %s: %s %s %s",_fileName.c_str(),var->name(),
		"_FillValue", nc_strerror(ncerror.get_err())));
	return -1;
      }
      break;
    }
  }
  else delete nca;

  if (v->units()) {
    nca = var->get_att("units");
    NcValues* uvals = 0;
    char* units = 0;
    if (nca) uvals = nca->values();
    if (uvals && nca->num_vals() >= 1 && nca->type() == ncChar)
        units = uvals->as_string(0);
    if (!units || strcmp(units,v->units())) {
#ifdef DEBUG
        DLOG(("new units=%s, old units=%s,len=%d",
            v->units(),
            (units ? units : "none"),
            (nca ? nca->num_vals() : 0)));
#endif
      if (!var->add_att("units",v->units())) {
	PLOG(("add_att %s: %s %s %s",_fileName.c_str(),var->name(),
		"units", nc_strerror(ncerror.get_err())));
	return -1;
      }
    }
    delete nca;
    delete uvals;
    delete [] units;
  }

  // all other attributes
  int natt = v->num_atts();
  for (int i = 0; i < natt; i++ ) {
    const VarAttr<char *> *a = v->get_att(i);
    if (!(nca = var->get_att(a->name()))) {
      const char *aval = a->val();
      if (aval && !var->add_att(a->name(),aval)) {
	PLOG(("add_att %s: %s %s %s",_fileName.c_str(),var->name(),
		a->name(), nc_strerror(ncerror.get_err())));
	return -1;
      }
    }
    else delete nca;
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
NcVar* NS_NcFile::find_var(OutVariable *v)
{
  int i;
  NcVar *var;
  const char *varName = v->name();
  const char *shortnameAttr = "short_name";
  const char *shortName = v->att_val(shortnameAttr);
  char tmpString[512];

  int varFound = 0;
  int needNewName = 0;

  NcAtt *att;
  char * attString;

  if ((var = get_var(varName))) {
    needNewName = 1;
    if (!shortName) varFound = 1;
    // Check its short_name attribute
    else if ((att = var->get_att(shortnameAttr))) {
      attString = 0;
      if (att->type() == ncChar && att->num_vals() > 0 &&
	(attString = att->as_string(0)) &&
	!strcmp(attString,shortName)) varFound = 1;
      delete att;
      delete [] attString;
    }
      
    if (!varFound) var = 0;
  }
  //
  // If we can't find a variable with the same NetCDF variable name,
  // and a matching short_name, look through all other variables for
  // one with a matching short_name
  //
  for (i = 0; shortName && !var && i < num_vars(); i++) {
    var = get_var(i);
    // Check its short_name attribute
    if ((att = var->get_att(shortnameAttr))) {
      attString = 0;
      if (att->type() == ncChar && att->num_vals() > 0 &&
	(attString = att->as_string(0)) &&
	!strcmp(attString,shortName)) {
	delete att;
	delete [] attString;
	break;	// match
      }
      delete att;
      delete [] attString;
    }
    var = 0;
  }

  if (var && var->type() != (NcType) v->data_type()) {
    var = 0;
    DLOG(("%s: variable %s is of wrong type",
	_fileName.c_str(),var->name()));
  }

  if (var && !check_var_dims(var)) {
    int l=0;
    DLOG(("%s: variable %s has incorrect dimensions",
	_fileName.c_str(),var->name()));
    sprintf(tmpString,"%s(",varName);
    for (i = 0; i < _ndims_req; i++) {
      if (i > 0) strcat(tmpString,",");
      l = strlen(tmpString);
      sprintf(tmpString+l,"%s=%d",_dimNames[i],_dimSizes[i]);
    }
    strcat(tmpString,")");
    DLOG(("%s: should be declared %s",_fileName.c_str(),tmpString));

    if (shortName) {
      //
      // Variable with matching short_name, but wrong dimensions
      // We'll change the short_name attribute of the offending
      // variable to "name_old" and create a new variable.
      strcpy(tmpString,shortName);
      strcat(tmpString,"_old");
      if (!var->add_att("short_name",tmpString)) {
	PLOG(("add_att %s: %s %s %s",_fileName.c_str(),var->name(),"short_name",
	  nc_strerror(ncerror.get_err())));
      }
    }
    var = 0;
  }

  if (!var && needNewName) {
    //
    // !var && needNewName means there was a variable with the same name,
    // but differing short_name, dimensions or type.  So we need to
    // change our name.
    //
    char *tmpVarName = new char[strlen(varName) + 5];
    for ( ;; needNewName++) {
      sprintf(tmpVarName,"%s_%d",varName,needNewName);
      if (!(get_var(tmpVarName))) break;
    }
    DLOG(("%s: %s new name= %s\n",
	_fileName.c_str(),var->name(),tmpVarName));
    v->set_name(tmpVarName);
    delete [] tmpVarName;
  }
  return var;
}

NcBool NS_NcFile::put_rec(const struct datarec_float* writerec,
	VariableGroup *vgroup,double dtime)
{
  long nrec;
  long nsample;
  NS_NcVar** vars;
  NS_NcVar *var;
  int i,iv,nv;
  double groupInt = vgroup->interval();
  double tdiff;
  time_t tnow;
  int ndims_req = vgroup->num_dims();
  int igroup = vgroup->ngroup();

  // this will add variables if necessary
#ifdef DEBUG
  DLOG(("calling get_vars"));
#endif
  if (!(vars = get_vars(vgroup))) return 0;
#ifdef DEBUG
  DLOG(("called get_vars"));
#endif

  dtime -= _baseTime;
  if (_ttType == FIXED_DELTAT && vgroup->num_samples() > 1) {
    nsample = (int)floor(fmod(dtime,_interval)/groupInt);
    tdiff = (nsample + .5) * groupInt - .5 * _interval;
    dtime -= tdiff;
  }
  else nsample = 0;

  if ((nrec = put_time(dtime,vars[0]->name())) < 0) return 0;

  nv = _nvars[igroup];

  int nstart = writerec->start.start_len;
  int ncount = writerec->count.count_len;
  long *start = (long *)writerec->start.start_val;
  long *count = (long *)writerec->count.count_val;
  const float *d = writerec->data.data_val;
  int nd = writerec->data.data_len;
  const float *dend = d + nd;

  if (nstart != ndims_req - 2 || nstart != ncount) {
    PLOG(("variable %s has incorrect start or count length",
      vars[0]->name()));
  }
#ifdef DEBUG
    DLOG(("nstart=%d,ncount=%d",nstart,ncount));
    for (i = 0; i < ncount; i++)
	DLOG(("count[%d]=%d",i,count[i]));
#endif

  for (iv = 0; iv < nv; iv++) {
    var = vars[iv];

    var->set_cur(nrec,nsample,start);
    if (var->isCnts() && writerec->cnts.cnts_len > 0) {
#ifdef DEBUG
    DLOG(("put counts"));
#endif
      if (!var->put((const nclong *)writerec->cnts.cnts_val,count)) {
	PLOG(("put cnts %s: %s %s",_fileName.c_str(),var->name(),nc_strerror(ncerror.get_err())));
	return 0;
      }
    }
    else if (d < dend) {
      if (!(i = var->put(d,count))) {
	PLOG(("put var %s: %s %s",_fileName.c_str(),var->name(),nc_strerror(ncerror.get_err())));
	return 0;
      }
#ifdef DEBUG
	DLOG(("var->put of %s, i=%d",var->name(),i));
#endif
      d += i;
    }
    // The last variables in a NS_TIMESERIES group may be counts variables
    // (having been found in the NetCDF file), but the user may not
    // pass the counts data to be written.  Don't issue a warning in this case.
    else if (!var->isCnts() || vgroup->rec_type() != NS_TIMESERIES)
      d += var->put_len(count);
  }
  if (d != dend)
    PLOG(("put check %s: %s put request for %d values, should be %d",
	_fileName.c_str(),vars[0]->name(),nd,d-writerec->data.data_val));

#ifdef DEBUG
	DLOG(("sync"));
#endif
  if ((tnow = time(0)) - _lastSync > 60) sync();
#ifdef DEBUG
	DLOG(("sync done"));
#endif
  _lastAccess = tnow;
  return 1;
}

NcBool NS_NcFile::put_rec(const struct datarec_long* writerec,
	VariableGroup *vgroup, double dtime)
{
  long nrec;
  long nsample;
  NS_NcVar** vars;
  NS_NcVar *var;
  int i,iv,nv;
  double groupInt = vgroup->interval();
  double tdiff;
  time_t tnow;
  int ndims_req = vgroup->num_dims();
  int igroup = vgroup->ngroup();

#ifdef DEBUG
  DLOG(("calling get_vars"));
#endif
  // this will add variables if necessary
  if (!(vars = get_vars(vgroup))) return 0;
#ifdef DEBUG
  DLOG(("called get_vars"));
#endif

  dtime -= _baseTime;
  if (_ttType == FIXED_DELTAT && vgroup->num_samples() > 1) {
    nsample = (int)floor(fmod(dtime,_interval)/groupInt);
    tdiff = (nsample + .5) * groupInt - .5 * _interval;
    dtime -= tdiff;
  }
  else nsample = 0;

  if ((nrec = put_time(dtime,vars[0]->name())) < 0) return 0;

  nv = _nvars[igroup];

  int nstart = writerec->start.start_len;
  int ncount = writerec->count.count_len;
  long *start = (long *)writerec->start.start_val;
  long *count = (long *)writerec->count.count_val;
  const nclong *d = (const nclong *)writerec->data.data_val;
  int nd = writerec->data.data_len;
  const nclong *dend = (const nclong *)d + nd;

  if (nstart != ndims_req - 2 || nstart != ncount) {
    PLOG(("variable %s has incorrect start or count length",
      vars[0]->name()));
  }

  for (iv = 0; iv < nv; iv++) {
    var = vars[iv];

    var->set_cur(nrec,nsample,start);
    if (var->isCnts() && writerec->cnts.cnts_len > 0) {
      if (!var->put((const nclong *)writerec->cnts.cnts_val,count)) {
	PLOG(("put cnts %s: %s %s",_fileName.c_str(),var->name(),nc_strerror(ncerror.get_err())));
	return 0;
      }
    }
    else if (d < dend) {
      if (!(i = var->put(d,count))) {
	PLOG(("put var %s: %s %s",_fileName.c_str(),var->name(),nc_strerror(ncerror.get_err())));
	return 0;
      }
      d += i;
    }
    else if (!var->isCnts() || vgroup->rec_type() != NS_TIMESERIES)
      d += var->put_len(count);
  }
  if (d != dend)
    PLOG(("put check %s: %s put request for %d values, should be %d",
	_fileName.c_str(),vars[0]->name(),nd,d-(const nclong *)writerec->data.data_val));

  if ((tnow = time(0)) - _lastSync > 60) sync();
  _lastAccess = tnow;
  return 1;
}


long NS_NcFile::put_time(double timeoffset,const char *varname)
{
  int i;
  float floatOffset;
  long nrec;

  /*
   * nrec is the record number to be written.
   * _nrecs is one more than the last record written,
   *	or if we're at the end of the file, the number of
   *	records in the file.
   */
  if (_ttType == VARIABLE_DELTAT) nrec = _nrecs;
  else nrec = (long)(timeoffset / _interval);

#ifdef DEBUG
  DLOG(("timeoffset=%f, timeOffset=%f,nrec=%d, nrecs=%d,interval=%f",
		(double)timeoffset,(double)_timeOffset,nrec,_nrecs,
		_interval));
#endif

#ifdef DOUBLE_CHECK
  if (nrec < _nrecs-1) {
    // time for this record has been written
    // double check time of record

    sync();
    double tmpOffset;
    NcValues *val;
    _timeOffsetVar->set_rec(nrec);
    if (!(val = _timeOffsetVar->get_rec())) {
      PLOG(("get_rec(%d) %s: %s %s",
	      _nrecs,_fileName.c_str(),_timeOffsetVar->name(),nc_strerror(ncerror.get_err())));
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
#ifdef WIERD_BUG
    /* Wierd bug fix */
    if (nrec == 0 && tmpOffset == 0.0) {
      PLOG(("%m Bad read of timeOffset do sync and try again"));
      sync();
      if (!(val = _timeOffsetVar->get_rec())) {
	PLOG(("get_rec(%d) %s: %s %s",
		_nrecs,_fileName.c_str(),_timeOffsetVar->name(),nc_strerror(ncerror.get_err())));
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
    }
    if (nrec == 0 && tmpOffset == 0.0) {
      PLOG(("Try again didn't help"));
      tmpOffset = _interval * .5;
    }
#endif
    
    if (fabs((double)tmpOffset - (double)timeoffset) > _interval / 1000.) {
      PLOG((
	"Invalid timeoffset=%f, file timeOffset=%f, nrec=%d, _nrecs=%d, _interval=%f, var=%s",
		  (double)timeoffset,(double)tmpOffset,nrec,_nrecs,_interval,varname));
    }
  }
#endif

  // Write time to previous records and the current record
  for (; _nrecs <= nrec; _nrecs++) {
    if (_ttType == VARIABLE_DELTAT) _timeOffset = timeoffset;
    else _timeOffset += _interval;
    switch (_timeOffsetType) {
    case ncFloat:
      floatOffset = _timeOffset;
      i = _timeOffsetVar->put_rec(&floatOffset,_nrecs);
      break;
    case ncDouble:
      i = _timeOffsetVar->put_rec(&_timeOffset,_nrecs);
      break;
    }
    if (!i) {
      PLOG(("time_offset put_rec %s: %s",_fileName.c_str(),nc_strerror(ncerror.get_err())));
      return -1;
    }
  }
#ifdef DEBUG
  DLOG(("after fill timeoffset = %f, timeOffset=%f,nrec=%d, _nrecs=%d,interval=%f",
		(double)timeoffset,(double)_timeOffset,nrec,_nrecs,_interval));
#endif

#ifdef LAST_TIME_CHECK
  if (_ttType == FIXED_DELTAT && nrec == _nrecs-1) {
    if (fabs((double)_timeOffset - (double)timeoffset) > _interval / 1000.) {
      PLOG((
	  "Invalid timeoffset = %f, file timeOffset=%f,nrec=%d, _nrecs=%d, interval=%f, var=%s",
		  timeoffset,_timeOffset,nrec,_nrecs,_interval,varname));
    }
  }
#endif
  return nrec;
}

int NS_NcFile::put_history(string val)
{
    if (val.length() == 0) return 0;

    string history;

    NcAtt *historyAtt = get_att("history");
    if (historyAtt) {
        char *htmp = historyAtt->as_string(0);
        history = htmp;
        delete [] htmp;
  #ifdef DEBUG
        DLOG(("history len=%d, history=%.40s",hlen,history));
  #endif
        delete historyAtt;
    }

    // avoid writing the same history record multiple times to the same file
    int updated = 0;

    string::size_type i1,i2 = 0;

    // check \n delimited records in h and history
    for (i1 = 0; i1 < val.length(); i1 = i2) {

        i2 = val.find('\n',i1);
        if (i2 == string::npos) i2 = val.length();
        else i2++;

        string::size_type j1,j2 = 0;
        for (j1 = 0; j1 < history.length(); j1 = j2) {
            j2 = history.find('\n',j1);
            if (j2 == string::npos) j2 = history.length();
            else j2++;

            if (history.substr(j1,j2-j2).find(val.substr(i1,i2-i1)) != string::npos) {
                // match of a line in val with the existing history
                val = val.substr(0,i1) + val.substr(i2);
                i2 = i1;
            }
        }

        if (val.length() > 0) {
            history += _historyHeader + val;
            if (!add_att("history",history.c_str()))
                PLOG(("add history att: %s: %s",_fileName.c_str(),nc_strerror(ncerror.get_err())));
        }
    }
  
    _lastAccess = time(0);

#ifdef DEBUG
    DLOG(("NS_NcFile::put_history: l=%d",l));
#endif

    // Don't sync;
    // sync();

    return 0;
}

NcBool NS_NcFile::check_var_dims(NcVar *var)
{

  //
  // Example:
  //
  //   NetCDF file
  //	dimensions:
  //		time=99;
  //		sample = 5;
  //		station = 8;
  //		sample_10 = 10;
  //		station_2 = 2;
  //    variables:
  //		x(time,sample,station);
  //		y(time);
  //		z(time,station_2);
  //		zz(time,sample_10,station_2);
  //   Input:
  //	 variable: x
  //	 dimnames:  time, sample, station
  //	 sizes:    any     5	    8
  //	 _ndims_req: 3
  //	Returned:
  //		function value: 1 (OK)
  //		indices = 0,1,2	sample is dimension 1 of variable,
  //				station is dimension 2
  //	Input:
  //	 variable: y
  //	 dimnames:  time, sample, station
  //	 sizes:    any      1	    1
  //	 _ndims_req: 3
  //	Returned:
  //		function value: 1 (OK)
  //		indices = 0,-1,-1 (variable has no sample or station dim)
  //	Input:
  //	 variable: z
  //	 dimnames:  time, sample, station
  //	 sizes:    any	 1	    2
  //	 ndimin: 3
  //	Returned:
  //		function value: 1 (OK)
  //		indices = 0,-1,1 variable has no sample dim,
  //				station_2 dim is dimension 1 (station_2)
  //				Note that the dimension name can
  //				have a _N suffix.  That way
  //				a more than one value for a station
  //				dimension can exist in the file -
  //				perhaps a variable was sampled
  //				by a subset of the stations.
  //				
  //	Input:
  //	 variable: zz
  //	 dimnames:  time, sample, station
  //	 sizes:	     any    10	    2
  //	 ndimin: 3
  //	Returned:
  //		function value: 1 (OK)
  //		indices = 0,1,2	sample_10 is dimension 1 of variable,
  //				station_2 is dimension 2
  //	Input:
  //	 variable: zz
  //	 dimnames:  time,sample, station
  //	 sizes:	  any	1	    9
  //	 ndimin: 3
  //	Returned:
  //		function value: 0 (No match, no station dimension
  //				of value 9)

  int ndims;
  NcDim* dim;
  int i,ireq;

  ndims = var->num_dims();
  if (ndims < 1) {
    PLOG(("%s: variable %s has no dimensions",
	_fileName.c_str(),var->name()));
    return 0;
  }
  if (!var->get_dim(0)->is_unlimited()) return 0;

  for (ireq = i = 0; i < ndims && ireq < _ndims_req; ) {
    dim = var->get_dim(i);
    
#ifdef DEBUG
    DLOG(("req dim[%d] = %s, size=%d ndimin=%d",
	ireq,_dimNames[ireq],_dimSizes[ireq],_ndims_req));
    DLOG(("dim[%d] = %s, size=%d ndims=%d",
	i,dim->name(),dim->size(),ndims));
#endif
    if (_dimSizes[ireq] == NC_UNLIMITED) {
      if (dim->is_unlimited()) _dimIndices[ireq++] = i++;
      else return 0;
    }
    else if (!strncmp(dim->name(),_dimNames[ireq],strlen(_dimNames[ireq]))) {
      // name match
#ifdef DEBUG
    DLOG(("name match"));
#endif
      if (dim->size() != _dimSizes[ireq]) return 0;
      _dimIndices[ireq++] = i++;
    }
    // If no name match, then the requested dimension must be 1.
    else {
#ifdef DEBUG
    DLOG(("no name match"));
#endif
      if (_dimSizes[ireq] != 1) return 0;
      _dimIndices[ireq++] = -1;
    }
  }
#ifdef DEBUG
    DLOG(("ireq=%d _ndims_req=%d, i=%d, ndims=%d",
	ireq,_ndims_req,i,ndims));
#endif
  // remaining requested or existing dimensions should be 1
  for (; ireq < _ndims_req; ireq++) if (_dimSizes[ireq] > 1) return 0;
  for (; i < ndims; i++) {
    dim = var->get_dim(i);
    if (dim->size() != 1) return 0;
  }
  return 1;
}

const NcDim* NS_NcFile::get_dim(NcToken prefix,long size)
{
  const NcDim* dim;
  int i,l;

  if (size == NC_UNLIMITED) return _recdim;
  if ((dim = NcFile::get_dim(prefix)) && dim->size() == size) return dim;

  int ndims = num_dims();

  l = strlen(prefix);

  // Look for a dimension whose name starts with prefix and with correct size
  //
  for (i = 0; i < ndims; i++) {
    dim = NcFile::get_dim(i);
#ifdef DEBUG
    DLOG(("dim[%d]=%s, size %d",i,dim->name(),dim->size()));
#endif
    if (!strncmp(dim->name(),prefix,l) && dim->size() == size) return dim;
  }

  // At this point:
  //	there are no dimensions starting with "prefix"
  //	or if there are, they don't have the correct size
  //

  char tmpString[64];

  for (i = 0; ;i++) {
    if (!i) strcpy(tmpString,prefix);
    else sprintf(tmpString+strlen(tmpString),"_%d",size);
    if (!(dim = NcFile::get_dim(tmpString))) break;
  }
  // found a unique dimension name, starting with prefix

  dim = add_dim(tmpString,size);
#ifdef DEBUG
  DLOG(("new dimension %s, size %d",tmpString,size));
#endif
  return dim;
}

NS_NcVar::NS_NcVar(NcVar *var,int *dimIndices,int ndimIndices,float ffill,
	NSlong lfill,int iscnts) :
	_var(var),_ndimIndices(ndimIndices),_isCnts(iscnts),_floatFill(ffill),
	_longFill(lfill)
{
  int i;
  _dimIndices = new int[_ndimIndices];
  _start = new long[_ndimIndices];
  _count = new long[_ndimIndices];
  for (i = 0; i < _ndimIndices; i++) _dimIndices[i] = dimIndices[i];
}


NS_NcVar::~NS_NcVar()
{
  delete [] _dimIndices;
  delete [] _start;
  delete [] _count;
}

NcBool NS_NcVar::set_cur(long nrec,int nsample,const long *start)
{
  int i,j,k;
  _start[0] = nrec;
  if ((k = _dimIndices[1]) > 0) _start[k] = nsample;

  for (i = 0,j = 2; j < _ndimIndices; i++,j++)
    if ((k = _dimIndices[j]) > 0) _start[k] = start[i];
  
  return _var->set_cur(_start);
}
int NS_NcVar::put(const float *d, const long *counts)
{
  int nout = put_len(counts);		// this sets _count
  // type conversion of one data value
  if (nout == 1 && _var->type() == ncLong) {
    nclong dl = (nclong)d[0];
    if (d[0] == _floatFill) dl = _longFill;
    return (_var->put(&dl,_count) ? nout : 0);
  }
  int i = _var->put(d,_count);
#ifdef DEBUG
  DLOG(("_var->put of %s, i=%d, nout=%d",
  	name(),i,nout));
#endif
  return (i ? nout : 0);
  // return (_var->put(d,_count) ? nout : 0);
}
  
int NS_NcVar::put(const nclong *d, const long *counts)
{
  int nout = put_len(counts);		// this sets _count
  // type conversion of one data value
  if (nout == 1 && _var->type() == ncFloat) {
    float df = d[0];
    return (_var->put(&df,_count) ? nout : 0);
  }
  return (_var->put(d,_count) ? nout : 0);
}
  
int NS_NcVar::put_len(const long *counts)
{
  int i,j,k;
  int nout = 1;
  _count[0] = 1;
  if ((k = _dimIndices[1]) > 0) _count[k] = 1;
#ifdef DEBUG
  DLOG(("%s: _dimIndices[1]=%d",
  	name(),_dimIndices[1]));
#endif

  for (i = 0,j = 2; j < _ndimIndices; i++,j++)
    if ((k = _dimIndices[j]) > 0) {
      _count[k] = counts[i];
#ifdef DEBUG
  DLOG(("%s: _dimIndices[%d]=%d,counts[%d]=%d",
  	name(),j,_dimIndices[j],i,counts[i]));
#endif
      nout *= _count[k];
    }
  return nout;
}
