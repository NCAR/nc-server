//              Copyright (C) by UCAR
//

#include <stddef.h>
#include <stdarg.h>
#include <stdlib.h>
#include <new>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <syslog.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <rpc/rpc.h>
#include <rpc/pmap_clnt.h>
#include <errno.h>
#include <sys/resource.h>
#include <netinet/in.h>
#include <pwd.h>

#include "nc_server_rpc.h"
#include "nc_server.h"
#include <nidas/util/Logger.h>

static uid_t useruid = 0;

using namespace std;

int usage(const char* argv0)
{
    cerr << "Usage: " << argv0 << " [-u username]" << endl;
    return 1;
}
int parseRunstring(int argc, char** argv)
{
    int c;
    while ((c = getopt(argc,argv,"u:")) != -1) {
        switch (c) {
        case 'u':
            {
                char *username = optarg;
                struct passwd* p = getpwnam(username);
                if (!p) {
                    cerr << "Unknown user: " << username << endl;
                    return 1;
                }
                useruid = p->pw_uid;
            }
            break;
        default:
            return usage(argv[0]);
        }
    }
    return 0;
}

extern "C" {
  void netcdfserverprog_1(struct svc_req *rqstp, register SVCXPRT *transp);
}
/*
#define  DEBUG
#define  RPC_SVC_FG
*/

int * openconnection_1_svc(connection *input,struct svc_req *)
{

  static int res;

  res = -1;

  Connections *connections = Connections::Instance();

  res = connections->OpenConnection(input);

  return &res;
}
int * definedatarec_1_svc(datadef *ddef,struct svc_req *)
{

  static int res;
  Connection *conn;
  Connections *connections = Connections::Instance();

  res = -1;

  if ((conn = (*connections)[ddef->connectionId]) == 0) {
    PLOG(("Invalid Connection ID: %d",ddef->connectionId));
    return &res;
  }

  res = conn->add_var_group(ddef);

#ifdef DEBUG
  DLOG(("definedatarec_1_svc res=%d",res));
#endif

  return &res;
}
int * writedatarec_float_1_svc(datarec_float *writereq,struct svc_req *)
{

  static int res;
  Connection *conn;
  Connections *connections = Connections::Instance();

  res = -1;

#ifdef DEBUG
  DLOG(("writereq->connectionId=%d",writereq->connectionId));
#endif

  if ((conn = (*connections)[writereq->connectionId]) == 0) {
    PLOG(("Invalid Connection ID: %d",writereq->connectionId));
    return &res;
  }

#ifdef DEBUG
  DLOG(("writereq->connectionId=%d",writereq->connectionId));
#endif

  res = conn->put_rec(writereq);

#ifdef DEBUG
  DLOG(("writedatarec_float_1_svc res=%d",res));
#endif
  return &res;
}

int * writedatarec_long_1_svc(datarec_long *writereq,struct svc_req *)
{

  static int res;
  Connection *conn;
  Connections *connections = Connections::Instance();

  res = -1;

  if ((conn = (*connections)[writereq->connectionId]) == 0) {
    PLOG(("Invalid Connection ID: %d",writereq->connectionId));
    return &res;
  }
  res = conn->put_rec(writereq);
#ifdef DEBUG
  DLOG(("writedatarec_long_1_svc res=%d",res));
#endif
  return &res;
}

void * writedatarecbatch_float_1_svc(datarec_float *writereq,struct svc_req *)
{

  Connections *connections = Connections::Instance();
  Connection *conn;
  int res;

  if ((conn = (*connections)[writereq->connectionId]) == 0) {
    PLOG(("Invalid Connection ID: %d",writereq->connectionId));
    return (void *)0;
  }
  res = conn->put_rec(writereq);

  /* Batch mode, return NULL, so RPC does not reply */
#ifdef DEBUG
  DLOG(("writedatarecbatch_float_1_svc res=%d",res));
#endif
  return (void *)0;
}

void * writedatarecbatch_long_1_svc(datarec_long *writereq,struct svc_req *)
{

  Connections *connections = Connections::Instance();
  Connection *conn;
  int res;

  if ((conn = (*connections)[writereq->connectionId]) == 0) {
    PLOG(("Invalid Connection ID: %d",writereq->connectionId));
    return (void *)0;
  }
  res = conn->put_rec(writereq);
#ifdef DEBUG
  DLOG(("writedatarecbatch_long_1_svc res=%d",res));
#endif

  /* Batch mode, return NULL, so RPC does not reply */
  return (void *)0;
}

int * writehistoryrec_1_svc(historyrec *writereq,struct svc_req *)
{

  Connections *connections = Connections::Instance();
  Connection *conn;
  static int res;

  res = -1;

#ifdef DEBUG
  DLOG(("writehistory writereq->connectionId=%d",writereq->connectionId));
#endif

  if ((conn = (*connections)[writereq->connectionId]) == 0) {
#ifdef DEBUG
    DLOG(("conn=%x",conn));
#endif
    PLOG(("Invalid Connection ID: %d",writereq->connectionId));
    return &res;
  }
#ifdef DEBUG
  DLOG(("writehistory writereq->connectionId=%d",writereq->connectionId));
#endif

  res = conn->put_history(writereq->history);

  return &res;
}

void * writehistoryrecbatch_1_svc(historyrec *writereq,struct svc_req *)
{

  Connections *connections = Connections::Instance();
  Connection *conn;
  int res;

#ifdef DEBUG
  DLOG(("writereq->connectionId=%d",writereq->connectionId));
#endif

  if ((conn = (*connections)[writereq->connectionId]) == 0) {
#ifdef DEBUG
    DLOG(("conn=%x",conn));
#endif
    PLOG(("Invalid Connection ID: %d",writereq->connectionId));
    return (void *)0;
  }
#ifdef DEBUG
  DLOG(("writereq->connectionId=%d",writereq->connectionId));
#endif

  res = conn->put_history(writereq->history);

  /* Batch mode, return NULL, so RPC does not reply */
  return (void *)0;
}

int * closeconnection_1_svc(int *connectionId,struct svc_req *)
{

  static int res;
  Connections *connections = Connections::Instance();

  res = connections->CloseConnection(*connectionId);

  return &res;
}

int * closefiles_1_svc(void *, struct svc_req *)
{
  static int res = 0;
  AllFiles* allfiles = AllFiles::Instance();
  allfiles->close();
  Connections* connections = Connections::Instance();
  connections->CloseOldConnections();
  return &res;
}

int * syncfiles_1_svc(void *, struct svc_req *)
{
  static int res = 0;
  AllFiles* allfiles = AllFiles::Instance();
  allfiles->sync();
  Connections* connections = Connections::Instance();
  connections->CloseOldConnections();
  return &res;
}
void * shutdown_1_svc(void *, struct svc_req *)
{
  AllFiles* allfiles = AllFiles::Instance();
  allfiles->close();
  Connections* connections = Connections::Instance();
  connections->CloseOldConnections();
  nc_shutdown(0);
  return (void *)0;
}

/*
 * this main was stolen from output of rpcgen
 */
int main (int argc, char **argv)
{
  pid_t pid;
  int i;
  struct sockaddr_in saddr;
  int sockfd;

  if (parseRunstring(argc,argv) != 0) return 1;

#ifndef SVR4
  SVCXPRT *transp;
#endif

#ifndef RPC_SVC_FG
		int size;
		struct rlimit rl;
		pid = fork();
		if (pid < 0) {
			perror("cannot fork");
			exit(1);
		}
		if (pid)
			exit(0);
		rl.rlim_max = 0;
		getrlimit(RLIMIT_NOFILE, &rl);
		if ((size = rl.rlim_max) == 0)
			exit(1);
		for (i = 0; i < size; i++)
			(void) close(i);
		i = open("/dev/console", 2);
		(void) dup2(i, 1);
		(void) dup2(i, 2);
		setsid();
		nidas::util::Logger::createInstance("nc_server",LOG_PID,LOG_LOCAL4,getenv("TZ"));
		ILOG(("nc_server starting"));
#endif

  if (getuid() != 0) {
    struct passwd* pwent = getpwuid(getuid());
    WLOG(("\
Warning: userid=%s (%d) is not root.  Calls to rpcbind may fail since we can't use a restricted port number\n",
    (pwent == NULL ? "unknown" : pwent->pw_name),getuid()));
  }

#ifdef SVR4
	if (!svc_create (netcdfserverprog_1, NETCDFSERVERPROG, NETCDFSERVERVERS, "netpath")) {
	  PLOG(("unable to create (NETCDFSERVERPROG, NETCDFSERVERVERS) for netpath."));
	  exit (1);
	}
#else

  (void) pmap_unset(NETCDFSERVERPROG, NETCDFSERVERVERS);

  transp = svctcp_create(RPC_ANYSOCK, 0, 0);
  if (transp == NULL) {
    PLOG(( "cannot create tcp service."));
    exit(1);
  }
  if (!svc_register(transp, NETCDFSERVERPROG, NETCDFSERVERVERS,
        netcdfserverprog_1, IPPROTO_TCP)) {
    PLOG(( "Unable to register (NETCDFSERVERPROG=%x, NETCDFSERVERVERS, tcp): %m",
    	NETCDFSERVERPROG));
    exit(1);
  }
#endif

  if (useruid != getuid()) setuid(useruid);

	svc_run ();
	PLOG(("svc_run returned"));
	exit (1);
	/* NOTREACHED */
}
