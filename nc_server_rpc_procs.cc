//
//              Copyright (C) by UCAR
//

#include "nc_server_rpc.h"
#include "nc_server.h"
#include <nidas/util/Logger.h>

/*
#define  DEBUG
#define  RPC_SVC_FG
*/

int *openconnection_1_svc(connection * input, struct svc_req *)
{

    static int res;

    res = -1;

    Connections *connections = Connections::Instance();

    res = connections->openConnection(input);

    return &res;
}

int *definedatarec_1_svc(datadef * ddef, struct svc_req *)
{

    static int res;
    Connection *conn;
    Connections *connections = Connections::Instance();

    res = -1;

    if ((conn = (*connections)[ddef->connectionId]) == 0) {
        PLOG(("Invalid connection ID: %d", ddef->connectionId));
        return &res;
    }

    res = conn->add_var_group(ddef);

#ifdef DEBUG
    DLOG(("definedatarec_1_svc res=%d", res));
#endif

    return &res;
}

int *writedatarec_float_1_svc(datarec_float * writereq, struct svc_req *)
{

    static int res;
    Connection *conn;
    Connections *connections = Connections::Instance();

    res = -1;

#ifdef DEBUG
    DLOG(("writereq->connectionId=%d", writereq->connectionId));
#endif

    if ((conn = (*connections)[writereq->connectionId]) == 0) {
        PLOG(("Invalid Connection ID: %d", writereq->connectionId));
        return &res;
    }
#ifdef DEBUG
    DLOG(("writereq->connectionId=%d", writereq->connectionId));
#endif

    res = conn->put_rec(writereq);

#ifdef DEBUG
    DLOG(("writedatarec_float_1_svc res=%d", res));
#endif
    return &res;
}

int *writedatarec_int_1_svc(datarec_int * writereq, struct svc_req *)
{

    static int res;
    Connection *conn;
    Connections *connections = Connections::Instance();

    res = -1;

    if ((conn = (*connections)[writereq->connectionId]) == 0) {
        PLOG(("Invalid Connection ID: %d", writereq->connectionId));
        return &res;
    }
    res = conn->put_rec(writereq);
#ifdef DEBUG
    DLOG(("writedatarec_int_1_svc res=%d", res));
#endif
    return &res;
}

void *writedatarecbatch_float_1_svc(datarec_float * writereq,
                                    struct svc_req *)
{

    Connections *connections = Connections::Instance();
    Connection *conn;
    int res;

    if ((conn = (*connections)[writereq->connectionId]) == 0) {
        PLOG(("Invalid Connection ID: %d", writereq->connectionId));
        return (void *) 0;
    }
    res = conn->put_rec(writereq);

    /* Batch mode, return NULL, so RPC does not reply */
#ifdef DEBUG
    DLOG(("writedatarecbatch_float_1_svc res=%d", res));
#endif
    return (void *) 0;
}

void *writedatarecbatch_int_1_svc(datarec_int * writereq,
                                   struct svc_req *)
{

    Connections *connections = Connections::Instance();
    Connection *conn;
    int res;

    if ((conn = (*connections)[writereq->connectionId]) == 0) {
        PLOG(("Invalid Connection ID: %d", writereq->connectionId));
        return (void *) 0;
    }
    res = conn->put_rec(writereq);
#ifdef DEBUG
    DLOG(("writedatarecbatch_int_1_svc res=%d", res));
#endif

    /* Batch mode, return NULL, so RPC does not reply */
    return (void *) 0;
}

int *writehistoryrec_1_svc(historyrec * writereq, struct svc_req *)
{

    Connections *connections = Connections::Instance();
    Connection *conn;
    static int res;

    res = -1;

#ifdef DEBUG
    DLOG(("writehistory writereq->connectionId=%d",
          writereq->connectionId));
#endif

    if ((conn = (*connections)[writereq->connectionId]) == 0) {
#ifdef DEBUG
        DLOG(("conn=%x", conn));
#endif
        PLOG(("Invalid Connection ID: %d", writereq->connectionId));
        return &res;
    }
#ifdef DEBUG
    DLOG(("writehistory writereq->connectionId=%d",
          writereq->connectionId));
#endif

    res = conn->put_history(writereq->history);

    return &res;
}

void *writehistoryrecbatch_1_svc(historyrec * writereq, struct svc_req *)
{

    Connections *connections = Connections::Instance();
    Connection *conn;
    int res;

#ifdef DEBUG
    DLOG(("writereq->connectionId=%d", writereq->connectionId));
#endif

    if ((conn = (*connections)[writereq->connectionId]) == 0) {
#ifdef DEBUG
        DLOG(("conn=%x", conn));
#endif
        PLOG(("Invalid Connection ID: %d", writereq->connectionId));
        return (void *) 0;
    }
#ifdef DEBUG
    DLOG(("writereq->connectionId=%d", writereq->connectionId));
#endif

    res = conn->put_history(writereq->history);

    /* Batch mode, return NULL, so RPC does not reply */
    return (void *) 0;
}

int *closeconnection_1_svc(int *connectionId, struct svc_req *)
{

    static int res;
    Connections *connections = Connections::Instance();

    res = connections->closeConnection(*connectionId);

    return &res;
}

int *closefiles_1_svc(void *, struct svc_req *)
{
    static int res = 0;
    AllFiles *allfiles = AllFiles::Instance();
    allfiles->close();
    Connections *connections = Connections::Instance();
    connections->closeOldConnections();
    return &res;
}

int *syncfiles_1_svc(void *, struct svc_req *)
{
    static int res = 0;
    AllFiles *allfiles = AllFiles::Instance();
    allfiles->sync();
    Connections *connections = Connections::Instance();
    connections->closeOldConnections();
    return &res;
}

void *shutdown_1_svc(void *, struct svc_req *)
{
    AllFiles *allfiles = AllFiles::Instance();
    allfiles->close();
    Connections *connections = Connections::Instance();
    connections->closeOldConnections();
    nc_shutdown(0);
    return (void *) 0;
}
