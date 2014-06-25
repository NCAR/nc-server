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

int *open_connection_2_svc(connection * input, struct svc_req *)
{

    static int res;

    res = -1;

    Connections *connections = Connections::Instance();

    res = connections->openConnection(input);

    return &res;
}

int *define_datarec_2_svc(datadef * ddef, struct svc_req *)
{

    static int res;
    Connection *conn;
    Connections *connections = Connections::Instance();

    res = -1;

    if ((conn = (*connections)[ddef->connectionId]) == 0) {
        PLOG(("define_datarec: invalid connection ID: %d",
                    (ddef->connectionId & 0xffff)));
        return &res;
    }

    res = conn->add_var_group(ddef);

#ifdef DEBUG
    DLOG(("define_datarec_2_svc res=%d", res));
#endif

    return &res;
}

int *write_datarec_float_2_svc(datarec_float * writereq, struct svc_req *)
{

    static int res;
    Connection *conn;
    Connections *connections = Connections::Instance();

    res = -1;

#ifdef DEBUG
    DLOG(("writereq->connectionId=%d", writereq->connectionId));
#endif

    if ((conn = (*connections)[writereq->connectionId]) == 0) {
        PLOG(("write_datarec_float: invalid connection ID: %d",
                    (writereq->connectionId & 0xffff)));
        return &res;
    }
#ifdef DEBUG
    DLOG(("writereq->connectionId=%d", writereq->connectionId));
#endif

    res = conn->put_rec(writereq);

#ifdef DEBUG
    DLOG(("write_datarec_float_2_svc res=%d", res));
#endif
    return &res;
}

int *write_datarec_int_2_svc(datarec_int * writereq, struct svc_req *)
{

    static int res;
    Connection *conn;
    Connections *connections = Connections::Instance();

    res = -1;

    if ((conn = (*connections)[writereq->connectionId]) == 0) {
        PLOG(("write_datarec_int: nvalid Connection ID: %d",
                    (writereq->connectionId & 0xffff)));
        return &res;
    }
    res = conn->put_rec(writereq);
#ifdef DEBUG
    DLOG(("write_datarec_int_2_svc res=%d", res));
#endif
    return &res;
}

void *write_datarec_batch_float_2_svc(datarec_float * writereq,
                                    struct svc_req *)
{

    Connections *connections = Connections::Instance();
    Connection *conn;

    if ((conn = (*connections)[writereq->connectionId]) == 0) {
        PLOG(("write_datarecbatch_float: invalid connection ID: %d",
                    (writereq->connectionId & 0xffff)));
        return (void *) 0;
    }
    conn->put_rec(writereq);

    /* Batch mode, return NULL, so RPC does not reply */
#ifdef DEBUG
    DLOG(("write_datarec_batch_float_2_svc res=%d", res));
#endif
    return (void *) 0;
}

void *write_datarec_batch_int_2_svc(datarec_int * writereq,
                                   struct svc_req *)
{
    Connections *connections = Connections::Instance();
    Connection *conn;

    if ((conn = (*connections)[writereq->connectionId]) == 0) {
        PLOG(("write_datarec_batch_int: invalid connection ID: %d",
                    (writereq->connectionId & 0xffff)));
        return (void *) 0;
    }
    conn->put_rec(writereq);
#ifdef DEBUG
    DLOG(("write_datarec_batch_int_2_svc res=%d", res));
#endif

    /* Batch mode, return NULL, so RPC does not reply */
    return (void *) 0;
}

int *write_history_2_svc(history_attr * attr, struct svc_req *)
{

    Connections *connections = Connections::Instance();
    Connection *conn;
    static int res;

    res = -1;

#ifdef DEBUG
    DLOG(("write_history attr->connectionId=%d",
          attr->connectionId));
#endif

    if ((conn = (*connections)[attr->connectionId]) == 0) {
#ifdef DEBUG
        DLOG(("conn=%x", conn));
#endif
        PLOG(("write_history: invalid connection ID: %d",
                    attr->connectionId));
        return &res;
    }
#ifdef DEBUG
    DLOG(("write_history attr->connectionId=%d",
          attr->connectionId));
#endif

    res = conn->put_history(attr->history);

    return &res;
}

void *write_history_batch_2_svc(history_attr * attr, struct svc_req *)
{

    Connections *connections = Connections::Instance();
    Connection *conn;

#ifdef DEBUG
    DLOG(("attr->connectionId=%d", attr->connectionId));
#endif

    if ((conn = (*connections)[attr->connectionId]) == 0) {
#ifdef DEBUG
        DLOG(("conn=%x", conn));
#endif
        PLOG(("write_history_batch: invalid connection ID: %d",
                    (attr->connectionId & 0xffff)));
        return (void *) 0;
    }
#ifdef DEBUG
    DLOG(("attr->connectionId=%d", attr->connectionId));
#endif

    conn->put_history(attr->history);

    /* Batch mode, return NULL, so RPC does not reply */
    return (void *) 0;
}

int *write_global_attr_2_svc(global_attr * attr, struct svc_req *)
{

    Connections *connections = Connections::Instance();
    Connection *conn;
    static int res;

    res = -1;

#ifdef DEBUG
    DLOG(("write_global_attr: attr->connectionId=%d",
          attr->connectionId));
#endif

    if ((conn = (*connections)[attr->connectionId]) == 0) {
#ifdef DEBUG
        DLOG(("conn=%x", conn));
#endif
        PLOG(("write_global_attr: invalid connection ID: %d",
                    attr->connectionId));
        return &res;
    }
#ifdef DEBUG
    DLOG(("write_global_attr: attr->connectionId=%d",
          attr->connectionId));
#endif

    res = conn->write_global_attr(attr->attr.name,attr->attr.value);

    return &res;
}

int *write_global_int_attr_2_svc(global_int_attr * attr, struct svc_req *)
{

    Connections *connections = Connections::Instance();
    Connection *conn;
    static int res;

    res = -1;

#ifdef DEBUG
    DLOG(("write_global_int_attr: attr->connectionId=%d",
          attr->connectionId));
#endif

    if ((conn = (*connections)[attr->connectionId]) == 0) {
#ifdef DEBUG
        DLOG(("conn=%x", conn));
#endif
        PLOG(("write_global_int_attr: invalid connection ID: %d",
                    attr->connectionId));
        return &res;
    }
#ifdef DEBUG
    DLOG(("write_global_int_attr: attr->connectionId=%d",
          attr->connectionId));
#endif

    res = conn->write_global_attr(attr->name,attr->value);

    return &res;
}

int *close_connection_2_svc(int *connectionId, struct svc_req *)
{

    static int res;
    Connections *connections = Connections::Instance();

    res = connections->closeConnection(*connectionId);

    return &res;
}

int *close_files_2_svc(void *, struct svc_req *)
{
    static int res = 0;
    AllFiles *allfiles = AllFiles::Instance();
    allfiles->close();
    Connections *connections = Connections::Instance();
    connections->closeOldConnections();
    return &res;
}

int *sync_files_2_svc(void *, struct svc_req *)
{
    static int res = 0;
    AllFiles *allfiles = AllFiles::Instance();
    allfiles->sync();
    Connections *connections = Connections::Instance();
    connections->closeOldConnections();
    return &res;
}

void *shutdown_2_svc(void *, struct svc_req *)
{
    AllFiles *allfiles = AllFiles::Instance();
    allfiles->close();
    Connections *connections = Connections::Instance();
    connections->closeOldConnections();
    nc_shutdown(0);
    return (void *) 0;
}

char **check_error_2_svc(int * id,struct svc_req *)
{
    static char * result = 0;
    Connections *connections = Connections::Instance();
    Connection *conn;

    if ((conn = (*connections)[*id]) == 0) {
        std::ostringstream ost;
        ost << "check_error: invalid connection ID " << (*id & 0xffff);
        PLOG(("%s",ost.str().c_str()));
        free(result);
        result = strdup(ost.str().c_str());
        return &result;
    }

    if (conn->getState() != Connection::CONN_OK) {
        free(result);
        result = strdup(conn->getErrorMsg().c_str());
        return &result;
    }
    else {
        if (!result || result[0]) {
            free(result);
            result = (char*)malloc(1);
            result[0] = 0;
        }
        return &result;
    }
}
