
#include "nc_server_client.h"

#include <nidas/util/Socket.h>
#include <nidas/util/Logger.h>
#include <stdlib.h> // getenv()

namespace n_u = nidas::util;

CLIENT*
nc_server_client_create(const std::string& servername)
{
    // If NC_SERVER_PORT is set in the environment, then that is the
    // nc_server instance we're supposed to connect to.
    CLIENT* client = 0;
    const char* envport = getenv("NC_SERVER_PORT");
    if (envport)
    {
        int sockp = RPC_ANYSOCK;
        int nc_server_port = atoi(envport);
        n_u::Inet4Address haddr = n_u::Inet4Address::getByName(servername);
        n_u::Inet4SocketAddress saddr(haddr, nc_server_port);

        DLOG(("connecting directly to rpc server at ")
             << saddr.toAddressString());
        client = clnttcp_create((struct sockaddr_in*)saddr.getSockAddrPtr(),
                               NETCDFSERVERPROG, NETCDFSERVERVERS,
                               &sockp, 0, 0);
        DLOG(("clnttcp_create() returned."));
    }
    else
    {
        client = clnt_create(servername.c_str(),
                             NETCDFSERVERPROG, NETCDFSERVERVERS, "tcp");
    }
    return client;
}


void
nc_server_client_destroy(CLIENT* client)
{
    clnt_destroy(client);
}
