#ifndef _nc_server_client_h_
#define _nc_server_client_h_

#include "nc_server_rpc.h"

#include <string>

/**
 * Create a client connected to the server on host @p servername.
 *
 * This looks up the server using the RPC portmapper service unless the
 * environment variable NC_SERVER_PORT is set, in which case that port number
 * is used to create the client connection.  Returns null if the server
 * connection fails.
 *
 * @param servername
 * @return CLIENT*
 */
CLIENT*
nc_server_client_create(const std::string& servername);

/**
 * Destroys a client created by nc_server_client_create().
 *
 * Essentially just a wrapper for clnt_destroy(), to be symmetrical with
 * nc_server_client_create().
 *
 * @param client
 */
void
nc_server_client_destroy(CLIENT* client);

// This is a header-only library so it can be used easily by both nc_server
// clients and the nidas shared modules, without adding a dependency on
// nidas_util to libnc_server_rpc and without adding another library.
#include "nc_server_client.cc"

#endif // _nc_server_client_h_
