
#include "nc_server.h"


int main(int argc, char **argv)
{
    NcServerApp ncserver;
    int res;
    if ((res = ncserver.parseRunstring(argc, argv)))
        return res;
    ncserver.setup();
    return ncserver.run();
}
