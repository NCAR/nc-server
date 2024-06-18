
#include "nc_server.h"

#include <iostream>

int main(int argc, char **argv)
{
    NcServerApp ncserver;
    int res;
    if ((res = ncserver.parseRunstring(argc, argv)))
        return res;
    try {
        ncserver.setup();
        return ncserver.run();
    }
    catch (std::exception& ex)
    {
        std::cerr << "exception: " << ex.what() << std::endl;
    }
}
