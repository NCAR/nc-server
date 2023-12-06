

#define BOOST_TEST_DYN_LINK 
#define BOOST_TEST_MODULE xml config test
#include <boost/test/unit_test.hpp>
namespace utf = boost::unit_test;

#include "nc_server.h"


BOOST_AUTO_TEST_CASE(create_basic_netcdf_file)
{
    std::string fileName{"/tmp/test_nc_server_datafile.nc"};
    auto openmode{NcFile::Replace};
    auto length = 24 * 3600;
    auto interval = 300;
    double dtime = 1701874262;
    NS_NcFile *ncfile = new NS_NcFile(fileName, openmode, interval,
                                      length, dtime);

    BOOST_TEST(ncfile->is_valid());
    ncfile->sync();

    NcVar* base_time = ncfile->get_var("base_time");
    BOOST_TEST(base_time);

    BOOST_TEST(base_time->as_double(0) == 1701820800);

    delete ncfile;
}
