

#define BOOST_TEST_DYN_LINK 
#define BOOST_TEST_MODULE test nc_server
#include <boost/test/unit_test.hpp>
namespace utf = boost::unit_test;

#include "nc_server.h"
#include <memory>

using std::unique_ptr;


BOOST_AUTO_TEST_CASE(create_basic_netcdf_file)
{
    std::string fileName{"/tmp/test_nc_server_datafile.nc"};
    auto openmode{NcFile::Replace};
    auto length = 24 * 3600;
    auto interval = 300;
    double dtime = 1701874262;
    unique_ptr<NS_NcFile> ncfile;
    ncfile.reset(new NS_NcFile(fileName, openmode, interval, length, dtime));

    BOOST_TEST(ncfile->is_valid());
    ncfile->sync();

    NcVar* base_time = ncfile->get_var("base_time");
    BOOST_TEST(base_time);

    BOOST_TEST(base_time->as_double(0) == 1701820800);

#ifdef notdef
    // Attempt at adding a variable, but add_var() is not public, and I'm not
    // sure this is the right way to go about this, so defer until really
    // needed.
    Variable var("T.2.5m.north");
    OutVariable tdry{var, NS_FLOAT, -999, -999};

    bool modified{false};
    unique_ptr<NS_NcVar> vtdry{ncfile->add_var(&tdry, modified)};
#endif

    ncfile->sync();
}
