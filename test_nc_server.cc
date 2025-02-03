

#define BOOST_TEST_DYN_LINK 
#define BOOST_TEST_MODULE test nc_server
#include <boost/test/unit_test.hpp>
namespace utf = boost::unit_test;

#include "nc_server.h"
#include <memory>
#include <stdlib.h> // system()

using std::string;
using std::unique_ptr;

using nidas::util::UTime;


BOOST_AUTO_TEST_CASE(create_basic_netcdf_file)
{
    string xfile = "./testing_isfs_20231206_000000.nc";
    system((string("/bin/rm -f ") + xfile).c_str());

    // std::string fileName{"/tmp/test_nc_server_datafile.nc"};
    // auto openmode{NcFile::Replace};
    double length = 24 * 3600;
    double interval = 300;
    char filename[] = "testing_isfs_%Y%m%d_%H%M%S.nc";
    char filedir[] = ".";
    char cdlfile[] = "";
    double dtime = UTime(true, 2023, 12, 6, 0, 0, 0).toDoubleSecs();
    NS_NcFile* ncfile;

    connection con{
        length, interval, filename, filedir, cdlfile
    };
    FileGroup filegroup(&con);

    ncfile = filegroup.get_file(dtime);

    BOOST_TEST(ncfile->is_valid());
    ncfile->sync();

    NcVar* base_time = ncfile->get_var("base_time");
    BOOST_TEST(base_time);

    BOOST_TEST(base_time->as_double(0) == 1701820800);

    BOOST_TEST(ncfile->getName() == xfile);

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


BOOST_AUTO_TEST_CASE(test_att_as_type)
{
    int ival;
    double dval;
    BOOST_TEST(att_as_type("int:", "int:123", ival));
    BOOST_TEST(ival == 123);
    BOOST_TEST(att_as_type("float:", "float:123.456", dval));
    BOOST_TEST(dval == 123.456);
    BOOST_TEST(!att_as_type("int:", "int:", ival));
    BOOST_TEST(!att_as_type("int:", "int", ival));
    BOOST_TEST(!att_as_type("float:", "float", dval));
}
