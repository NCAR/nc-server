// -*- mode: C++; indent-tabs-mode: nil; c-basic-offset: 4; -*-
// vim: set shiftwidth=4 softtabstop=4 expandtab:
/*
 ********************************************************************
 ** NIDAS: NCAR In-situ Data Acquistion Software
 **
 ** 2006, Copyright University Corporation for Atmospheric Research
 **
 ** This program is free software; you can redistribute it and/or modify
 ** it under the terms of the GNU General Public License as published by
 ** the Free Software Foundation; either version 2 of the License, or
 ** (at your option) any later version.
 **
 ** This program is distributed in the hope that it will be useful,
 ** but WITHOUT ANY WARRANTY; without even the implied warranty of
 ** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 ** GNU General Public License for more details.
 **
 ** The LICENSE.txt file accompanying this software contains
 ** a copy of the GNU General Public License. If it is not found,
 ** write to the Free Software Foundation, Inc.,
 ** 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 **
 ********************************************************************
*/

#include "NetcdfRPCChannel.h"
#include "nc_server_client.h"
#include "CStringCache.h"

#include <nidas/core/DSMConfig.h>
#include <nidas/core/Site.h>
#include <nidas/core/Project.h>
#include <nidas/core/SampleTag.h>
#include <nidas/core/CalFile.h>
#include <nidas/core/Variable.h>
#include <nidas/core/Version.h>
#include <nidas/util/Logger.h>
#include <nidas/util/UTime.h>
#include <nidas/util/Process.h>
#include <nidas/util/util.h>

#include <stdlib.h>

using namespace nidas::dynld::isff;
using namespace std;
using namespace nidas::core;

namespace n_u = nidas::util;
using nidas::util::LogContext;
using nidas::util::LogMessage;


NIDAS_CREATOR_FUNCTION_NS(isff,NetcdfRPCChannel)


NetcdfRPCChannel::NetcdfRPCChannel():
    _name(),_server(),_fileNameFormat(),_directory(),_cdlFileName(),
    _fillValue(1.e37), _fileLength(SECS_PER_DAY),
    _clnt(0), _connectionId(0), _rpcBatchPeriod(300),
    _rpcWriteTimeout(),_rpcOtherTimeout(),_rpcBatchTimeout(),
    _ntry(0),_lastNonBatchWrite(0),
    _groupById(),_stationIndexById(),_groups(),
    _sampleTags(), _constSampleTags(),
    _timeInterval(300)
{
    setName("NetcdfRPCChannel");
    setRPCTimeout(300);
    _rpcBatchTimeout.tv_sec = 0;
    _rpcBatchTimeout.tv_usec = 0;
}

/* copy constructor */
NetcdfRPCChannel::NetcdfRPCChannel(const NetcdfRPCChannel& x):
    IOChannel(x),
    _name(x._name),
    _server(x._server),
    _fileNameFormat(x._fileNameFormat),
    _directory(x._directory),
    _cdlFileName(x._cdlFileName),
    _fillValue(x._fillValue),
    _fileLength(x._fileLength),
    _clnt(0),_connectionId(0),
    _rpcBatchPeriod(x._rpcBatchPeriod),
    _rpcWriteTimeout(x._rpcWriteTimeout),
    _rpcOtherTimeout(x._rpcOtherTimeout),
    _rpcBatchTimeout(x._rpcBatchTimeout),
    _ntry(0),_lastNonBatchWrite(0),
    _groupById(),_stationIndexById(),_groups(),
    _sampleTags(), _constSampleTags(),
    _timeInterval(x._timeInterval)
{
    _rpcBatchTimeout.tv_sec = 0;
    _rpcBatchTimeout.tv_usec = 0;
}

NetcdfRPCChannel::~NetcdfRPCChannel()
{
    list<SampleTag*>::iterator si = _sampleTags.begin();
    for ( ; si != _sampleTags.end(); ++si) delete *si;
}

void NetcdfRPCChannel::addSampleTag(const SampleTag* val)
{
    SampleTag* tag = new SampleTag(*val);
    _sampleTags.push_back(tag);
    _constSampleTags.push_back(tag);
}

void NetcdfRPCChannel::setName(const std::string& val)
{
    _name = val;
}

void NetcdfRPCChannel::setServer(const string& val)
{
    _server = val;
    setName(string("ncserver: ") + getServer() + ':' + 
            getDirectory() + "/" + getFileNameFormat());
}

void NetcdfRPCChannel::setFileNameFormat(const string& val)
{
    _fileNameFormat = val;
    setName(string("ncserver: ") + getServer() + ':' + 
            getDirectory() + "/" + getFileNameFormat());
}

void NetcdfRPCChannel::setDirectory(const string& val)
{
    _directory = val;
    setName(string("ncserver: ") + getServer() + ':' + 
            getDirectory() + "/" + getFileNameFormat());
}

void NetcdfRPCChannel::setRPCTimeout(int secs)
{
    _rpcWriteTimeout.tv_sec = secs;
    _rpcWriteTimeout.tv_usec = 0;
    _rpcOtherTimeout.tv_sec = secs * 5;
    _rpcOtherTimeout.tv_usec = 0;
}

int NetcdfRPCChannel::getRPCTimeout() const
{
    return _rpcWriteTimeout.tv_sec;
}

struct timeval& NetcdfRPCChannel::getRPCWriteTimeoutVal()
{
    return _rpcWriteTimeout;
}

struct timeval& NetcdfRPCChannel::getRPCOtherTimeoutVal()
{
    return _rpcOtherTimeout;
}

void NetcdfRPCChannel::setRPCBatchPeriod(int secs)
{
    _rpcBatchPeriod = secs;
}

int NetcdfRPCChannel::getRPCBatchPeriod() const
{
    return _rpcBatchPeriod;
}

void NetcdfRPCChannel::requestConnection(IOChannelRequester* rqstr)
{
    connect();
    rqstr->connected(this);
}


IOChannel* NetcdfRPCChannel::connect()
{
    // expand the file and directory names.

    if (getDSMConfig()) {
        setDirectory(getDSMConfig()->expandString(getDirectory()));
        setFileNameFormat(getDSMConfig()->expandString(getFileNameFormat()));
        setCDLFileName(getDSMConfig()->expandString(getCDLFileName()));
    }
    else {
        setDirectory(Project::getInstance()->expandString(getDirectory()));
        setFileNameFormat(Project::getInstance()->expandString(getFileNameFormat()));
        setCDLFileName(Project::getInstance()->expandString(getCDLFileName()));
    }

    _clnt = nc_server_client_create(getServer());
    if (_clnt == (CLIENT *) NULL)
    {
        throw n_u::IOException(getName(),"clnt_create",
            clnt_spcreateerror(_server.c_str()));
    }

    connection conn;
    conn.filenamefmt = (char *)getFileNameFormat().c_str();

    conn.outputdir = (char *)getDirectory().c_str();
    conn.cdlfile = (char *)getCDLFileName().c_str();
    conn.filelength = getFileLength();
    conn.interval = getTimeInterval();

    int result = 0;
    enum clnt_stat clnt_stat;

    DLOG(("calling clnt_call(OPEN_CONNECTION)"));
    if ((clnt_stat = clnt_call(_clnt, OPEN_CONNECTION,
                               (xdrproc_t) xdr_connection, (caddr_t) &conn,
                               (xdrproc_t) xdr_int,  (caddr_t) &result,
                               _rpcOtherTimeout)) != RPC_SUCCESS)
    {
        n_u::IOException e(getName(), "open",
                           clnt_sperror(_clnt,_server.c_str()));
        nc_server_client_destroy(_clnt);
        _clnt = 0;
        throw e;
    }

    _connectionId = result;
    if (_connectionId < 0) {
        nc_server_client_destroy(_clnt);
        _clnt = 0;
        throw n_u::IOException(getName(),"open",
            string("perhaps ") + _directory + " does not exist on server");
    }

    {
        ostringstream idstr;
        idstr << (_connectionId & 0xffff);
        setName(string("ncserver: ") + getServer() + ':' + 
            getDirectory() + "/" + getFileNameFormat() + ", id " + idstr.str());
    }

    _lastNonBatchWrite = time((time_t *)0);

    return this;
}


void NetcdfRPCChannel::defineData()
{
    Project* project = Project::getInstance();

    unsigned int nstations = 0;
    if (project->getMaxSiteNumber() > 0)
            nstations = project->getMaxSiteNumber() -
                project->getMinSiteNumber() + 1;
    set<int> stns;
    
    list<const SampleTag*> tags = getSampleTags();
    list<const SampleTag*>::const_iterator si = tags.begin();
    for ( ; si != tags.end(); ++si) {
        const SampleTag* stag = *si;

        // station: -1 unknown, 0 for non-station, otherwise > 0
        int tagStation = stag->getStation();
        for (VariableIterator vi = stag->getVariableIterator();
                vi.hasNext(); ) {
            const Variable* var = vi.next();

            VLOG(("NetcdfRPCChannel::connect(), stag=")
                 << stag->getDSMId() << ',' << stag->getSpSId()
                 << ", var=" << var->getName()
                 << " varstation=" << var->getStation()
                 << " tagstation=" << tagStation);
            int vstn = var->getStation();

            if (vstn < 0)
                WLOG(("var %s is from station %d", 
                      var->getName().c_str(), vstn));

            if (tagStation < 0) tagStation = vstn;
            if (vstn > 0) stns.insert(vstn);
            if (tagStation >= 0 && vstn != tagStation)
            {
                WLOG(("var %s is from station %d, others in this sample "
                      "are from %d", var->getName().c_str(), vstn, tagStation));
            }
        }
        _stationIndexById[stag->getId()] = tagStation - 1;
    }

    vector<ParameterT<int> > dims;
    vector<ParameterT<int> > noStnDims;

    // if we have data from stations with value > 0
    if (stns.size() > 0) {
        if (stns.size() != nstations)
            WLOG(("nstations=%d, stns.size()=%d",
                  nstations, stns.size()));

        ParameterT<int> stnDim;
        stnDim.setName("station");
        stnDim.setValue(nstations);
        dims.push_back(stnDim);
    }

    si = tags.begin();
    for ( ; si != tags.end(); ++si) {
        const SampleTag* stag = *si;

        NcVarGroupFloat* grp = getNcVarGroupFloat(dims,stag);
        if (!grp) {
            if (_stationIndexById[stag->getId()] < 0)
                grp = new NcVarGroupFloat(noStnDims,stag,_fillValue);
            else
                grp = new NcVarGroupFloat(dims,stag,_fillValue);
            grp->connect(this,_fillValue);
            _groups.push_back(grp);
        }
        VLOG(("adding to groupById, tag=")
             << stag->getDSMId() << ',' << stag->getSpSId()
             << ", dims.size()=" << dims.size());
        _groupById[stag->getId()] = grp;
    }

    writeGlobalAttr("NIDAS_version", Version::getSoftwareVersion());

    // previously project_config had the form <config>=<version>, where
    // <config> was the expanded xml file path and <version> was generated
    // from svn status.  these are now separate.  project_config is now just a
    // project config specifier, as specific as possible, naming not just the
    // xml file but the config name in a multi-config project.
    // project_version is whatever is passed in the environment, so it's up to
    // the caller to have generated it.  the isfs_config_begin and
    // isfs_config_end are more useful context for the global attributes, but
    // they are also used when a netcdf file is opened to clamp a filename to
    // the beginning of a config which otherwise would be clamped to the
    // interval.

    string configName;
    const char* env = getenv("ISFS_CONFIG_SPECIFIER");
    configName = env ? env : "";
    if (configName.empty())
        configName = Project::getInstance()->getConfigName();
    if (configName.length() > 0) {
        writeGlobalAttr("project_config", configName);
    }

    if ((env = getenv("ISFS_CONFIG_VERSION")))
    {
        writeGlobalAttr("project_version", env);
    }
    if ((env = getenv("ISFS_CONFIG_BEGIN")))
    {
        writeGlobalAttr("isfs_config_begin", env);
    }
    if ((env = getenv("ISFS_CONFIG_END")))
    {
        writeGlobalAttr("isfs_config_end", env);
    }

    const Dataset& dataset = Project::getInstance()->getDataset();

    if (dataset.getName().length() > 0) {
        writeGlobalAttr("dataset", dataset.getName());
        if (dataset.getDescription().length() > 0)
            writeGlobalAttr("dataset_description", dataset.getDescription());
    }
    else if ((env = getenv("DATASET")))
    {
        // fall back to the name in the environment, assuming it must be the
        // active settings.  however, the description is not available.
        writeGlobalAttr("dataset", env);
        if ((env = getenv("DATASET_DESCRIPTION")))
            writeGlobalAttr("dataset_description", env);
    }

    // Write some string project parameters as NetCDF global attributes
    const char* str_params[] = {"wind3d_horiz_coordinates", 0 };

    for (const char** pstr = str_params; *pstr; pstr++) {
        const Parameter* parm =
            Project::getInstance()->getParameter(*pstr);
        if (parm && parm->getType() == Parameter::STRING_PARAM &&
                parm->getLength() == 1) {
            string val = parm->getStringValue(0);
            writeGlobalAttr(*pstr, val);
        }
    }

    // Write some integer project parameters as NetCDF global attributes
    const char* int_params[] = {
        "wind3d_horiz_rotation", "wind3d_tilt_correction", 0
    };
    for (const char** pstr = int_params; *pstr; pstr++) {
        const Parameter* parm =
            Project::getInstance()->getParameter(*pstr);
        if (parm && ((parm->getType() == Parameter::BOOL_PARAM ||
                parm->getType() == Parameter::INT_PARAM ||
                parm->getType() == Parameter::FLOAT_PARAM) &&
                parm->getLength() == 1)) {
            int val = (int) parm->getNumericValue(0);
            writeGlobalAttr(*pstr, val);
        }
    }

    // these two attributes, file_length_seconds and calibration_file_path,
    // seem to be redundant or ambiguous.  the actual calibration path is
    // different for every sensor, and the global attribute should be
    // superceded by the calfiles added to specific variables as attributes.
    // the file length of course is redundant to general users of the data,
    // because the actual length can be determined directly from the time
    // variable.  however, these are both used by the isfs R package, so they
    // remain.

    // Write file length as a global attribute
    writeGlobalAttr("file_length_seconds", getFileLength());

    string cpstr;
    const vector<string>& calpaths = nidas::core::CalFile::getAllPaths();
    for (vector<string>::const_iterator pi = calpaths.begin(); pi != calpaths.end(); ++pi) {
        string cpath = Project::getInstance()->expandString(*pi);
        if (cpstr.length() > 0) cpstr += ':';
        cpstr += cpath;
    }
    if (cpstr.length() > 0)
        writeGlobalAttr("calibration_file_path", cpstr);
}

NcVarGroupFloat* NetcdfRPCChannel::getNcVarGroupFloat(
        const vector<ParameterT<int> >&dims,
        const SampleTag* stag)
{
    for (auto& grp: _groups)
    {
        const auto& dimensions = grp->getDimensions();
        if (dimensions.size() != dims.size()) continue;

        auto di1 = dimensions.begin();
        auto di2 = dims.begin();
        for ( ; di1 != dimensions.end(); ++di1, ++di2)
        {
            const ParameterT<int>& p1 = *di1;
            const ParameterT<int>& p2 = *di2;
            if (p1.getName() != p2.getName()) break;
            if (p1.getLength() != 1) break;
            if (p1.getLength() != p2.getLength()) break;
            if (p1.getValue(0) != p2.getValue(0)) break;
        }
        if (di1 != dimensions.end())
            continue;

        const auto& grp_variables = grp->getVariables();
        const auto& tag_variables = stag->getVariables();
        if (grp_variables.size() != tag_variables.size())
            continue;
        auto vi1 = grp_variables.begin();
        auto vi2 = tag_variables.begin();
        for ( ; vi1 != grp_variables.end(); ++vi1, ++vi2)
        {
            const Variable* v1 = *vi1;
            const Variable* v2 = *vi2;
            if (!(*v1 == *v2)) break;
        }
        if (grp->getInterval() != stag->getPeriod())
            continue;
        if (vi1 == grp_variables.end())
            return grp;
    }
    return 0;
}

void NetcdfRPCChannel::write(const Sample* samp) 
{
    if (!_data_defined)
    {
        defineData();
        _data_defined = true;
    }
    dsm_sample_id_t sampid = samp->getId();

    map<dsm_sample_id_t,NcVarGroupFloat*>::const_iterator gi =
            _groupById.find(sampid);

    VLOG(("NetcdfRPCChannel::write: ")
         << n_u::UTime(samp->getTimeTag()).format(true,"%Y %m %d %H:%M:%S.%3f")
         << ' ' << GET_DSM_ID(sampid) << ',' << GET_SHORT_ID(sampid)
         << " group.size=" << _groupById.size()
         << " found group=" << (gi != _groupById.end()));
    if (gi == _groupById.end()) return;

    NcVarGroupFloat* g = gi->second;

    int stationIndex = _stationIndexById[samp->getId()];

    VLOG(("NetcdfRPCChannel::write, stationIndex=") << stationIndex);
    g->write(this,samp,stationIndex);
}

void NetcdfRPCChannel::write(datarec_float *rec)
{
    /*
     * Every so often in batch mode check if nc_server actually responds.
     */
    if (_rpcBatchPeriod == 0 || time(0) - _lastNonBatchWrite > _rpcBatchPeriod) {
        nonBatchWrite(rec);
        return;
    }

    /*
     * For RPC batch mode, the timeout is set to 0.
     */
    VLOG(("NetcdfRPRChannel::write ")
         << n_u::UTime(rec->time).format(true,"%Y %m %d %H:%M:%S.%3f ")
         << " id=" << rec->datarecId << " v[0]=" << rec->data.data_val[0]);
    enum clnt_stat clnt_stat;
    clnt_stat = clnt_call(_clnt, WRITE_DATAREC_BATCH_FLOAT,
        (xdrproc_t) xdr_datarec_float, (caddr_t) rec,
        (xdrproc_t) NULL, (caddr_t) NULL,
        _rpcBatchTimeout);
    if (clnt_stat != RPC_SUCCESS)
        throw n_u::IOException(getName(),"write",clnt_sperror(_clnt,""));
}

void NetcdfRPCChannel::nonBatchWrite(datarec_float *rec)
{
    int result = 0;
    enum clnt_stat clnt_stat;

    for ( ; ; ) {
        clnt_stat = clnt_call(_clnt, WRITE_DATAREC_FLOAT,
            (xdrproc_t) xdr_datarec_float, (caddr_t) rec,
            (xdrproc_t) xdr_int, (caddr_t) &result,
            _rpcWriteTimeout);
        if (clnt_stat != RPC_SUCCESS) {
            bool serious = (clnt_stat != RPC_TIMEDOUT && clnt_stat != RPC_CANTRECV) ||
                _ntry++ >= NTRY;
            if (serious) 
                throw n_u::IOException(getName(),"write", clnt_sperror(_clnt,""));
            if (_ntry > NTRY / 2) {
                WLOG(("%s: %s, timeout=%d secs, ntry=%d",
                      getName().c_str(),
                      clnt_sperror(_clnt, "nc_server not responding"),
                      _rpcWriteTimeout.tv_sec, _ntry));
            }
        }
        else {
            if (!result && _ntry > 0)
                WLOG(("") << getName() << ": OK");
            _ntry = 0;
            /* If result is non-zero, then an error occured on nc_server.
             * checkError() will retrieve the error string and throw the exception.
             */
            if (result) {
                checkError();
                // checkError should throw an exception if the above call returned a
                // negative result.  If not something's not working right.
                throw n_u::IOException(getName(),"write","unknown error");
            }
            _lastNonBatchWrite = time((time_t*)0);
            break;
        }
    }
}

void NetcdfRPCChannel::writeGlobalAttr(const string& name, const string& value)
{
    int result = 0;
    enum clnt_stat clnt_stat;

    global_attr gattr;
    gattr.connectionId = _connectionId;
    gattr.attr.name = const_cast<char*>(name.c_str());
    gattr.attr.value = const_cast<char*>(value.c_str());

    clnt_stat = clnt_call(_clnt, WRITE_GLOBAL_ATTR,
        (xdrproc_t) xdr_global_attr, (caddr_t) &gattr,
        (xdrproc_t) xdr_int, (caddr_t) &result,
        _rpcWriteTimeout);
    if (clnt_stat != RPC_SUCCESS) {
        bool serious = (clnt_stat != RPC_TIMEDOUT && clnt_stat != RPC_CANTRECV) ||
            _ntry++ >= NTRY;
        if (serious) 
            throw n_u::IOException(getName(),"writeGlobalAttr", clnt_sperror(_clnt,""));
        if (_ntry > NTRY / 2) {
            WLOG(("%s: %s, timeout=%d secs, ntry=%d",
                  getName().c_str(),
                  clnt_sperror(_clnt, "nc_server not responding"),
                  _rpcWriteTimeout.tv_sec, _ntry));
        }
    }
    else {
        if (!result && _ntry > 0)
            WLOG(("") << getName() << ": OK");
        _ntry = 0;
        /* If result is non-zero, then an error occured on nc_server.
         * checkError() will retrieve the error string and throw the exception.
         */
        if (result) {
            checkError();
            // checkError should throw an exception if the above call returned a
            // negative result.  If not something's not working right.
            throw n_u::IOException(getName(),"writeGlobalAttr","unknown error");
        }
        _lastNonBatchWrite = time((time_t*)0);
    }
}

void NetcdfRPCChannel::writeGlobalAttr(const string& name, int value)
{
    int result = 0;
    enum clnt_stat clnt_stat;

    global_int_attr gattr;
    gattr.connectionId = _connectionId;
    gattr.name = const_cast<char*>(name.c_str());
    gattr.value = value;

    clnt_stat = clnt_call(_clnt, WRITE_GLOBAL_INT_ATTR,
        (xdrproc_t) xdr_global_int_attr, (caddr_t) &gattr,
        (xdrproc_t) xdr_int, (caddr_t) &result,
        _rpcWriteTimeout);
    if (clnt_stat != RPC_SUCCESS) {
        bool serious = (clnt_stat != RPC_TIMEDOUT && clnt_stat != RPC_CANTRECV) ||
            _ntry++ >= NTRY;
        if (serious) 
            throw n_u::IOException(getName(),"writeGlobalAttr", clnt_sperror(_clnt,""));
        if (_ntry > NTRY / 2) {
            WLOG(("%s: %s, timeout=%d secs, ntry=%d",
                  getName().c_str(),
                  clnt_sperror(_clnt, "nc_server not responding"),
                  _rpcWriteTimeout.tv_sec, _ntry));
        }
    }
    else {
        if (!result && _ntry > 0)
            WLOG(("") << getName() << ": OK");
        _ntry = 0;
        /* If result is non-zero, then an error occured on nc_server.
         * checkError() will retrieve the error string and throw the exception.
         */
        if (result) {
            checkError();
            // checkError should throw an exception if the above call returned a
            // negative result.  If not something's not working right.
            throw n_u::IOException(getName(),"writeGlobalAttr","unknown error");
        }
        _lastNonBatchWrite = time((time_t*)0);
    }
}

void NetcdfRPCChannel::checkError()
{
    VLOG(("NetcdfRPRChannel::checkError ")
         << n_u::UTime().format(true,"%Y %m %d %H:%M:%S.%3f "));

    char* errormsg = 0;
    enum clnt_stat clnt_stat = clnt_call(_clnt, CHECK_ERROR,
        (xdrproc_t) xdr_int, (caddr_t) &_connectionId,
        (xdrproc_t) xdr_wrapstring, (caddr_t) &errormsg,
        _rpcWriteTimeout);

    if (clnt_stat != RPC_SUCCESS) {
        bool serious = (clnt_stat != RPC_TIMEDOUT && clnt_stat != RPC_CANTRECV) ||
                _ntry++ >= NTRY;
        if (serious)
            throw n_u::IOException(getName(),"checkError",clnt_sperror(_clnt,""));
        WLOG(("%s: %s, timeout=%d secs, ntry=%d",
              getName().c_str(),
              clnt_sperror(_clnt, "nc_server not responding"),
              _rpcWriteTimeout.tv_sec, _ntry));
    }
    else {
        if (!errormsg[0] && _ntry > 0)
            WLOG(("") << getName() << ": OK");
        _ntry = 0;
        _lastNonBatchWrite = time((time_t*)0);

        /*
           If error string is non-empty, then an error occured on nc_server.
           The returned string must be freed.
        */
        if (errormsg[0]) {
            string msg = errormsg;
            xdr_free((xdrproc_t)xdr_wrapstring,(char*)&errormsg);
            throw n_u::IOException(getName(),"write",msg);
        }
        xdr_free((xdrproc_t)xdr_wrapstring,(char*)&errormsg);
    }
}

void NetcdfRPCChannel::close()
{
    list<NcVarGroupFloat*>::const_iterator gi = _groups.begin();
    for ( ; gi != _groups.end(); ++gi) delete *gi;
    _groups.clear();
    _groupById.clear();

    if (_clnt) {
        int result = 0;
        enum clnt_stat clnt_stat;
        if ((clnt_stat = clnt_call(_clnt, CLOSE_CONNECTION,
            (xdrproc_t) xdr_int, (caddr_t) &_connectionId,
            (xdrproc_t) xdr_int, (caddr_t) &result,
            _rpcOtherTimeout)) != RPC_SUCCESS) {
          n_u::IOException e(getName(),"close",clnt_sperror(_clnt,""));
          nc_server_client_destroy(_clnt);
          _clnt = 0;
          throw e;
        }
        nc_server_client_destroy(_clnt);
        _clnt = 0;
        ILOG(("closed: ") << getName());
    }
}

void NetcdfRPCChannel::fromDOMElement(const xercesc::DOMElement* node)
{
    XDOMElement xnode(node);
    if(node->hasAttributes()) {
        // get all the attributes of the node
        xercesc::DOMNamedNodeMap *pAttributes = node->getAttributes();
        int nSize = pAttributes->getLength();
        for(int i=0;i<nSize;++i) {
            XDOMAttr attr((xercesc::DOMAttr*) pAttributes->item(i));
            // get attribute name
            const std::string& aname = attr.getName();
            const std::string& aval = attr.getValue();

            string sval;
            if (getDSMConfig()) sval = getDSMConfig()->expandString(aval);
            else sval = Project::getInstance()->expandString(aval);

            if (aname == "server") setServer(n_u::Process::expandEnvVars(aval));
            else if (aname == "dir") setDirectory(aval);
            else if (aname == "file") setFileNameFormat(aval);
            else if (aname == "cdl") setCDLFileName(sval);
            else if (aname == "interval") {
                istringstream ist(sval);
                int val;
                ist >> val;
                if (ist.fail())
                    throw n_u::InvalidParameterException(getName(),
                        sval, sval);
                setTimeInterval(val);
            }
            else if (aname == "length") {
                istringstream ist(sval);
                int val;
                ist >> val;
                if (ist.fail())
                    throw n_u::InvalidParameterException(getName(),
                        sval, sval);
                setFileLength(val);
            }
            else if (aname == "floatFill") {
                istringstream ist(sval);
                float val;
                ist >> val;
                if (ist.fail())
                    throw n_u::InvalidParameterException(getName(),
                        aname, sval);
                setFillValue(val);
            }
            else if (aname == "timeout") {
                istringstream ist(sval);
                int val;
                ist >> val;
                if (ist.fail())
                    throw n_u::InvalidParameterException(getName(),
                        sval, sval);
                setRPCTimeout(val);
            }
            else if (aname == "batchPeriod") {
                istringstream ist(sval);
                int val;
                ist >> val;
                if (ist.fail())
                    throw n_u::InvalidParameterException(getName(),
                        sval, sval);
                setRPCBatchPeriod(val);
            }
            else throw n_u::InvalidParameterException(getName(),
                        "unrecognized attribute", aname);
        }
    }
}

NcVarGroupFloat::NcVarGroupFloat(
        const std::vector<ParameterT<int> >& dims,
        const SampleTag* stag,float fill):
        _dimensions(dims),
        _sampleTag(*stag),_rec(),
        _weightsIndex(-1),
        _fillValue(fill),
        _interval(stag->getPeriod())
{
    _rec.start.start_val = 0;
    _rec.count.count_val = 0;
    _rec.cnts.cnts_val = 0;
    _rec.data.data_val = 0;
}

NcVarGroupFloat::~NcVarGroupFloat()
{
    delete [] _rec.start.start_val;
    delete [] _rec.count.count_val;
    delete [] _rec.cnts.cnts_val;
    delete [] _rec.data.data_val;
}

void NcVarGroupFloat::connect(NetcdfRPCChannel* conn, float _fillValue)
{
    CStringCache strings;
    datadef ddef; 
    ddef.connectionId = conn->getConnectionId(); 
    ddef.rectype = NS_TIMESERIES;
    ddef.datatype = NS_FLOAT;
    ddef.fillmissingrecords = 1;
    ddef.floatFill = _fillValue;
    ddef.intFill = 0;
    ddef.interval = _sampleTag.getPeriod();

    int ndims = _dimensions.size();
    ddef.dimensions.dimensions_val = 0;
    ddef.dimensions.dimensions_len = ndims;

    if (ndims > 0) {
        ddef.dimensions.dimensions_val = new dimension[ndims];
        for(int i = 0; i < ndims; i++) {
            ddef.dimensions.dimensions_val[i].name =
                strings.cache(_dimensions[i].getName());
            ddef.dimensions.dimensions_val[i].size =
                _dimensions[i].getValue(0);
        }
    }
    _weightsIndex = -1;
    string weightsName;
    VariableIterator vi = _sampleTag.getVariableIterator();
    for (int i = 0; vi.hasNext(); i++) {
        const Variable* var = vi.next();
        if (var->getType() == Variable::WEIGHT) {
            _weightsIndex = i;
            weightsName = var->getName();
            string::size_type n;
            while ((n = weightsName.find('.')) != string::npos)
                    weightsName[n] = '_';
        }
    }

    int nvars = _sampleTag.getVariables().size();
    if (_weightsIndex >= 0) {
        if (_weightsIndex != nvars - 1)
            throw n_u::IOException(conn->getName(),"connect",
                "weights variable should be last");
        nvars--;
    }

    ddef.variables.variables_val = new variable[nvars];
    ddef.variables.variables_len = nvars;

    vi = _sampleTag.getVariableIterator();
    for (int i = 0; vi.hasNext(); ) {
        const Variable* var = vi.next();
        struct variable& dvar = ddef.variables.variables_val[i];

        if (var->getType() == Variable::WEIGHT) continue;
        if (ndims > 0) 
            dvar.name = strings.cache(var->getNameWithoutSite());
        else
            dvar.name = strings.cache(var->getName());

        if (var->getConverter())
            dvar.units = strings.cache(var->getConverter()->getUnits());
        else
            dvar.units = strings.cache(var->getUnits());

        int nattrs = 0;
        if (_weightsIndex >= 0) nattrs++;
        if (var->getLongName().length() > 0) nattrs++;

        // Find this original Variable in the Project to get to any attributes
        // which were added to it during processing.
        VariableIterator vit(Project::getInstance());
        const Variable* origin_var{ nullptr };
        while (!origin_var && vit.hasNext())
        {
            const Variable* vp = vit.next();
            if (vp->getName() == var->getName())
                origin_var = vp;
        }

        std::vector<Parameter> attributes;
        if (origin_var)
        {
            attributes = origin_var->getAttributes();
            nattrs += attributes.size();
        }
        dvar.attrs.attrs_len = nattrs;
        dvar.attrs.attrs_val = 0;

        if (nattrs > 0) {
            dvar.attrs.attrs_val = new str_attr[nattrs];

            int iattr = 0;
            if (_weightsIndex >= 0) {
                str_attr *s = dvar.attrs.attrs_val + iattr++;
                s->name = strings.cache("counts");
                s->value = strings.cache(weightsName);
            }
            if (var->getLongName().length() > 0) {
                str_attr *s = dvar.attrs.attrs_val + iattr++;
                s->name = strings.cache("long_name");
                s->value = strings.cache(var->getLongName());
            }
            // The current RPC is limited to string attributes for variables,
            // so send the type as a special prefix in the string value,
            // float: or int:.  For bool, keep to the precedent established by
            // the global attributes and use int values 0 and 1.
            for (auto& p: attributes)
            {
                string type_prefix;
                if (p.getType() == Parameter::FLOAT_PARAM)
                    type_prefix = "float:";
                else if (p.getType() == Parameter::INT_PARAM)
                    type_prefix = "int:";
                else if (p.getType() == Parameter::BOOL_PARAM)
                    type_prefix = "int:";
                str_attr *s = dvar.attrs.attrs_val + iattr++;
                s->name = strings.cache(p.getName());
                s->value = strings.cache(type_prefix + p.getStringValue());
                DLOG(("adding attribute to var ") << var->getName()
                      << ": " << s->name << "=" << s->value);
            }
        }
        i++;
    }

    CLIENT *clnt = conn->getRPCClient();
    enum clnt_stat clnt_stat;
    int ntry;
    int result = 0;
    for (ntry = 0; ntry < 5; ntry++) {
        clnt_stat = clnt_call(clnt, DEFINE_DATAREC,
              (xdrproc_t) xdr_datadef, (caddr_t) &ddef,
              (xdrproc_t) xdr_int, (caddr_t) &result,
              conn->getRPCOtherTimeoutVal());
        if (clnt_stat == RPC_SUCCESS) break;
        WLOG(("nc_server DEFINE_DATAREC failed: %s, timeout=%d secs, ntry=%d",
              clnt_sperrno(clnt_stat), conn->getRPCTimeout(), ntry+1));
        if (clnt_stat != RPC_TIMEDOUT && clnt_stat != RPC_CANTRECV) break;
    }
    if (ntry > 0 && clnt_stat == RPC_SUCCESS)
        WLOG(("nc_server OK"));

    for (int i=0; i < nvars; i++) {
        struct variable& dvar = ddef.variables.variables_val[i];
        delete [] dvar.attrs.attrs_val;
    }

    delete [] ddef.variables.variables_val;
    delete [] ddef.dimensions.dimensions_val;

    if (clnt_stat != RPC_SUCCESS)
        throw n_u::IOException(conn->getName(),"define data rec",
            clnt_sperrno(clnt_stat));

    // If return is < 0, fetch the error string, and throw exception
    if (result < 0) {
        conn->checkError();
        // checkError should throw an exception if the above call returned a
        // negative result.  If not something's not working right.
        throw n_u::IOException(conn->getName(),"define data rec","unknown error");
    }

    // initialize data record
    _rec.datarecId = result;
    _rec.connectionId = conn->getConnectionId();
   
    _rec.start.start_len = ndims;
    _rec.count.count_len = ndims;
    _rec.start.start_val = new int[ndims];
    _rec.count.count_val = new int[ndims];

    _rec.cnts.cnts_val = 0;
    _rec.cnts.cnts_len = 0;
    if (_weightsIndex >= 0) {
        _rec.cnts.cnts_val = new int[1];
        _rec.cnts.cnts_len = 1;
    }

    _rec.data.data_val = new float[nvars];
    _rec.data.data_len = nvars;
}

void NcVarGroupFloat::write(NetcdfRPCChannel* conn,const Sample* samp,
                            int stationIndex)
{
    const SampleT<float>* fsamp = static_cast<const SampleT<float>*>(samp);
   
    /* constant members of rec have been initialized */

    /* time in seconds since 1970 Jan 1 00:00 GMT */
    _rec.time = (double)fsamp->getTimeTag() / USECS_PER_SEC;
    {
        static LogContext lp(LOG_VERBOSE);
        if (lp.active())
        {
            LogMessage lmsg(&lp);
            n_u::UTime ut(fsamp->getTimeTag());
            lmsg << ut.format(true,"%Y %m %d %H:%M:%S.%6f ")
                 << stationIndex << ' ';
            for (unsigned int i = 0; i < fsamp->getDataLength(); i++)
                lmsg << fsamp->getConstDataPtr()[i] << ' ';
        }
    }

    if (stationIndex >= 0) {
        _rec.start.start_val[0] = stationIndex;
        _rec.count.count_val[0] = 1;
    }
    else {
        assert(_rec.start.start_len == 0);
        assert(_rec.count.count_len == 0);
    }

    size_t dlen = fsamp->getDataLength();

    if (_weightsIndex >= 0) {
        if ((signed)fsamp->getDataLength() > _weightsIndex)
            _rec.cnts.cnts_val[0] =
                (int)rint(fsamp->getConstDataPtr()[_weightsIndex]);
        else _rec.cnts.cnts_val[0] = 0;
        _rec.cnts.cnts_len = 1;
        dlen--;
    }

    const float* fdata = fsamp->getConstDataPtr();
    for (unsigned int i = 0; i < _rec.data.data_len; i++) {
        if (i >= dlen || isnan(fdata[i]))
                _rec.data.data_val[i] = _fillValue;
        else _rec.data.data_val[i] = fdata[i];
    }

    conn->write(&_rec);
}

/**
 * Basic read is not implemented. Always throws IOException.
 */
size_t NetcdfRPCChannel::read(void*, size_t)
{
    throw n_u::IOException(getName(), "read","not supported");
}

/**
 * Basic write is not implemented. Always throws IOException.
*/
size_t NetcdfRPCChannel::write(const void*, size_t)
{
    throw n_u::IOException(getName(), "default write","not supported");
}

/**
 * Basic write is not implemented. Always throws IOException.
 */
size_t NetcdfRPCChannel::write(const struct iovec*, int)
{
    throw n_u::IOException(getName(), "default write","not supported");
}

void NetcdfRPCChannel::setCDLFileName(const std::string& val)
{
    _cdlFileName = val;
    DLOG(("") << getName() << ": cdl name set: " << _cdlFileName);
}
