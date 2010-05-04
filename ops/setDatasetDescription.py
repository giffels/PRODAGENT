#!/usr/bin/env python
"""
_setDatasetDescription_

Given the dataset path, the Description string (up to 1000 characters) and a
DBS Instance url (writer), it will update the processed dataset description

Usage:

python setDatasetDescription.py PATH 'DESCRIPTION' --url <DBSURL>

    PATH: Full dataset path
    DESCRIPTION: Up to 1000 characters. Use single (') quotes as boundaries.
    DBSURL: Writer URL

Example:

    python setDatasetDescription.py \
    /RelValTTbar_Tauola/CMSSW_3_5_8-START3X_V26_E7TeV_AVE_2_BX156-v1/GEN-SIM-DIGI-RAW-HLTDEBUG \
    '{"MixingModule": {"input": "/RelValMinBias/CMSSW_3_5_8-MC_3XY_V26-v1/GEN-SIM-DIGI-RAW-HLTDEBUG"}}' \
    --url https://cmsdbsprod.cern.ch:8443/cms_dbs_prod_global_writer/servlet/DBSServlet


"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: setDatasetStatus.py,v 1.1 2009/07/14 13:56:03 direyes Exp $"

import sys
from DBSAPI.dbsApi import DbsApi
from DBSAPI.dbsException import *
from DBSAPI.dbsApiException import *
from DBSAPI.dbsOptions import DbsOptionParser

optManager  = DbsOptionParser()
(opts,args) = optManager.getOpt()
api = DbsApi(opts.__dict__)

try:
    if len(sys.argv) < 3 or opts.__dict__.get('url', None) is None:
        print "%s" % __doc__ 
        sys.exit(1)
    if sys.argv[2].find("'"):
        print "%s" % __doc__  
        sys.exit(1)
    path = sys.argv[1];
    newDescription = sys.argv[2]
    api.updateProcDSDesc(path, newDescription)

except DbsApiException, ex:
    print "Caught API Exception %s: %s "  % (ex.getClassName(), ex.getErrorMessage() )
    if ex.getErrorCode() not in (None, ""):
        print "DBS Exception Error Code: ", ex.getErrorCode()

print "Done"

