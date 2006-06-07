#!/usr/bin/env python
"""
_SiteLocalConfig_

Utility for reading a site local config XML file and converting it
into an object with an API for getting info from it

"""

import os

from IMProv.IMProvLoader import loadIMProvFile
from IMProv.IMProvQuery import IMProvQuery

from FwkJobRep.TrivialFileCatalog import tfcFilename, readTFC

class SiteConfigError(StandardError):
    """
    Exception class placeholder
    """
    pass


def loadSiteLocalConfig():
    """
    _loadSiteLocalConfig_

    Runtime Accessor for the site local config.

    Requires that CMS_PATH is defined as an environment variable

    """
    defaultPath = "$CMS_PATH/SITECONF/local/JobConfig/site-local-config.xml"
    actualPath = os.path.expandvars(defaultPath)
    if os.environ.get("CMS_PATH", None) == None:
        msg = "Unable to find site local config file:\n"
        msg += "CMS_PATH variable is not defined."
        raise SiteConfigError, msg
    
    if not os.path.exists(actualPath):
        msg = "Unable to find site local config file:\n"
        msg += actualPath
        raise SiteConfigError, msg

    config = SiteLocalConfig(actualPath)
    return config
    

class SiteLocalConfig:
    """
    _SiteLocalConfig_

    Readonly API object for getting info out of the SiteLocalConfig file

    """
    def __init__(self, siteConfigXML):
        self.siteConfigFile = siteConfigXML
        self.siteName = None
        self.eventData = {}
        self.calibData = {}
        self.read()


    def trivialFileCatalog(self):
        """
        _trivialFileCatalog_

        Return an instance of FwkJobRep.TrivialFileCatalog
        if there is a catalog specified in eventData

        """
        tfcUrl = self.eventData.get('catalog', None)
        if tfcUrl == None:
            return None
        try:
            tfcFile = tfcFilename(tfcUrl)
            tfcInstance = readTFC(tfcFile)
        except StandardError, ex:
            msg = "Unable to load TrivialFileCatalog:\n"
            msg += "URL = %s\n" % tfcUrl
            raise SiteConfigError, msg
        return tfcInstance
            

    def read(self):
        """
        _read_

        Load data from SiteLocal Config file and populate this object

        """
        try:
            node = loadIMProvFile(self.siteConfigFile)
        except StandardError, ex:
            msg = "Unable to read SiteConfigFile: %s\n" % self.siteConfigFile
            msg += str(ex)
            raise SiteConfigError, msg

        #  //
        # // site name
        #//
        nameQ = IMProvQuery("/site-local-config/site")
        nameNodes = nameQ(node)
        if len(nameNodes) == 0:
            msg = "Unable to find site name in SiteConfigFile:\n"
            msg += self.siteConfigFile
            raise SiteConfigError, msg
        self.siteName = str(nameNodes[0].attrs.get("name"))

        #  //
        # // event data (Trivial Catalog location)
        #//
        
        catalogQ = IMProvQuery("/site-local-config/site/event-data/catalog")
        catNodes = catalogQ(node)
        if len(catNodes) == 0:
            msg = "Unable to find catalog entry for event data in:\n"
            msg += self.siteConfigFile
            raise SiteConfigError, msg

        self.eventData['catalog'] = str(catNodes[0].attrs.get("url"))
        

        #  //
        # // calib data
        #//
        
        calibQ = IMProvQuery("/site-local-config/site/calib-data/*")
        
        calibNodes = calibQ(node)
        if len(calibNodes) == 0:
            msg = "Unable to find calib data entry in:\n"
            msg += self.siteConfigFile
            raise SiteConfigError, msg
        for calibNode in calibNodes:
            self.calibData[str(calibNode.name)] = \
                      str(calibNode.attrs.get("url"))

        return




    
