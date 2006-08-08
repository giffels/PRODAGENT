#!/usr/bin/env python
"""
_DLSUtils_

DLS Interface tools for PhEDEx Interface component

"""

from ProdAgentCore.Configuration import loadProdAgentConfiguration

import logging
import dlsClient
from dlsDataObjects import *

import dbsCgiApi
from dbsException import DbsException



_DBSConfig = {}
_DLSConfig = {}


def loadDBSDLS():
    """
    _loadDLSConfig_

    Extract the DLS contact information from the prod agent config

    """
    try:
        config = loadProdAgentConfiguration()
    except StandardError, ex:
        msg = "Error reading configuration:\n"
        msg += str(ex)
        logging.error(msg)
        raise RuntimeError, msg

    try:
        dlsConfig = config.getConfig("DLSInterface")
    except StandardError, ex:
        msg = "Error reading configuration for DLSInterface:\n"
        msg += str(ex)
        logging.error(msg)
        raise RuntimeError, msg
    _DLSConfig = dlsConfig
    logging.debug("DLS Config: %s" % _DLSConfig)
    try:
        dbsConfig = config.getConfig("LocalDBS")
    except StandardError, ex:
        msg = "Error reading configuration for LocalDBS:\n"
        msg += str(ex)
        logging.error(msg)
        raise RuntimeError, msg
    _DBSConfig = dbsConfig
    logging.debug("DBS Config: %s" % _DBSConfig)
    try:
        dlsapi = dlsClient.getDlsApi(dls_type = dlsConfig['DLSType'],
                                     dls_endpoint = dlsConfig['DLSAddress'])
    except dlsApi.DlsApiError, inst:
        msg = "Error when binding the DLS interface: " + str(inst)
        logging.error(msg)
        raise RuntimeError, msg
    
    try:
        dbsApi = dbsCgiApi.DbsCgiApi(dbsConfig['DBSURL'],
                                 { 'instance': dbsConfig['DBSAddress']})
    except StandardError, ex:
        msg = "Error when binding the DBS interface"
        logging.error(msg)
        raise RuntimeError, msg
    

    return dlsapi, dbsApi


class DBSDLSToolkit:
    """
    _DBSDLSToolkit_

    Tools for extracting data from DBS and DLS, using a static DLS api and
    DBS api  instances embedded in the class

    """
    _DLS, _DBS = loadDBSDLS()
    _DBSConf = _DBSConfig
    _DLSConf = _DLSConfig
    def __init__(self):
        pass

    def dbsName(self):
        """
        _dbsName_

        return the DBS Instance Name

        """
        return self._DBSConf.get("DBSAddress", None)

    def listFileBlocksForDataset(self, dataset):
        """
        _listFileBlocksForDataset_

        return a list of file block names for a datase

        """
        try:
            fileBlocks = self._DBS.getDatasetFileBlocks(dataset)
        except DbsException, ex:
            msg = "DbsException for DBS API getDatasetFileBlocks:\n"
            msg += "  Dataset = %s\n" % dataset
            msg += "  Exception Class: %s\n" % ex.getClassName()
            msg += "  Exception Message: %s\n" % ex.getErrorMessage()
            logging.error(msg)
            raise RuntimeError, msg

        
        return fileBlocks

    def getFileBlockLocation(self, fileBlockName):
        """
        _getFileBlockLocation_

        Get a list of fileblock locations

        """
        try:
            locations = self._DLS.getLocations(fileBlockName)
        except dlsApi.DlsApiError, ex:
            msg = "Error in the DLS query: %s\n" % str(ex)
            msg += "When trying to get locations for file Block:\n"
            msg += "%s\n" % fileBlockName
            logging.error(msg)
            return []
        result = []
        for loc in locations:
            for locInst in  loc.locations:
                host = locInst.host
                if host not in result:
                    result.append(host)
        return result
    




    
