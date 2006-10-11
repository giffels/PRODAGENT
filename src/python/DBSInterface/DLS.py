#!/usr/bin/env python

"""
_DLS_

Class to extract/insert info from DLS
based on DLS API
 
"""
import os,string,exceptions

from ProdAgentCore.Configuration import loadProdAgentConfiguration        
import dlsClient
from dlsDataObjects import *

import logging

# ##############
class addDLSError(exceptions.Exception):
  def __init__(self,errmsg):
   args= errmsg
   exceptions.Exception.__init__(self, args)
   pass
                                                                                
                                                                                
  def getClassName(self):
   """ Return class name. """
   return "%s" % (self.__class__.__name__)
                                                                                
                                                                                
  def getErrorMessage(self):
   """ Return exception error. """
   return "%s" % (self.args)
                                                                                


# ##############
class DLS:
  """
  _DLS_

  interface to extract/insert info from DLS
  """
# ##############
  def __init__(self, type, endpoint):
    """
     Construct api object.
    """
## get configuration here instead of doing it at DBSComponent startup
#    try:
#      config = loadProdAgentConfiguration()
#      compCfg = config.getConfig("DLSInterface")
#    except StandardError, ex:
#      msg = "Error reading configuration:\n"
#      msg += str(ex)
#      raise RuntimeError, msg
#
#    type=compCfg['DLSType']
#    endpoint=compCfg['DLSAddress']
#
    try:
        self.dlsapi = dlsClient.getDlsApi(dls_type=type,dls_endpoint=endpoint)
    except dlsApi.DlsApiError, inst:
        msg = "Error when binding the DLS interface: " + str(inst)
        raise RuntimeError, msg

# #########################
  def getFileBlockLocation(self,fileblock):
        """
         query DLS to get fileblock location
        """
        entryList=[]
        try:
         entryList=self.dlsapi.getLocations(fileblock)
        except dlsApi.DlsApiError, inst:
          msg = "Error in the DLS query: %s." % str(inst)
          if "DLS Server don't respond" in msg:
            raise RuntimeError, msg

        SEList=[]
        for entry in entryList:
         for loc in entry.locations:
          SEList.append(str(loc.host))

        return SEList

# ##############
  def addEntryinDLS(self,fileblock,SE):
        """
          add fileblock associated to SE in DLS
        """
        fblock=DlsFileBlock(fileblock)
        location=DlsLocation(SE)
        entry=DlsEntry(fblock,[location])
        try:
          self.dlsapi.add([entry],errorTolerant=False)
          #self.dlsapi.add([entry])
        except dlsApi.DlsApiError, inst:
          msg = "Error adding a DLS entry: %s." % str(inst)
          logging.error(msg)
          raise addDLSError(msg)

        return

