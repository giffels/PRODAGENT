#!/usr/bin/env python
"""
_Config_

Get Configuration for ProdAgentDB from the ProdAgent config file.
This should be available through PRODAGENT_CONFIG env var

If the configuration is not available, the default config settings
here will be used.

"""
import os
import logging

from ProdAgentCore.Configuration import loadProdAgentConfiguration

def loadConf():
    try:
        cfg = loadProdAgentConfiguration()
        dbConfig = cfg.getConfig("ProdAgentDB")
        defaultConfig.update(dbConfig)
    except StandardError, ex:
        msg = "ProdAgentDB.Config:"
        msg += "Unable to load ProdAgent Config for ProdAgentDB\n"
        msg += "%s\n" % ex
        logging.warning(msg)

    


defaultConfig={'dbName':'ProdAgentDB',
               'host':'localhost',
               'user':'Proddie',
               'passwd':'ProddiePass',
               'socketFileLocation':'/opt/openpkg/var/mysql/mysql.sock',
               'dbPortNr':''
              }


loadConf()

        
