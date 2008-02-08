#!/usr/bin/env python
"""
_Config_

Get Configuration from the ProdAgent config file by providing the block name provided in config block
in ProdAgent config file.
This should be available through PRODAGENT_CONFIG env var

If the configuration is not available, the default config settings
here will be used.

"""
import os
import logging
from ProdAgentCore.Configuration import loadProdAgentConfiguration
first_access = True


def loadConfig(db = "ProdAgentDB"):
    """
    loadConfig method that loads the configuration of database base from ProdAgentConfig.xml and return it asdictionary 
    object

    Argument:
            db : database configuration block to load

    Return:
            Dictionary containing the required configuration  

    """
    try:
        global first_access
        
        cfg = loadProdAgentConfiguration()
        dbConfig = cfg.getConfig(db)
        if (first_access == True):
          defaultConfig.update(dbConfig)
                  
        first_access = False
        return dbConfig 
    except StandardError, ex:
        msg = "%s.Config:" % db
        msg += "Unable to load ProdAgent Config for " + db
        msg += "%s\n" % ex
        logging.warning(msg)

    


defaultConfig={'dbName':'ProdAgentDB',
               'host':'localhost',
               'user':'Proddie',
               'passwd':'ProddiePass',
               'socketFileLocation':'/opt/openpkg/var/mysql/mysql.sock',
               'portNr':'',
               'refreshPeriod' : 4*3600 ,
               'maxConnectionAttempts' : 5,
               'dbWaitingTime' : 10 
              }

#// called at startup to fill the defaultConfig dictionary with ProdAgentDB config :: BACKWARD COMPATIBILITY
loadConfig()
       

