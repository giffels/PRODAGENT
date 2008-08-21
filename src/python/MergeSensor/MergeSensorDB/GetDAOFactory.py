#!/usr/bin/env python

"""
Returns DAOFactory isntance for MergeSensorDB package 
"""

from ProdAgentCore.Configuration import ProdAgentConfiguration

from WMCore.Database.DBFactory import DBFactory
from WMCore.DAOFactory import DAOFactory
import logging
import os


def getDAOFactory ():
    """
    Initialize the DAOFactory and connection to the respective database.

    Return:
           DAOFactory instance
    """

    #  //
    # // Load ProdAgent Config block to get connection parameters 
    #//

    config = os.environ.get("PRODAGENT_CONFIG", None)
    if config == None:
        msg = "No ProdAgent Config file provided\n"
        msg += "Set $PRODAGENT_CONFIG variable\n"

    cfgObject = ProdAgentConfiguration()
    cfgObject.loadFromFile(config)
    dbConfig = cfgObject.get("ProdAgentDB")
    dbURL = None
    param = {}
   
    try:

       #// connection parameters
       dbURL = dbConfig['dbURL']

       #// customized parameters
       if dbConfig.has_key('socketFileLocation'): 
          param['unix_socket'] = dbConfig['socketFileLocation']

       #// otherwise use default socket location /tmp

    except Exception, ex:
         msg = "Parameter missing \n"
         msg += str(ex)
         raise RuntimeError, msg
 
 
    #// Get Logger
    logger = logging.getLogger('MergeSensorDB')
    logger.setLevel(logging.ERROR)

    #// Initializing dbFactory    
    dbFactory = DBFactory(logger, dbURL, options = param)

    daoFactory = DAOFactory(package = 'MergeSensor.MergeSensorDB',
                         logger = logger,
                         dbinterface = dbFactory.connect())

    return daoFactory  #//END getDAOFactory
