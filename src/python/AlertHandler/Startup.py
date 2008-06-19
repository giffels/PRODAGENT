#!/usr/bin/env python
"""
_StartComponent_

This script starts the AlertHandler Component by reading it's configuration from the common configuration file, ProdAgentConfig.xml
"""

import os
from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.CreateDaemon import createDaemon
from ProdAgentCore.PostMortem import runWithPostMortem
from AlertHandler.AlertHandlerComponent import AlertHandlerComponent

  
def StartComponent():
    """
    _StartComponent_
    Function that Starts this Component
    """
  
    try:
    
        config = loadProdAgentConfiguration()
        componentConfig = config.getConfig('AlertHandler')
    
    except Exception, ex:
   
        msg = 'Failed to load ProdAgent Configuration'
        msg +='\nDetails: %s' % str(ex)

        raise RuntimeError, msg   
     

    componentConfig['ComponentDir'] = os.path.expandvars (componentConfig['ComponentDir'])
     
    #// Create Daemon and run the component
  
    createDaemon (componentConfig['ComponentDir'])
    component = AlertHandlerComponent(**dict(componentConfig))   
    runWithPostMortem (component, componentConfig['ComponentDir'])

    return


if __name__ == '__main__':

   StartComponent() 



