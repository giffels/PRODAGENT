#!/usr/bin/env python
"""
_ConfigCheck_

Util for testing a ProdAgentConfig has several required keys.
Call from prodAgentd when starting up

"""

import logging
import ProdAgent.ResourceControl.ResourceControlAPI as ResCon

def parameterExists(cfgName, cfgBlock, parameterName):
    """
    _parameterExists_

    Check that a parameter exists in the cfgBlock provided

    """
    if not cfgBlock.has_key(parameterName):
        msg = "Warning: Component %s has no parameter: %s\n" % (
            cfgName, parameterName)
        msg += "In its configuration.\n"
        msg += "Please check your ProdAgentConfig.xml file and add\n"
        msg += "<Parameter Name=\"%s\" Value=\"Value Here\"/>\n" % (
            parameterName,)
        msg += "To the ConfigBlock named %s\n" % cfgName
        logging.warning(msg)
    return



def configCheck(cfgObject):
    """
    _configCheck_

    Sanity check the cfgObject provided

    """
    for comp in cfgObject.listComponents():
        #  //
        # // Check all components have a ComponentDir
        #//
        compCfg = cfgObject.get(comp, {})
        parameterExists(comp, compCfg, "ComponentDir")
        

    #  //
    # // Check some core settings
    #//  
    prodAgent = cfgObject.get("ProdAgent", {})
    prodAgentName = prodAgent.get("ProdAgentName", None)
    if prodAgentName == None:
        msg = "ProdAgent Name is not provided\n"
        msg += "Please add a ProdAgentName parameter to the ProdAgent"
        msg += " ConfigBlock in your ProdAgentConfig file"
        logging.warning(msg)

    #  //
    # // Local and Global DBS
    #//
    localDBS = cfgObject.get("LocalDBS", {})
    localUrl = localDBS.get("DBSURL", None)
    if localUrl == None:
        msg = "Local DBS URL is not provided in ProdAgentConfig\n"
        msg += "Please add a DBSURL parameter in a ConfigBlock named LocalDBS"
        logging.warning(msg)
    
    globalDBS = cfgObject.get("GlobalDBSDLS", {})
    globalUrl = globalDBS.get("DBSURL", None)
    if globalUrl == None:
        msg = "Global DBS URL is not provided in ProdAgentConfig\n"
        msg += "Please add a DBSURL parameter in a ConfigBlock "
        msg += "named GlobalDBSDLS"
        logging.warning(msg)

        
    #  //
    # // Sites defined in ResConDB ??
    #//
    siteList = ResCon.allSiteData()
    if len(siteList) == 0:
        msg = "Warning: No sites are listed in the ResourceControlDB "
        msg += "for this ProdAgent\n"
        msg += "You may need to add some sites using the\n"
        msg += "PRODAGENT/util/resourceControl.py script\n"
        logging.warning(msg)
        
