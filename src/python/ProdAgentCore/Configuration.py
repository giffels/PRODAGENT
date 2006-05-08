#!/usr/bin/env python
"""
_Configuration_

Objects to manage/save/load/access a single configuration setup
for an entire ProdMgrLite system including components

To retrieve the ProdMgr Configuration for a component do:

from ProdMgrCore.Configuration import loadProdMgrConfiguration
cfg = loadProdMgrConfiguration()
componentSettings = cfg.getConfig("componentNameHere")

Note that this method requires that the env var PRODMGR_CONFIG is
set to point to the config file to be loaded.

You can also access configuration for core prodmgr services like
ProdMgrDB, MessageService and JobStates, for example
dbSettings = cfg.getConfig("ProdMgrDB")

The config objects are dictionaries and parameters can be accessed in the
usual manner.

parameterValue = componentSettings["SomeParameter"]

Note that all values are saved as strings, so if you require any type
conversion you have to do that yourself. Eg:

intParamValue = int( componentSettings["SomeInteger"] )


"""
__revision__ = "$Id: Configuration.py,v 1.4 2006/04/13 19:26:49 evansde Exp $"
__version__ = "$Revision: 1.4 $"
__author__ = "evansde@fnal.gov"


import os
import socket

from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvDoc import IMProvDoc
from IMProv.IMProvQuery import IMProvQuery
from IMProv.IMProvLoader import loadIMProvFile


def loadProdMgrConfiguration():
    """
    _loadProdMgrConfiguration_

    Util method to load the ProdMgrConfiguration from a location
    defined by the env var PRODMGR_CONFIG

    Returns a ProdMgrConfiguration object
    
    """
    envVar = os.environ.get("PRODMGR_CONFIG", None)
    if envVar == None:
        msg = "Cannot load ProdMgr Configuration:\n"
        msg += "PRODMGR_CONFIG is not set:\n"
        raise RuntimeError, msg
    if not os.path.exists(envVar):
        msg = "File Not Found:\n"
        msg += "%s\n" % envVar
        msg += "PRODMGR_CONFIG must point to a valid file\n"
        raise RuntimeError, msg

    config = ProdMgrConfiguration()
    config.loadFromFile(envVar)
    return config

    

class ProdMgrConfiguration(dict):
    """
    _ProdMgrConfiguration_

    Configuation container for the ProdMgr

    Essentially a map of named ConfigBlock instances.

 

    The Core ConfigBlocks are added by default, Component ConfigBlocks
    are added with the newComponentConfig method.
    No Components are added by default.

  

    """
    def __init__(self):
        dict.__init__(self)
        self.components = []
        #  //
        # // Core non-component pieces are included by default
        #//
        self.setdefault("ProdMgr", ConfigBlock("ProdMgr"))
        self.setdefault("ProdMgrDB", ConfigBlock("ProdMgrDB"))
        
    def save(self):
        """
        _save_

        Generate an IMProvNode object to save this object to XML

        """
        result = IMProvNode("ProdMgrConfiguration")
        for item in self.components:
            result.addNode(IMProvNode("Component", None,
                                      Name = item))
        for item in self.values():
            result.addNode(item.save())
            
        return result

    def load(self, improvNode):
        """
        _load_

        Populate self based on content of improvNode instance
        """
        componentQ = IMProvQuery(
            "ProdMgrConfiguration/Component[attribute(\"Name\")]"
            )
        configQ = IMProvQuery(
            "ProdMgrConfiguration/ConfigBlock"
            )
        components = componentQ(improvNode)
        configs = configQ(improvNode)
        for comp in components:
            self.components.append(comp)
        for config in configs:
            cfgBlock = ConfigBlock("temp")
            cfgBlock.load(config)
            self[cfgBlock.name] = cfgBlock
        
        return

    def loadFromFile(self, filename):
        """
        _loadFromFile_

        Read the file provided to extract the configuration

        """
        try:
            improv = loadIMProvFile(filename)
        except StandardError, ex:
            msg = "Cannot read file: %s\n" % filename
            msg += "Failed to load ProdMgrConfiguration\n"
            raise RuntimeError, msg

        self.load(improv)
        return

    def saveToFile(self, filename):
        """
        _saveToFile_

        Save this instance to the file provided

        """
        doc = IMProvDoc("ProdMgrConfig")
        doc.addNode(self.save())
        handle = open(filename, 'w')
        handle.write(doc.makeDOMDocument().toprettyxml())
        handle.close()
        return
        

    def configNames(self):
        """
        _configNames_

        get list of all ConfigBlock names in this instance

        """
        return self.keys()
        
    def newComponentConfig(self, configName):
        """
        _newComponentConfig_
        
        Get a new ConfigBlock instance for a component of the prodAgent.
        configName must not exist yet.
        
        """
        if configName in self.configNames():
            msg = "Duplicate Config Name added to ProdMgrConfiguration\n"
            msg += "%s already exists\n" % configName
            raise RuntimeError, msg

        newCfg = ConfigBlock(configName)
        self[configName] = newCfg
        self.components.append(configName)
        return newCfg
    

    
    def getConfig(self, name):
        """
        _getConfig_

        Get the ConfigBlock for the name provided
        Returns None if not found
        """
        return self.get(name, None)
    
    def listComponents(self):
        """
        _listComponents_

        Get list of all components in this Config
        Does not include core non component config blocks such as
        ProdMgrDB, JobStates and MessageService blocks
        
        """
        return self.components
        
    

    def __str__(self):
        """string rep of config"""
        return str(self.save())


    
    
        
class ConfigBlock(dict):
    """
    _ConfigBlock_

    Dict based named configuration block to contain a group
    of parameters for a configuration object

    A single instance should correspond to a single Component.
    If key/value pairs is insufficient to configure a component
    (Eg it needs a list etc) then please let me know and we can add
    the appropriate functionality. For now, just keeping it simple...
    
    """
    def __init__(self, name):
        self.name = name
        self.comment = None
        dict.__init__(self)

    def save(self):
        """
        _save_

        Generate an IMProvNode containing the configuration for this
        Component
        
        """
        result = IMProvNode("ConfigBlock", None,
                            Name = self.name)
        if self.comment != None:
            result.addNode(IMProvNode("Comment", self.comment))
        for key, val in self.items():
            result.addNode(IMProvNode("Parameter", None,
                                      Name = str(key), Value = str(val))
                           )
        return result
    
    def load(self, improvNode):
        """
        _load_

        Populate self based on content of improvNode instance
        """
        nameQ = IMProvQuery("/ConfigBlock[attribute(\"Name\")]")
        paramQ = IMProvQuery("/ConfigBlock/Parameter")
        commentQ = IMProvQuery("/ConfigBlock/Comment[text()]")
        
        self.name = str(nameQ(improvNode)[0])
        comments = commentQ(improvNode)
        if len(comments) > 0:
            self.comment = str(comments[0])
        else:
            self.comment = None
        
        params = paramQ(improvNode)
        for paramNode in params:
            paramName = str(paramNode.attrs['Name'])
            paramVal = str(paramNode.attrs['Value'])
            self[paramName] = paramVal
            
        return

        
