#!/usr/bin/env python
"""
_PluginConfiguration_

Objects to manage/save/load/access a Plugin Configuration.

Uses the same block structure as the ProdAgent configuration

"""
__revision__ = "$Id: PluginConfiguration.py,v 1.1 2006/05/18 22:03:47 evansde Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "evansde@fnal.gov"


import os

from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvDoc import IMProvDoc
from IMProv.IMProvQuery import IMProvQuery
from IMProv.IMProvLoader import loadIMProvFile


from ProdAgentCore.Configuration import ConfigBlock
from ProdAgentCore.Configuration import loadProdAgentConfiguration

def loadPluginConfig(component, plugin):
    """
    _loadPluginConfig_

    Get the PluginConfiguration object for the component plugin
    using the name specified.

    The Component will be looked up from the main ProdAgentConfig using
    the PRODAGENT_CONFIG env var.

    This Config will be searched for the plugin file using the convention:
    <pluginName>PluginConfig

    This is assumed to be a file which is a saved PluginConfiguration
    object

    """
    prodAgentConfig = loadProdAgentConfiguration()
    compConfig = prodAgentConfig.getConfig(component)
    keyname = "%sPluginConfig" % plugin
    filename = compConfig.get(keyname, None)
    if filename == None:
        msg = "Cannot find entry for Plugin: %s\n" % plugin
        msg += "In Config for component: %s\n" % component
        msg += "Expected parameter: %s\n " % keyname
        raise RuntimeError, msg
    filename = os.path.expandvars(filename)
    if not os.path.exists(filename):
        msg = "Plugin Configuration file Not Found:\n"
        msg += "%s\n" % filename
        msg += "For component %s plugin %s\n" % (component, plugin)
        raise RuntimeError, msg

    plugCfg = PluginConfiguration()
    try:
        plugCfg.loadFromFile(filename)
    except StandardError, ex:
        msg = "Error loading Plugin Configuration for:\n"
        msg += "Component %s Plugin %s\n" % (component, plugin)
        msg += "From File:\n%s\n" % filename
        msg += str(ex)
        raise RuntimeError, msg
    return plugCfg

    


class PluginConfiguration(dict):
    """
    _PluginConfiguration_

    Plugin Configuration object that can be saved/loaded to a file and
    contains a set of named ConfigBlocks

    """
    def __init__(self):
        dict.__init__(self)


    def newBlock(self, blockName):
        """
        _newBlock_

        Create a new ConfigBlock with the name provided and
        return it to be populated

        """
        newBlock = ConfigBlock(blockName)
        self[blockName] = newBlock
        return newBlock
    

    def save(self):
        """
        _save_

        Serialise self into IMProvNode structure

        """
        result = IMProvNode(self.__class__.__name__)
        for value in self.values():
            result.addNode(value.save())
        return result
    
        

    def load(self, improvNode):
        """
        _load_

        Unserialize from IMProvNode into self

        """
        configQ = IMProvQuery(
            "%s/ConfigBlock" % self.__class__.__name__
            )
        configs = configQ(improvNode)
        for config in configs:
            cfgBlock = ConfigBlock("temp")
            cfgBlock.load(config)
            self[cfgBlock.name] = cfgBlock
        return
    


    def writeToFile(self, filename):
        """
        _writeToFile_

        Write data in this instance to file specified as XML

        """
        doc = IMProvDoc("ProdAgentPluginConfig")
        doc.addNode(self.save())
        handle = open(filename, 'w')
        handle.write(doc.makeDOMDocument().toprettyxml())
        handle.close()
        return

    def loadFromFile(self, filename):
        """
        _loadFromFile_

        Read data from File and populate self

        """
        try:
            improv = loadIMProvFile(filename)
        except StandardError, ex:
            msg = "Cannot read file: %s\n" % filename
            msg += "Failed to load ProdAgent PluginConfiguration\n"
            raise RuntimeError, msg

        self.load(improv)
        return


    
