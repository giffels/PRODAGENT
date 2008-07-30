#!/usr/bin/env python
"""
_ConfDBEmulator_

Emulates ConfDB.
"""

__revision__ = "$Id: $"
__version__ = "$Revision: $"

import random
import os
import cherrypy
import logging

from ProdAgentCore.Configuration import ProdAgentConfiguration

class ConfDBEmulator:
    """
    _ConfDBEmulator_

    Emulates ConfDB.
    """

    def __init__ (self):
        """
        _init_
            
        Read in configuration from the ConfDBEmulator section of the ProdAgent
        config file.  The ConfDBEmulator expects the following parameters to be
        defined in the config:
          RecoConfigs - A comma seperated list of config names.  For each config
            name another parameter must exist where its name is the config name
            and its value is the absolute path to the config.
        """ 
        config = os.environ.get("PRODAGENT_CONFIG", None)
        if config == None:
            cherrypy.log("PRODAGENT_CONFIG env variable is not set.",
                         context='', severity=logging.ERROR, traceback=False)            
            return

        cfgObject = ProdAgentConfiguration()
        cfgObject.loadFromFile(config)
        confDBEmulatorConfig = cfgObject.get("ConfDBEmulator")

        if confDBEmulatorConfig == None:
            cherrypy.log("PA config does not have a ConfDBEmulator block.",
                         context='', severity=logging.ERROR, traceback=False)            
            return

        self.recoConfigs = {}
        for recoConfigName in confDBEmulatorConfig["RecoConfigs"].split(","):
            self.recoConfigs[recoConfigName] = confDBEmulatorConfig[recoConfigName]

    def readConfig(self, configName):
        """
        _readConfig_

        Given the name of a config, read it off of disk and return the contents
        of the file.  If an error occurs while reading the config an empty
        string will be returned.
        """
        cherrypy.log("readConfig(): configName before %s" % configName,
                     context='', severity=logging.ERROR, traceback=False)
        
        configNameParts = configName.split("/")
        if configNameParts[-1][0] == "V":
            try:
                configNumber = int(configNameParts[-1][1:])
            except ValueError, ex:
                pass
            else:
                configName = ""
                for configNamePart in configNameParts[:-1]:
                    configName += configNamePart + "/"
                configName = configName[:-1]

        cherrypy.log("readConfig(): configName after %s" % configName,
                     context='', severity=logging.ERROR, traceback=False)
        
        if configName not in self.recoConfigs.keys():
            cherrypy.log("Don't know anything about config: %s" % configName,
                         context='', severity=logging.ERROR, traceback=False)
            return ""
        
        configPath = self.recoConfigs[configName]

        try:
            configFileHandle = open(configPath, 'r')
        except IOError, ex:
            cherrypy.log("Error opening file %s: %s" % (configPath, str(ex)),
                         context='', severity=logging.ERROR, traceback=False)
            return ""

        configFile = "# %s/V1 (CMSSW_2_0_10)\n" % configName
        configFile += configFileHandle.read()
        configFileHandle.close()

        return configFile

    def generateSettingsPage(self):
        """
        _generateSettingsPage_

        Generate the HTML for the settings page which is displayed any time
        the ConfDBEmulator is accessed without any parameters.  This page
        displays the emulator mode, all known configs as well as their path
        and the mapping of datasets to config names.
        """
        settingsPage = "<html><head>\n"
        settingsPage += "<title>ConfDB Emulator Settings</title>\n"
        settingsPage += "</head><body>\n"
        settingsPage += "ConfDB Emulator Settings<br><br>\n"

        settingsPage += "<dl><dt>Known Configs</dt>\n"
        for config in self.recoConfigs.keys():
            settingsPage += "<dd>%s -> %s</dd>\n" % \
                            (config, self.recoConfigs[config])
        settingsPage += "</dl>\n"
        
        settingsPage += "</body></html>\n"
        
        return settingsPage

    def index (self, *arg, **args):
        """
        _index_

        Method called by cherrypy when the ConfDB Emulator URL is accessed.
        This will return different pages depending on the parameters that are
        passed in with the URL:
          dbName, configName and format parameters - The config specified by the
            configName parameter will be returned.

          Anything else - The settings page will be displayed.
        """
        if "dbName" in args.keys() and "configName" in args.keys() and \
                 "format" in args.keys():
            result = self.readConfig(args["configName"])
        else:
            result = self.generateSettingsPage()

        return result
          
    index.exposed = True
