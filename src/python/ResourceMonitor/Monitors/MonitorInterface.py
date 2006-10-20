#!/usr/bin/env python
"""
_MonitorInterface_

Interface class for Monitor plugins.

Interface is pretty simple:

Override the Call method to return a ResourceConstraint instance,
which is the number of resources available for jobs and constraints.

The PluginConfig mechanism is used for this as well, so you can read
dynamic parameters from self.pluginConfig


"""

import logging
from ProdAgentCore.PluginConfiguration import loadPluginConfig
from ProdAgentCore.ResourceConstraint import ResourceConstraint

class MonitorInterface:
    """
    _MonitorInterface_
    

    Abstract Interface Class for Resource Monitor Plugins

    """
    def __init__(self):
        self.pluginConfig = None
        try:
            #  //
            # // Always searches in ResourceMonitor Config Block
            #//  for parameter called MonitorPluginConfig
            self.pluginConfig = loadPluginConfig("ResourceMonitor",
                                                 "Monitor")
        except StandardError, ex:
            msg = "Failed to load Plugin Config for Monitor Plugin:\n"
            msg += "Plugin Name: %s\n" % self.__class__.__name__
            msg += str(ex)
            logging.warning(msg)
            
        self.checkPluginConfig()

    def newConstraint(self):
        """
        _newConstraint_

        Factory method, returns a new, empty constraint

        """
        return ResourceConstraint()

    
    def checkPluginConfig(self):
        """
        _checkPluginConfig_

        Override this method to validate/add defaults/manipulate the
        plugin config after it has been loaded.

        If the pluginConfig is None it either doesnt exist, or there was
        an error.
        
        """
        pass


    def __call__(self):
        """
        _operator()_

        Override this method to make whatever callouts you need to
        determine that you have resources available

        Should return a list of ResourceConstraint instances that will be
        published as  ResourceAvailable events
        """
        return [ResourceConstraint() ]

    
