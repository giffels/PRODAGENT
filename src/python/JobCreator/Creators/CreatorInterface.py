#!/usr/bin/env python
"""
_CreatorInterface_

Base object for all Creator Plugins.
Creator Plugins should inherit this object, override the appropriate methods
as needed and register their implementation with the Creator Registry


"""

import logging

from ProdAgentCore.PluginConfiguration import loadPluginConfig


class CreatorInterface:
    """
    _CreatorInterface_

    Base callable object to define structure for a Creator plugin.

    The Creator acts on a tree of TaskObjects when the __call__ operator
    is invoked. Hook methods are provided to be allow operations on the
    entire tree or individual nodes.

    An interface to retrieve the plugin arguments from the configuration
    information is provided.

    """
    def __init__(self):
        self.pluginConfig = None

        try:
            self.pluginConfig = loadPluginConfig("JobCreator",
                                                 self.__class__.__name__)
        except StandardError, ex:
            msg = "Failed to load Plugin Config for Creator Plugin:\n"
            msg += "Plugin Name: %s\n" % self.__class__.__name__
            msg += str(ex)
            logging.warning(msg)
            
        self.checkPluginConfig()
            
    def checkPluginConfig(self):
        """
        _checkPluginConfig_

        Override this method to validate/add defaults/manipulate the
        plugin config after it has been loaded.

        If the pluginConfig is None it either doesnt exist, or there was
        an error.
        
        """
        pass


    def __call__(self, taskObjectTree):
        """
        _operator()_

        _Do not override this method_

        Main call operator that exposes the TaskObject tree to all the
        hook methods.
        
        """
        logging.debug(
            "CreatorInterface.__call__ Invoked: %s" % self.__class__.__name__
            )
        #  //
        # // preprocess entire tree
        #//
        self.preprocessTree(taskObjectTree)
        
        #  //
        # // Recursively process all task objects one by one
        #//  with the processTaskObject call
        taskObjectTree(self.processTaskObject)
        
        #  //
        # // post process entire tree
        #//
        self.postprocessTree(taskObjectTree)
        logging.debug(
            "CreatorInterface.__call__ Completed: %s" % self.__class__.__name__
            )
        return
    

    def preprocessTree(self, taskObjectTree):
        """
        _preprocessTree_

        Hook method to access the entire tree before anything has happened to
        it. Good place for installing toplevel things like ShREEK Plugins,
        extra python modules, job wide settings.

        """
        pass
        
        
    
    def processTaskObject(self, taskObject):
        """
        _processTaskObject_

        Process an individual TaskObject. Can be used as a distributor method
        based on type.

        """
        pass

    
    def postprocessTree(self, taskObjectTree):
        """
        _postprocessTree_

        Hook method to acces the entire tree after all the processing has been
        done.

        """
        pass
    
