#!/usr/bin/env python
"""
_JobEmulatorCreator_

Creator plugin for the Job Emulator.

"""

from JobCreator.Registry import registerCreator 
from JobCreator.Creators.CreatorInterface import CreatorInterface

class JobEmulatorCreator(CreatorInterface):
    """
    _JobEmulatorCreator_

    Creator plugin for the Job Emulator.

    """
    def __init__(self):
        #  //
        # // Init the base class, this will load the plugin config
        #//
        CreatorInterface.__init__(self)


    def checkPluginConfig(self):
        """
        _checkPluginConfig_

        This method overrides the base class method and can be used to
        check the configuration for the plugin, setting defaults, filling
        in missing examples or throwing misconfiguration exceptions etc

        """
        #  //
        # // The plugin configuration is available as an instance attribute
        #//  called pluginConfig. It is an instance of
        #  //ProdAgentCore.PluginConfiguration and is basically a dictionary
        # // of dictionaries.
        #//
        if self.pluginConfig == None:
            #  //
            # // None indicates config is either not found or has some
            #//  problem
            print "Plugin Config not available..."
            return
        for block in self.pluginConfig.keys():
            print "Block Name %s has contents:" % block
            for param, value in self.pluginConfig[block].items():
                print "   Parameter: %s = %s " % (param, value)

        return


    def preprocessTree(self, taskObjectTree):
        """
        _preprocessTree_

        This method is a hook to get the entire TaskObject tree
        before you do anything with it.

        This is the place to add in job wide settings, ShREEK Plugins
        etc.

        """
        print taskObjectTree


    def postprocessTree(self, taskObjectTree):
        """
        _postprocessTree_

        After all of the TaskObjects have been processed, this hook
        provides access to the entire tree again.

        """
        print taskObjectTree


    def processTaskObject(self, taskObject):
        """
        _processTaskObject_

        Act on each individual TaskObject.

        It is a good idea to use this method as a distributor based on
        the TaskObject's Type value. This will indicate if the TaskObject
        is a CMSSW task, StageOut task etc

        """
        typeVal = taskObject['Type']
        print ">>>processTaskObject", typeVal
        if typeVal == "CMSSW":
            self.cmsswTaskObject(taskObject)
            return
        elif typeVal == "Script":
            self.scriptTaskObject(taskObject)
            return
        elif typeVal == "StageOut":
            self.stageOutTaskObject(taskObject)
            return
        else:
            return
        

    #  //
    # // The methods below do not override any base class methods and
    #//  are for demonstration purposes
    #  //
    # //
    #//
    def stageOutTaskObject(self, taskObject):
        """
        _stageOutTaskObject_

        Example method for dealing with a StageOut type TaskObject

        """
        print taskObject['Name'],  "is a StageOut Task"

    def cmsswTaskObject(self, taskObject):
        """
        _cmsswTaskObject_

        Example method for dealing with CMSSW type task Object

        """
        print taskObject['Name'],  "is a CMSSW Task"

    def scriptTaskObject(self, taskObject):
        """
        _scriptTaskObject_

        Example method for dealing with a Script type TaskObject
        
        """
        print taskObject['Name'],  "is a Script Task"
        
#  //
# // Register the creator plugin. Best way is to use the Class Name itself
#//
registerCreator(JobEmulatorCreator, JobEmulatorCreator.__name__)
