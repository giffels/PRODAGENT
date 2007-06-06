#!/usr/bin/env python
"""
_ARCCreator_

ARC Middleware/Nordugrid Creator.

This plugin is used by the job creator component to install standard
environment setup for jobs sent to sites via the ARC Middleware

"""

from JobCreator.Registry import registerCreator
from JobCreator.Creators.CreatorInterface import CreatorInterface
from JobCreator.JCException import JCException

from JobCreator.ScramSetupTools import setupScramEnvironment
from JobCreator.ScramSetupTools import scramProjectCommand
from JobCreator.ScramSetupTools import scramRuntimeCommand

class ARCCreator(CreatorInterface):
    """
    _ARCCreator

    Process a TaskObject Tree and add in setup details appropriate to
    the ARC Middleware environment.

    

    """
    def __init__(self):
        CreatorInterface.__init__(self)
        self.swSetupCommand = None


    def checkPluginConfig(self):
        """
        _checkPluginConfig_

        Validate/default config for this plugin

        This plugin has a Config file associated with it:
        CreatorPluginConfig.xml

        You can use this method to check what is in there

        """

        if self.pluginConfig == None:
            msg = "Creator Plugin Config could not be loaded for:\n"
            msg += self.__class__.__name__
            raise JCException(msg, ClassInstance = self)
            
	if not self.pluginConfig.has_key("SoftwareSetup"):
            swsetup = self.pluginConfig.newBlock("SoftwareSetup")
            swsetup['ScramCommand'] = "scramv1"
            swsetup['ScramArch'] = "slc3_ia32_gcc323"

        #  //
        # // Make sure the standard environment setup is set to
        #//  something.
        self.swSetupCommand = self.pluginConfig['SoftwareSetup'].get(
            "SoftwareSetupCommand",
            ". $VO_CMS_SW_DIR/cmsset_default.sh ;") # CERN default
        
            
        return

    
    def processTaskObject(self, taskObject):
        """
        _processTaskObject_

        Process each TaskObject based on type

        CMSSW means add a CMSSW setup to it, so get CMS_PATH defined,
              make scram available etc.

        
        StageOut means it is a stage out node, needs to setup castor
                 SITECONF, TFC etc

        CleanUp is for cleaning unmerged files, so will probably mean same
                setup as StageOut is needed
        
        """
        typeVal = taskObject['Type']
        if typeVal == "CMSSW":
            self.handleCMSSWTaskObject(taskObject)
            return
        elif typeVal == "StageOut":
            self.handleStageOut(taskObject)
            return
        elif typeVal == "CleanUp":
            self.handleCleanUp(taskObject)
            return
        else:
            return


    def preprocessTree(self, taskObjectTree):
        """
        _preprocessTree_

        Get the entire tree of task objects, useful for
        installing job wide monitoring etc.

        Skip this for now.

        """
        return


   



    def handleCMSSWTaskObject(self, taskObject):
        """
        _handleCMSSWTaskObject_
        
        Method to customise CMSSW type (Eg cmsRun application) TaskObjects
        
        """
        test = taskObject.has_key("CMSProjectVersion") and \
               taskObject.has_key("CMSProjectName")
        if not test:
            return

        taskObject['Environment'].addVariable(
            "SCRAM_ARCH",
            self.pluginConfig['SoftwareSetup']['ScramArch'])

        
        #  //
        # // Command that sets up CMS environment, makes scram available 
        #//  etc
        taskObject['PreTaskCommands'].append(
           setupScramEnvironment(self.swSetupCommand))

        #  //
        # // Build a scram setup for the job environment
        #//
        scramSetup = taskObject.addStructuredFile("scramSetup.sh")
        scramSetup.interpreter = "."
        taskObject['PreAppCommands'].append(
          setupScramEnvironment(self.swSetupCommand))
        taskObject['PreAppCommands'].append(". scramSetup.sh")

        scramSetup.append("#!/bin/bash")
        scramSetup.append(
          scramProjectCommand(taskObject['CMSProjectName'],
                            taskObject['CMSProjectVersion'])
        )
        scramSetup.append(
          scramRuntimeCommand(
            taskObject['CMSProjectVersion'],
            self.pluginConfig['SoftwareSetup']['ScramCommand'],
            True)
          )
        
        return


    def handleStageOut(self, taskObject):
        """
        _handleStageOut_
        
        Handle a StageOut task object.
        
        """
        #  //
        # // Assuming stage out tools have same setup as rest of CMS
        #//  environment
        taskObject['PreStageOutCommands'].append(self.swSetupCommand)
        
        
        return
    
    
    def handleCleanUp(self, taskObject):
        """
        _handleCleanUp_

        Handle a CleanUp task object

        """
        #  //
        # // Assuming stage out tools have same setup as rest of CMS
        #//  environment, this is usually the same as the StageOut
        taskObject['PreCleanUpCommands'].append(
            self.swSetupCommand
            )
        
        return


#  //
# // Register an instance of ARCCreator with the Creator Registry
#//  (Add import in Creators/__init__.py of this module to enable auto
#  // registration based on import of entire module)
# // 
#//
registerCreator(ARCCreator, ARCCreator.__name__)
