#!/usr/bin/env python
"""
_BashEnvironmentMaker_

Create an environment setup bash shell script for a TaskObject
based on the contents of its Environment attribute

The environment script is generated as a StructuredFile and added
to the TaskObject.

Adds the name of the environment structuredFile instance to the TaskObject
as BashEnvironmentSetup

"""
__version__ = "$Revision: 1.1 $"
__revision__ = "$Id: BashEnvironmentMaker.py,v 1.1 2005/12/30 18:46:36 evansde Exp $"
__author__ = "evansde@fnal.gov"


import os



class BashEnvironmentMaker:
    """
    _BashEnvironmentMaker_


    """
    def __init__(self, envScriptName = "taskEnvironment.sh"):
        self.envScriptName = envScriptName
        
    def __call__(self, taskObject):
        """
        _operator()_

        Act on a TaskObject instance to generate a StructuredFile
        object containing a bash setup to setup the Environment specified
        by the TaskObject Environment attribute.
        
        """
        env = taskObject['Environment']
        
        structFile = taskObject.addStructuredFile(self.envScriptName)
        structFile.interpreter = "."
        structFile.setExecutable()
        structFile.append("#!/bin/bash")
        
        for key in env.keys():
            strg = "export %s=%s\n" % (
                key,
                str(env[key]),
                )
            structFile.append(strg)

        taskObject['BashEnvironment'] = self.envScriptName
        return
            
            
