#!/usr/bin/env python
"""
_WriteShREEKConfig_

Two tools in here:
*generateShREEKConfig*

Generates a ShREEKConfig Object for a TaskObject Tree using the
toplevel TaskObjects ShREEKTask to provide the Task Tree.
The ShREEKConfig Object is added to the top node of the tree under the
key ShREEKConfig. This enables the manipulation of the ShREEK Configuration


*writeShREEKConfig*

Takes the ShREEKConfig Object from the top level task object
and writes it out into the Target directory

"""
import os
from ShREEK.ShREEKConfig import ShREEKConfig 


def generateShREEKConfig(taskObject): 
    """
    _generateShREEKConfig_

    Create a new ShREEKConfig object and insert the TaskTree from this
    object into it, and add the config to the TaskObject using a ShREEKConfig
    key

    No monitoring or plugin setups are added to ShREEK beyond the default
    set.
    
    """
    config = ShREEKConfig()
    
    config.setJobId(taskObject['JobName'])
    config.setTaskTree(taskObject['ShREEKTask'])    
    taskObject['ShREEKConfig'] = config
    return


def writeShREEKConfig(targetDir, taskObject):
    """
    _writeShREEKConfig_

    Extracts the ShREEKConfig attribute from the taskObject
    and writes it to a file called "ShREEKConfig.xml" in the
    target Directory provided

    """
    configObj = taskObject.get('ShREEKConfig', None)
    if configObj == None:
        msg = "No ShREEKConfig contained in TaskObject:\n"
        msg += "writeShREEKConfig expects a ShREEKConfig attribute\n"
        raise RuntimeError, msg

    writeTo = os.path.join(targetDir, "ShREEKConfig.xml")
    configObj.save(writeTo)
    return

    
    
