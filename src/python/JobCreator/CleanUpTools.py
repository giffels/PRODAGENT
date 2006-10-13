#!/usr/bin/env python
"""
_CleanUpTools_

Tools for manipulating CleanUp type nodes for including
CleanUp processes in a job

"""

import inspect
import os
import StageOut.RuntimeCleanUp as RuntimeCleanUp


class InsertCleanUp:
    """
    _InsertCleanUp_

    Put in standard CleanUp specific fields in the task Objects

    """
    def __call__(self, taskObject):
        """
        _operator()_

        Act on a taskObject and if its type is CleanUp, add in the standard
        fields for a CleanUp task

        """
        if taskObject['Type'] != "CleanUp":
            return
        #  //
        # // Lists of pre stage out and post stage out shell commands
        #//  for setting up stage out env
        #  //Should be bash shell commands as strings
        taskObject['PreCleanUpCommands'] = []
        taskObject['PostCleanUpCommands'] = []

        #  //
        # // Determine what is being staged out from the parent node
        #//  (This should be a CMSSW node)
        parent = taskObject.parent
        if parent == None:
            # no parent => dont know what to stage out
            return

        if parent['Type'] != "CMSSW":
            # parent isnt a CMSSW node, dont know what it does...
            return
        cleanUpFor = parent['Name']
        taskObject['CleanUpFor'] = cleanUpFor

class PopulateCleanUp:
    """
    _PopulateCleanUp_


    Site specific customisation should be done in the Creator object
    for that site. This object is used after the customisation

    """
    def __call__(self, taskObject):
        """
        _operator()_


        Operate on a TaskObject of type CleanUp to insert a standard
        StageOut structure

        """
        if taskObject['Type'] != "CleanUp":
            return

     
        #  //
        # // Pre and Post Stage out commands
        #//
        precomms = taskObject.get("PreCleanUpCommands", [])
        postcomms = taskObject.get("PostCleanUpCommands", [])
        

        #  //
        # // Install the main script
        #//
        srcfile = inspect.getsourcefile(RuntimeCleanUp)
        if not os.access(srcfile, os.X_OK):
            os.system("chmod +x %s" % srcfile)
        taskObject.attachFile(srcfile)
        exeScript = taskObject[taskObject['Executable']]

        for precomm in precomms:
            exeScript.append(str(precomm))
        exeScript.append("./RuntimeCleanUp.py")
        for postcomm in postcomms:
            exeScript.append(str(postcomm))

      
        #  //
        # // Populate the RunResDB
        #//
        runres = taskObject['RunResDB']
        toName = taskObject['Name']
        cleanUpFor = taskObject['CleanUpFor']
        paramBase = "/%s/CleanUpParameters" % toName
        runres.addPath(paramBase)

        runres.addData("/%s/CleanUpFor" % paramBase, cleanUpFor)
        return
                       
                        

