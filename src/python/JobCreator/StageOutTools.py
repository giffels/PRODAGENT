#!/usr/bin/env python
"""
_StageOutTools_

Tools for manipulating StageOut type nodes for including
StageOut processes in a job

"""

import inspect
import os
import JobCreator.RuntimeTools.RuntimeStageOut as RuntimeStageOut
from MB.FileMetaBroker import FileMetaBroker
from MB.Persistency import save as saveMetabroker
from IMProv.IMProvDoc import IMProvDoc


class PopulateStageOut:
    """
    _PopulateStageOut_


    Take a StageOut task object and add standard stuff to it, including
    metadata fields, RunRes info and main script.

    Site specific customisation should be done in the Creator object
    for that site

    """
    def __call__(self, taskObject):
        """
        _operator()_


        Operate on a TaskObject of type StageOut to insert a standard
        StageOut structure

        """
        if taskObject['Type'] != "StageOut":
            return

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
        stageOutFor = parent['Name']

        #  //
        # // Install the main script
        #//
        srcfile = inspect.getsourcefile(RuntimeStageOut)
        if not os.access(srcfile, os.X_OK):
            os.system("chmod +x %s" % srcfile)
        taskObject.attachFile(srcfile)
        exeScript = taskObject[taskObject['Executable']]
        exeScript.append("./RuntimeStageOut.py")

      
        #  //
        # // Populate the RunResDB
        #//
        runres = taskObject['RunResDB']
        toName = taskObject['Name']

        paramBase = "/%s/StageOutParameters" % toName
        runres.addPath(paramBase)

        runres.addData("/%s/StageOutFor" % paramBase, stageOutFor)
        runres.addData("/%s/NumRetries" % paramBase, 1)
        runres.addData("/%s/Templates" % paramBase,
                       "StageOutTemplatesFile.xml")
        
        runres.addPath("/%s/FilesToTransfer" % paramBase)
        runres.addPath("/%s/TransferSuccess" % paramBase)
        runres.addPath("/%s/TransferFailure" % paramBase)
        

        
        
        #  //
        # // Template meta broker that will be used to define 
        #//  parameters for the stage out.
        #  //A list of these objects may be provided to provide
        # // alternative stage out settings for retries.
        #//  
        taskObject['StageOutTemplates'] = [FileMetaBroker()]
        taskObject.addIMProvDoc('StageOutTemplatesFile')
        
        
        return
                       
                        


class StoreStageOutTemplates:
    """
    _StoreStageOutTemplates_

    Operator for StageOut TaskObjects after customisation, that
    inserts the template FMB objects into the RunResDB for the
    stage out task

    """
    def __call__(self, taskObject):
        """
        _operator()_


        """
        if taskObject['Type'] != "StageOut":
            return
        for item in taskObject['StageOutTemplates']:
            taskObject['StageOutTemplatesFile'].addNode(saveMetabroker(item))
        return
    
