#!/usr/bin/env python
"""
_StageOutTools_

Tools for manipulating StageOut type nodes for including
StageOut processes in a job

"""

import logging
import inspect
import os
import JobCreator.RuntimeTools.RuntimeStageOut as OldRuntimeStageOut
import StageOut.RuntimeStageOut as NewRuntimeStageOut
from MB.FileMetaBroker import FileMetaBroker
from MB.Persistency import save as saveMetabroker
from IMProv.IMProvDoc import IMProvDoc
from xml.sax import make_parser
from IMProv.IMProvLoader import IMProvHandler
from IMProv.IMProvQuery import IMProvQuery
from ProdAgentCore.PluginConfiguration import loadPluginConfig

class InsertStageOut:
    """
    _InsertStageOutAttrs_

    Put in standard StageOut specific fields in the task Objects

    """
    def __call__(self, taskObject):
        """
        _operator()_

        Act on a taskObject and if its type is StageOut, add in the standard fields for a StageOut tgask

        """
        if taskObject['Type'] != "StageOut":
            return
        #  //
        # // Lists of pre stage out and post stage out shell commands
        #//  for setting up stage out env
        #  //Should be bash shell commands as strings
        taskObject['PreStageOutCommands'] = []
        taskObject['PostStageOutCommands'] = []

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
        taskObject['StageOutFor'] = stageOutFor
        #  //
        # // Template meta broker that will be used to define 
        #//  parameters for the stage out.
        #  //A list of these objects may be provided to provide
        # // alternative stage out settings for retries.
        #//  
        taskObject['StageOutTemplates'] = [FileMetaBroker()]
        taskObject.addIMProvDoc('StageOutTemplatesFile')

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
        # // Pre and Post Stage out commands
        #//
        precomms = taskObject.get("PreStageOutCommands", [])
        postcomms = taskObject.get("PostStageOutCommands", [])
        

        #  //
        # // Install the main script
        #//
        srcfile = inspect.getsourcefile(OldRuntimeStageOut)
        if not os.access(srcfile, os.X_OK):
            os.system("chmod +x %s" % srcfile)
        taskObject.attachFile(srcfile)
        exeScript = taskObject[taskObject['Executable']]

        for precomm in precomms:
            exeScript.append(str(precomm))
        exeScript.append("./RuntimeStageOut.py")
        for postcomm in postcomms:
            exeScript.append(str(postcomm))
            
      
        #  //
        # // Populate the RunResDB
        #//
        runres = taskObject['RunResDB']
        toName = taskObject['Name']
        stageOutFor = taskObject['StageOutFor']
        paramBase = "/%s/StageOutParameters" % toName
        runres.addPath(paramBase)

        runres.addData("/%s/StageOutFor" % paramBase, stageOutFor)
        runres.addData("/%s/NumRetries" % paramBase, 1)
        runres.addData("/%s/Templates" % paramBase,
                       "StageOutTemplatesFile.xml")
        
        runres.addPath("/%s/FilesToTransfer" % paramBase)
        runres.addPath("/%s/TransferSuccess" % paramBase)
        runres.addPath("/%s/TransferFailure" % paramBase)
        

        
        
        
        
        
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
    
class NewInsertStageOut:
    """
    _NewInsertStageOutAttrs_

    Put in standard StageOut specific fields in the task Objects

    """
    def __call__(self, taskObject):
        """
        _operator()_

        Act on a taskObject and if its type is StageOut, add in the standard fields for a StageOut tgask

        """
        if taskObject['Type'] != "StageOut":
            return
        #  //
        # // Lists of pre stage out and post stage out shell commands
        #//  for setting up stage out env
        #  //Should be bash shell commands as strings
        taskObject['PreStageOutCommands'] = []
        taskObject['PostStageOutCommands'] = []

        #  //
        # // Determine what is being staged out from the parent node
        #//  (This should be a CMSSW node)
        parent = taskObject.parent
        if parent == None:
            # no parent => dont know what to stage out
            return

        if parent['Type'] not in  ("CMSSW", "SVSuite"):
            # parent isnt a CMSSW node, dont know what it does...
            return
        stageOutFor = parent['Name']
        taskObject['StageOutFor'] = stageOutFor
        return



class NewPopulateStageOut:
    """
    _NewPopulateStageOut_


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
        # // Pre and Post Stage out commands
        #//
        precomms = taskObject.get("PreStageOutCommands", [])
        postcomms = taskObject.get("PostStageOutCommands", [])
        

        #  //
        # // Install the main script
        #//
        srcfile = inspect.getsourcefile(NewRuntimeStageOut)
        if not os.access(srcfile, os.X_OK):
            os.system("chmod +x %s" % srcfile)
        taskObject.attachFile(srcfile)
        exeScript = taskObject[taskObject['Executable']]

        for precomm in precomms:
            exeScript.append(str(precomm))
        exeScript.append("./RuntimeStageOut.py")
        for postcomm in postcomms:
            exeScript.append(str(postcomm))

      
        #  //
        # // Populate the RunResDB
        #//
        runres = taskObject['RunResDB']
        toName = taskObject['Name']
        stageOutFor = taskObject['StageOutFor']
        paramBase = "/%s/StageOutParameters" % toName
        runres.addPath(paramBase)

        runres.addData("/%s/StageOutFor" % paramBase, stageOutFor)

        #  //
        # // Configuration for retries?
        #//
        try:
            creatorCfg = loadPluginConfig("JobCreator", "Creator")
            stageOutCfg = creatorCfg.get("StageOut", {})
            numRetres = int(stageOutCfg.get("NumberOfRetries", 3))
            retryPause = int(stageOutCfg.get("RetryPauseTime", 600))
            runres.addData("%s/RetryPauseTime" % paramBase, retryPause)
            runres.addData("%s/NumberOfRetries" % paramBase, numRetries)
            msg = "Stage Out Retries = %s; Pause = %s" % (
                numRetries, retryPause)
            logging.debug(msg)
        except:
            logging.debug("No Retry/Pause Stage Out cfg found")
        
        #  //
        # // Is there an override for this in the JobSpec??
        #//
        cfgStr = taskObject['JobSpecNode'].configuration

        if len(cfgStr) == 0:
            return
        
        handler = IMProvHandler()
        parser = make_parser()
        parser.setContentHandler(handler)
        try:
            parser.feed(cfgStr)
        except Exception, ex:
            # No xml data, no override, nothing to be done...
            return

        logging.debug("StageOut Override provided")
        override = handler._ParentDoc
        commandQ = IMProvQuery("/Override/command[text()]")
        optionQ = IMProvQuery("/Override/option[text()]")
        seNameQ = IMProvQuery("/Override/se-name[text()]")
        lfnPrefixQ = IMProvQuery("/Override/lfn-prefix[text()]")
        
        command = commandQ(override)[0]
        option = optionQ(override)[0]
        seName = seNameQ(override)[0]
        lfnPrefix = lfnPrefixQ(override)[0]
        
        logging.debug("%s %s %s %s " % (command, option, seName, lfnPrefix))
        overrideBase = "/%s/StageOutParameters/Override" % toName
        runres.addPath(overrideBase)
        runres.addData("/%s/command" % overrideBase, command)
        runres.addData("/%s/option" % overrideBase, option)
        runres.addData("/%s/se-name" % overrideBase, seName)
        runres.addData("/%s/lfn-prefix" % overrideBase, lfnPrefix)
        
        return
                       
                        
