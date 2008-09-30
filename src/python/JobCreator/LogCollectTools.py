#!/usr/bin/env python
"""
LogCollectTools

Tools for manipulating LogCollect type nodes for LogCollect jobs

"""

import inspect
import os
import JobCreator.RuntimeTools.RuntimeLogCollector as RuntimeLogCollector

from xml.sax import make_parser
from IMProv.IMProvLoader import IMProvHandler
from IMProv.IMProvQuery import IMProvQuery

import logging

class InsertLogCollect:
    """
    _InsertLogCollect_

    Put in standard LogCollect specific fields in the task Objects

    """
    def __call__(self, taskObject):
        """
        _operator()_

        Act on a taskObject and if its type is CleanUp, add in the standard
        fields for a CleanUp task

        """
        if taskObject['Type'] != "LogCollect":
            return
        #  //
        # // Lists of pre stage out and post stage out shell commands
        #//  for setting up stage out env
        #  //Should be bash shell commands as strings
        taskObject['PreLogCollectCommands'] = []
        taskObject['PostLogCollectCommands'] = []
        #  //
        # // Determine what is being staged out from the parent node
        #//  (This should be a CMSSW node)
        #parent = taskObject.parent
#        if parent == None:
#            msg = "CleanUp Task has no parent, assuming file list..."
#            print msg
#            # no parent => dont know what to stage out
#            taskObject['CleanUpFor'] = None
#            return
#
#        if parent['Type'] != "CMSSW":
#            # parent isnt a CMSSW node, dont know what it does...
#            return
#        cleanUpFor = parent['Name']
#        taskObject['CleanUpFor'] = cleanUpFor

class PopulateLogCollect:
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
        if taskObject['Type'] != "LogCollect":
            return

     
        #  //
        # // Pre and Post Stage out commands
        #//
        precomms = taskObject.get("PreLogCollectCommands", [])
        postcomms = taskObject.get("PostLogCollectCommands", [])
        

        #  //
        # // Install the main script
        #//
        srcfile = inspect.getsourcefile(RuntimeLogCollector)
        if not os.access(srcfile, os.X_OK):
            os.system("chmod +x %s" % srcfile)
        taskObject.attachFile(srcfile)
        exeScript = taskObject[taskObject['Executable']]
        
        envScript = taskObject[taskObject["BashEnvironment"]]
        envCommand = "%s %s" % (envScript.interpreter, envScript.name)
        exeScript.append(envCommand)

        for precomm in precomms:
            exeScript.append(str(precomm))
        exeScript.append("./RuntimeLogCollector.py")
        for postcomm in postcomms:
            exeScript.append(str(postcomm))

      
        #  //
        # // Populate the RunResDB
        #//
        runres = taskObject['RunResDB']
        taskName = taskObject['Name']

        payloadNode = taskObject.get("JobSpecNode", None)
        if payloadNode == None:
            payloadNode = taskObject["PayloadNode"]
        cfgStr = payloadNode.configuration

        if len(cfgStr) == 0:
            return

        handler = IMProvHandler()
        parser = make_parser()
        parser.setContentHandler(handler)
        try:
            parser.feed(cfgStr)
        except Exception, ex:
            # No xml data, no override, nothing to be done...
            logging.error("No StageOut or LFN list found for LogCollect job:")
            raise
        
        logging.debug("StageOut Override for LogArch provided")
        override = handler._ParentDoc
        commandQ = IMProvQuery("/LogCollectorConfig/Override/command[text()]")
        optionQ = IMProvQuery("/LogCollectorConfig/Override/option[text()]")
        seNameQ = IMProvQuery("/LogCollectorConfig/Override/se-name[text()]")
        lfnPrefixQ = IMProvQuery("/LogCollectorConfig/Override/lfn-prefix[text()]")
        
        command = commandQ(override)[0]
        option = optionQ(override)[0]
        seName = seNameQ(override)[0]
        lfnPrefix = lfnPrefixQ(override)[0]
        
        # TODO: Looks like RunRes not being filled, fix.
        logging.debug("%s %s %s %s " % (command, option, seName, lfnPrefix))
        overrideBase = "/%s/StageOutParameters/Override" % taskName
        runres.addPath(overrideBase)
        runres.addData("/%s/command" % overrideBase, command)
        runres.addData("/%s/option" % overrideBase, option)
        runres.addData("/%s/se-name" % overrideBase, seName)
        runres.addData("/%s/lfn-prefix" % overrideBase, lfnPrefix)


        # now get files to collect
        log = handler._ParentDoc
        logQ = IMProvQuery("/LogCollectorConfig/LogsToCollect/[text()]")
        logs = logQ(log)
        logging.debug("LogsToCollect: %s" % str(logs))
        runres.addPath("/%s/LogsToCollect" % taskName)
        for log in logs:
            runres.addData("/%s/lfn" % "LogsToCollect", log)


        # get wf and se
        general = handler._ParentDoc
        wfQ = IMProvQuery("/LogCollectorConfig/Wf[text()]")
        wf = wfQ(general)[0]
        runres.addData("/%s/wf" % taskName, wf)
        seQ = IMProvQuery("/LogCollectorConfig/Se[text()]")
        se = seQ(general)[0]
        runres.addData("/%s/se" % taskName, se)

        return
                       
                        
