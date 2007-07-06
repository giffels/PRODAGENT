#!/usr/bin/env python
"""
_LogArchTools_

Tools for populating a LogArchive type TaskObject as part of a job


"""

import inspect, os
import logging
from ProdCommon.MCPayloads.PayloadNode import listAllNames
import JobCreator.RuntimeTools.RuntimeLogArch as RuntimeLogArch
from xml.sax import make_parser
from IMProv.IMProvLoader import IMProvHandler
from IMProv.IMProvQuery import IMProvQuery


class InstallLogArch:
    """
    _InstallLogArch_

    Install standard fields into a LogArch TaskObject

    """
    def __call__(self, taskObject):
        if taskObject['Type'] != "LogArchive":
            return

        runres = taskObject['RunResDB']
        taskName = taskObject['Name']

        specNode = taskObject.get("PayloadNode", None)
        if specNode == None:
            specNode = taskObject.get("JobSpecNode")

        inputNodes = listAllNames(specNode)
            
        for item in inputNodes:
            if item == taskName:
                continue
            runres.addData("/%s/InputTasks" % taskName, item)

        taskObject['PreLogArchCommands'] = []
        taskObject['PostLogArchCommands'] = []
        
        #  //
        # // add files matching the following patterns to
        #//  the tarball. These should be compileable regexps.
        #  //File names in the task directory will be compared
        # // to these using 
        #//  re.compile(expr)
        #  //re.search(expr) 
        # // 
        #//
        matchFiles = [
            ".log$",
            "^FrameworkJobReport.xml$",
            "^FrameworkJobReport-Backup.xml$",
            "^PSet.py$",
            ]

        taskObject['LogMatchRegexps'] = matchFiles
        return

class PopulateLogArch:
    """
    _PopulateLogArch_

    Convert TaskObject fields & data into actual scripts

    """
    
    
    def __call__(self, taskObject):
        """
        _operator()_

        Operate on a Task Object to install the runtime scripts

        """
        if taskObject['Type'] != "LogArchive":
            return

        runres = taskObject['RunResDB']
        taskName = taskObject['Name']
        #  //
        # // Pre and Post Stage out commands
        #//
        precomms = taskObject.get("PreLogArchCommands", [])
        postcomms = taskObject.get("PostLogArchCommands", [])
        

        #  //
        # // Install the main script
        #//
        srcfile = inspect.getsourcefile(RuntimeLogArch)
        if not os.access(srcfile, os.X_OK):
            os.system("chmod +x %s" % srcfile)
        taskObject.attachFile(srcfile)
        exeScript = taskObject[taskObject['Executable']]

        for precomm in precomms:
            exeScript.append(str(precomm))
        exeScript.append("./RuntimeLogArch.py")
        for postcomm in postcomms:
            exeScript.append(str(postcomm))

        #  //
        # // Add complete list of regexps to RunRes configuration
        #//
        for regexp in taskObject['LogMatchRegexps']:
            runres.addData("%s/LogMatchRegexp" % taskName, regexp)
            
        #  //
        # // If an override stage out location has been provided
        #//  pack it into the configuration
        
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
            return
        
        logging.debug("StageOut Override for LogArch provided")
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
        overrideBase = "/%s/StageOutParameters/Override" % taskName
        runres.addPath(overrideBase)
        runres.addData("/%s/command" % overrideBase, command)
        runres.addData("/%s/option" % overrideBase, option)
        runres.addData("/%s/se-name" % overrideBase, seName)
        runres.addData("/%s/lfn-prefix" % overrideBase, lfnPrefix)
        
        return
        
