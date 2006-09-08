#!/usr/bin/env python
"""
_SVSuiteTools_

Tools for populating and concretizing an SVSuite type TaskObject

"""
import inspect
import os
from xml.sax import make_parser
from IMProv.IMProvLoader import IMProvHandler
from SVSuite.Configuration import Configuration
import SVSuite.RuntimeSVSuite as RuntimeSVSuite
from JobCreator.AppTools import _StandardPreamble

class InsertSVSuiteDetails:
    """
    _InsertSVSuiteDetails_

    Add appropriate fields and metadata to an SVSuite TaskObject


    """
    def __call__(self, taskObject):
        """
        _operator()_

        Act on a task object if it is SVSuite Type

        """
        jobSpec = taskObject['JobSpecNode']
        if jobSpec.type != "SVSuite":
            return
        
        appDetails = jobSpec.application
        taskObject['CMSProjectName'] = jobSpec.application['Project']
        taskObject['CMSProjectVersion'] = jobSpec.application['Version']
        taskObject['CMSExecutable'] = jobSpec.application['Executable']

        #  //
        # // SVSuite Configuration object
        #//
        svSuiteConfig = Configuration()
        xmlConfig = jobSpec.configuration
        handler = IMProvHandler()
        parser = make_parser()
        parser.setContentHandler(handler)
        parser.feed(xmlConfig)
                
        svSuiteConfig.load(handler._ParentDoc)
        svSuiteConfig.jobId = taskObject['JobName']
        taskObject['SVSuiteConfiguration'] = svSuiteConfig        
        
        taskObject['PreTaskCommands'] = []
        taskObject['PostTaskCommands'] = []

        scramSetup = taskObject.addStructuredFile("scramSetup.sh")
        scramSetup.append("#!/bin/sh")
        scramSetup.append(_StandardPreamble)
        
        
        taskObject['SVSuiteSetupCommand'] = ". scramSetup.sh"
        svSuiteConfig.swSetupCommand = taskObject['SVSuiteSetupCommand']

        lfnBase = taskObject['JobSpecNode'].getParameter("UnmergedLFNBase")[0]
        outputLfn = os.path.join(lfnBase, taskObject['JobName'])
        outputLfn += "-Output.tgz"
        svSuiteConfig.outputLfn = outputLfn
        
        
        #  //
        # // Determine input node name from parent node
        #//  (This should be a CMSSW node)
        parent = taskObject.parent
        if parent == None:
            # no parent => cant add svSuite node
            return

        if parent['Type'] != "CMSSW":
            # parent isnt a CMSSW node, dont know what it does...
            return
        taskObject['SVSuiteInput'] = parent['Name']

        
        return
    
    
class PopulateSVSuite:
    """
    _PopulateSVSuite_

    After customisation by plugins, concretise the SVSuite tasks
    
    """
    def __call__(self, taskObject):
        """
        _operator()_

        

        """
        if taskObject['Type'] != "SVSuite":
            return
        
        #  //
        # // Install the Runtime python script
        #//
        srcfile = inspect.getsourcefile(RuntimeSVSuite)
        if not os.access(srcfile, os.X_OK):
            os.system("chmod +x %s" % srcfile)
        taskObject.attachFile(srcfile)
        exeScript = taskObject[taskObject['Executable']]

        #  //
        # // Populate the runres DB
        #//
        runres = taskObject['RunResDB']
        toName = taskObject['Name']
        input = taskObject['SVSuiteInput']
        paramBase = "/%s/SVSuiteParameters" % toName
        runres.addPath(paramBase)
        runres.addData("/%s/SVSuiteInput" % paramBase, input)
        runres.addData("/%s/SVSuiteSetupCommand" % paramBase,
                       taskObject['SVSuiteSetupCommand'])
        
        #  //
        # // build the main script
        #//
        exeScript = taskObject[taskObject['Executable']]

        #  //
        # // Install standard error handling command
        #//        
        envScript = taskObject[taskObject["BashEnvironment"]]
        envCommand = "%s %s" % (envScript.interpreter, envScript.name)
        exeScript.append(envCommand)

        for item in taskObject['PreTaskCommands']:
            exeScript.append(item)

        exeScript.append("./RuntimeSVSuite.py & ")
        exeScript.append("PROCID=$!")
        exeScript.append("echo $PROCID > process_id")
        exeScript.append("wait $PROCID")
        exeScript.append("EXIT_STATUS=$?")
        exeScript.append(
            "if [ ! -e exit.status ]; then echo \"$EXIT_STATUS\" > exit.status; fi")

        exeScript.append("echo `date +%s` >| end.time")
        for item in taskObject['PostTaskCommands']:
            exeScript.append(item)
        exeScript.append("echo \"Ended: `date +%s`\"")
        exeScript.append("exit $EXIT_STATUS")

        taskObject['SVSuiteConfig'] = taskObject['SVSuiteConfiguration'].save()
        taskObject['IMProvDocs'].append("SVSuiteConfig")
        return
