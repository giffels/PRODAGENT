#!/usr/bin/env python
"""
_AppTools_

Tools for generating the main Application wrapper script to run the app itself.
Standard Keys are added to the script object and are converted into the
main script later on (after manipulation to customise the TaskObjects)

The general format of the main CMS App scripts is:

#!/bin/sh
<Local Environment: generated from TaskObject Environment attr>
<PreTaskCommands: list of commands to issue to set up the task>
(  # Start Application Subshell
<PreAppCommands: Setup software here>
<CMSExecutable>  <CMSCommandLineArgs> &
PROCID=$!
echo $PROCID > process_id
wait $PROCID
EXIT_STATUS=$?
<PostAppCommand: Commands to be executed post app in SW env>
exit $EXIT_STATUS
) # End of App environment subshell
EXIT_STATUS=$?
<PostTaskCommands: list of commands to issue post task outside the SW env>
exit EXIT_STATUS # propagate App Exit status out of task

"""
import inspect
import os
import JobCreator.RuntimeTools.RuntimePSetPrep as RuntimePSetModule
import JobCreator.RuntimeTools.RuntimeFwkJobRep as RuntimeFwkJobRep

class InsertAppDetails:
    """
    _InsertAppDetails_

    TaskObject operator.
    Extract the Application information from the JobSpec and generate
    a standard exec script framework in the TaskObject.
    
    """
    def __call__(self, taskObject):
        """
        _operator()_

        Act on a TaskObject, pull application details out of the JobSpec
        it was created from and install a standard structure for generating
        the main Executable script that will be invoked by ShREEK

        """
        jobSpec = taskObject['JobSpecNode']
        if jobSpec.type != "CMSSW":
            return
        appDetails = jobSpec.application
        
        taskObject['CMSProjectName'] = jobSpec.application['Project']
        taskObject['CMSProjectVersion'] = jobSpec.application['Version']
        taskObject['CMSExecutable'] = jobSpec.application['Executable']
        taskObject['CMSPythonPSet'] = jobSpec.configuration
        
        #  //
        # // Add an empty structured file to contain the PSet after
        #//  it is converted from the Python format. 
        taskObject.addStructuredFile("PSet.cfg")
        taskObject['CMSCommandLineArgs'] = " PSet.cfg "
        
        
        
        #  //
        # // Add structures to enable manipulation of task main script
        #//  These fields are used to add commands and script calls
        #  //at intervals in the main script.
        # //
        #//
        taskObject['PreTaskCommands'] = []
        taskObject['PostTaskCommands'] = []
        taskObject['PreAppCommands'] = []
        taskObject['PostAppCommands'] = []
        
        return

    
        
        

class PopulateMainScript:
    """
    _PopulateMainScript_

    Act on the TaskObject to convert fields into commands and insert them
    into the main script structured file instance.

    """
    def __call__(self, taskObject):
        """
        _operator()_

        For a TaskObject that has the appropriate App Keys generate
        a standard task running script
        
        """
        requireKeys = ['CMSProjectName', 
                       'CMSProjectVersion',
                       'CMSExecutable',
                       'CMSCommandLineArgs']
        
        for item in requireKeys:
            if not taskObject.has_key(item):
                return

        #for item in requireKeys:
        #    print item, taskObject[item]

        
        
        exeScript = taskObject[taskObject['Executable']]

        envScript = taskObject[taskObject["BashEnvironment"]]
        envCommand = "%s %s" % (envScript.interpreter, envScript.name)
        exeScript.append(envCommand)

        for item in taskObject['PreTaskCommands']:
            exeScript.append(item)

        exeScript.append("if [ -e \"./exit.status\" ]; then /bin/rm ./exit.status; fi")
        
        exeScript.append("( # Start App Subshell")

        for item in taskObject['PreAppCommands']:
            exeScript.append(item)

        exeComm = "%s %s &" % (taskObject['CMSExecutable'],
                               taskObject['CMSCommandLineArgs'])
        exeScript.append(exeComm)
        exeScript.append("PROCID=$!")
        exeScript.append("echo $PROCID > process_id")
        exeScript.append("wait $PROCID")
        exeScript.append("EXIT_STATUS=$?")
        exeScript.append("echo \"$EXIT_STATUS\" > exit.status")
        exeScript.append("echo \"App exit status: $EXIT_STATUS\"")
        for item in taskObject['PostAppCommands']:
            exeScript.append(item)
        exeScript.append("exit $EXIT_STATUS")
        exeScript.append(") # End of App Subshell")
        exeScript.append("EXIT_STATUS=$?")
        for item in taskObject['PostTaskCommands']:
            exeScript.append(item)
        exeScript.append("echo \"Ended: `date +%s`\"")
        exeScript.append("exit $EXIT_STATUS")
        return
        
        

class InsertPythonPSet:
    """
    _InsertPythonPSet_

    Insert the Python PSet object as a StructuredFile so that
    it can be reloaded at runtime and dumped into {{{}}}format

    This object also attaches the runtime file for dumping out the
    {{{}}}cfg file, as well as the PreTaskCommand to invoke it
    
    """
    
    def __call__(self, taskObject):
        """
        _operator()_

        Act on a TaskObject to extract the CMSPythonPSet
        attribute after it has been edited, and convert it into
        a StructuredFile to be written into the JobArea

        """
        psetConfig = taskObject.get('CMSPythonPSet', None)
        if psetConfig == None:
            return
        psetFile = taskObject.addStructuredFile("PSet.py")
        psetFile.append(str(psetConfig))

        #  //
        # // Install runtime script and add command to invoke it
        #//
        srcfile = inspect.getsourcefile(RuntimePSetModule)
        taskObject.attachFile(srcfile)
        taskObject['PreTaskCommands'].append(
            "./RuntimePSetPrep.py PSet.py PSet.cfg"
            )
        
        return
        
        
        
class InsertJobReportTools:
    """
    _InsertJobReportTools_

    Insert the Runtime tool for processing the job report after it has
    been produced by a CMSSW executable

    """
    def __call__(self, taskObject):
        """
        _operator()_

        Act on the TaskObject instance and insert the Runtime job report
        processing script that runs after the executable.

        """
        if taskObject['Type'] != "CMSSW":
            return

        srcfile = inspect.getsourcefile(RuntimeFwkJobRep)
        if not os.access(srcfile, os.X_OK):
            os.system("chmod +x %s" % srcfile)
        taskObject.attachFile(srcfile)
        taskObject['PostTaskCommands'].append(
            "./RuntimeFwkJobRep.py "
            )
        return
    
    
