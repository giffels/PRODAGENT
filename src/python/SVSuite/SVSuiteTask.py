#!/usr/bin/env python
"""
_SVSuiteTask_


Object to manage and invoke an SVSuite task based on a Configuration
object passed to it

It is responsible for providing the environment required for the tools
to be run, and manage the execution of the tools

"""

import os

from SVSuite.SVSuiteTool import SVSuiteTool
from SVSuite.SVSuiteError import SVSuiteToolFailure
from SVSuite.SVSuiteError import SVSuiteStageInFailure
from SVSuite.StageIn import stageInFile
from SVSuite.Execute import execute

from FwkJobRep.FwkJobReport import FwkJobReport



class SVSuiteTask:
    """
    _SVSuiteTask_
    

    """
    def __init__(self, configuration):
        self.configuration = configuration
        self.failure = False
        self.failureDetails = None
        self.jobReport = FwkJobReport()



    def __call__(self):
        """
        _operator()_

        Invoke this task, by running the various components of the
        task in order

        """
        
        self.stageIn()

        if not self.failure:
            self.runTools()
            
        if self.failure:
            self.manageFailure()
        else:
            self.manageOutput()
        
        if self.configuration.writeJobReport:
            # save the job report file.
            self.jobReport.write("FrameworkJobReport.xml")
        return

    def stageIn(self):
        """
        _stageIn_

        Invoke the StageIn of reference data and SW packaging using
        the LFNs provided.
        
        """
        if not self.configuration.doStageIn:
            return

        #  //
        # // check directory to stage into
        #//
        dataDir = self.configuration.svSuiteDataDir
        if not os.path.exists(dataDir):
            os.makedirs(dataDir)

        #  //
        # // do the stage in to the data dirs
        #//
        results = []
        for stage in self.configuration.stageIn:
            try:
                result = stageInFile(stage, dataDir)
                results.append(result)
            except SVSuiteStageInFailure, failure:
                msg = "Failed to Stage In %s:\n" % stage
                msg += str(failure)
                print msg
                self.failure = True
                self.failureDetails = failure
                return
        #  //
        # // Unpack the tarfiles
        #//
        for result in results:
            command = "tar -zxf %s --directory %s" % (result,
                                                      os.path.dirname(result))
            execute(command)
            
        return
            
            


    def runTools(self):
        """
        _runTools_

        For each tool, run that tool in the appropriate environment

        """
        for tool in self.configuration.tools:
            svTool = SVSuiteTool(tool)
            #  //
            # // Add in the settings from the config so the tool
            #//  has the settings to work with
            svTool.environment["SVSUITE_DATA_DIR"] = \
                             self.configuration.svSuiteDataDir
            svTool.environment["SVSUITE_OUTPUT_DIR"] =  \
                             self.configuration.svSuiteOutputDir
            svTool.environment["SVSUITE_BIN_DIR"] = \
                             self.configuration.svSuiteBinDir
            svTool.environment["SVSUITE_INPUT_DIR"] =  \
                             self.configuration.svSuiteInputDir
            svTool.environment['SVSUITE_VERSION'] = \
                             self.configuration.swVersion
            svTool.swSetupCommand = self.configuration.swSetupCommand
            
            #  //
            # // Invoke the tool
            #//
            try:
                svTool()
            except SVSuiteToolFailure, failure:
                msg = "Failed to Run Tool:\n"
                msg += str(failure)
                print msg
                self.failure = True
                self.failureDetails = failure
                break
            self.broadcast(list(svTool.filter))
        return

    def manageOutput(self):
        """
        _manageOutput_

        Handle the output from this task by Tarring it up and adding it
        to the job report for stage out.

        """


        lfn = self.configuration.outputLfn
        tarname = os.path.basename(lfn)
        pfn = os.path.join(os.getcwd(), tarname)
        
        
        if self.configuration.zipOutput:
            sourceDir = self.configuration.svSuiteOutputDir
            # make tarfile here
            tarCommand = "tar -czf %s -C %s %s " % (
                tarname,
                os.path.dirname(sourceDir),
                os.path.basename(sourceDir)
                )
            execute(tarCommand)
            outputFile = self.jobReport.newFile()            
            outputFile["LFN"] = lfn
            outputFile["PFN"] = pfn
        return

    def manageFailure(self):
        """
        _manageFailure_

        In the event of a tool failure, generate the error report and add
        it to the job report

        """
        errDetails = self.jobReport.addError(
            self.failureDetails.exitStatus,
            self.failureDetails.__class__.__name__)
        errDetails['Description'] = str(self.failureDetails)

        self.jobReport.status = "Failed"
        self.jobReport.exitCode = self.failureDetails.exitStatus
        return
        
        
    def broadcast(self, data):
        """
        _broadcast_

        List of OVAL flagged data lines provided, format them and dispatch
        to MonALISA

        """
        # implement me...
        pass
    
