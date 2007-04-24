#!/usr/bin/env python
"""
_BulkSubmitterInterface_

Implement a bulk friendly version of the SubmitterInterface that doesnt
create the job tarball, but provides the same API to the JobSubmitter
component


"""

import logging
import popen2
import fcntl, select, sys, os

from ProdAgentCore.Configuration import ProdAgentConfiguration
from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.PluginConfiguration import loadPluginConfig
from ProdAgentCore.ProdAgentException import ProdAgentException



def makeNonBlocking(fd):
    fl = fcntl.fcntl(fd, fcntl.F_GETFL)
    try:
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NDELAY)
    except AttributeError:
	fcntl.fcntl(fd, fcntl.F_SETFL, fl | fcntl.FNDELAY)
    

class BulkSubmitterInterface:

    def __init__(self):
        self.parameters = {}
        self.toSubmit = {}
        self.specFiles = {}
        self.isBulk = False
        self.primarySpecInstance = None
        self.whitelist = []
        self.blacklist = []
        self.applicationVersions = []
        self.publishToDashboard = []
        
        #  //
        # // Load plugin configuration
        #//
        self.pluginConfig = None
        try:
            #  //
            # // Always searches in JobSubmitter Config Block
            #//  for parameter called SubmitterPluginConfig
            self.pluginConfig = loadPluginConfig("JobSubmitter",
                                                 "Submitter")
        except StandardError, ex:
            msg = "Failed to load Plugin Config for Submitter Plugin:\n"
            msg += "Plugin Name: %s\n" % self.__class__.__name__
            msg += str(ex)
            logging.warning(msg)

        self.checkPluginConfig()

        
        
    def __call__(self, workingDir = None, jobCreationArea = None, 
                 jobname = None, **args):
        """
        _Operator()_

        Take a working area, job area and job name and generate
        a tarball in the working area containing the jobCreationArea
        contents.
        
        Invoke the overloaded methods to handle wrapper creation and
        submission for that job

        """
        logging.debug("BulkSubmitterInterface.__call__")
        logging.debug("Subclass:%s" % self.__class__.__name__)
        self.parameters.update(args)
        

        self.primarySpecInstance = self.parameters['JobSpecInstance']
        jobSpecCaches = self.parameters.get("CacheMap", {})
        self.blacklist = self.primarySpecInstance.siteBlacklist
        self.whitelist = self.primarySpecInstance.siteWhitelist
        self.applicationVersions = self.primarySpecInstance.listApplicationVersions()
                   
        
        if not self.primarySpecInstance.isBulkSpec():
            logging.debug("Non Bulk Submission...")
            self.isBulk = False
            self.toSubmit.update(jobSpecCaches)
            nameOfJob = self.primarySpecInstance.parameters['JobName']
            self.specFiles[nameOfJob] = "%s/%s-JobSpec.xml" % (
                jobSpecCaches[nameOfJob], nameOfJob)
            logging.debug("self.toSubmit = %s" % self.toSubmit)
            logging.debug("self.specFiles = %s" % self.specFiles) 
        else:
            logging.debug("Non Bulk Submission...")
            self.isBulk = True
            self.toSubmit.update(jobSpecCaches)
            self.specFiles.update(self.primarySpecInstance.bulkSpecs)
            logging.debug("self.toSubmit = %s" % self.toSubmit)
            logging.debug("self.specFiles = %s" % self.specFiles)
            
        #  //
        # // Invoke whatever is needed to do the submission
        #//
        self.doSubmit()
        return
    
    def checkPluginConfig(self):
        """
        _checkPluginConfig_

        Override this method to check/set defaults etc for the
        Plugin config for the Submitter Plugin being used

        If self.pluginConfig == None, there was an error loading the config
        or it was not found

        """
        pass
    
    def doSubmit(self, wrapperScript, jobTarball):
        """
        _doSubmit_
        
        Invoke some command that submits the job.

        Arguments are the location of the wrapper script to submit the job
        with as an executable, and the location of the jobTarball that
        contains the actual job guts.
        
        """
        msg =  "Virtual Method SubmitterInterface.doSubmit called"
        raise RuntimeError, msg
        

    

        
    
    
    def executeCommand(self, command):
        """
        _executeCommand_

        Util it execute the command provided in a popen object

        """
        logging.debug("SubmitterInterface.executeCommand:%s" % command)

        child = popen2.Popen3(command, 1) # capture stdout and stderr from command
        child.tochild.close()             # don't need to talk to child
        outfile = child.fromchild 
        outfd = outfile.fileno()
        errfile = child.childerr
        errfd = errfile.fileno()
        makeNonBlocking(outfd)            # don't deadlock!
        makeNonBlocking(errfd)
        outdata = errdata = ''
        outeof = erreof = 0
        stdoutBuffer = ""
        while 1:
            ready = select.select([outfd,errfd],[],[]) # wait for input
            if outfd in ready[0]:
                outchunk = outfile.read()
                if outchunk == '': outeof = 1
                stdoutBuffer += outchunk
                sys.stdout.write(outchunk)
            if errfd in ready[0]:
                errchunk = errfile.read()
                if errchunk == '': erreof = 1
                sys.stderr.write(errchunk)
            if outeof and erreof: break
            select.select([],[],[],.1) # give a little time for buffers to fill

        try:
            exitCode = child.poll()
        except Exception, ex:
            msg = "Error retrieving child exit code: %s\n" % ex
            msg = "while executing command:\n"
            msg += command
            logging.error("BulkSubmitterInterface:Failed to Execute Command")
            logging.error(msg)
            raise RuntimeError, msg
        
        if exitCode:
            msg = "Error executing command:\n"
            msg += command
            msg += "Exited with code: %s\n" % exitCode
            logging.error("SubmitterInterface:Failed to Execute Command")
            logging.error(msg)
            raise RuntimeError, msg
        return  stdoutBuffer
    
    def publishSubmitToDashboard(self, dashboardInfo):
        """
        _publishSubmitToDashboard_

        Publish the dashboard info to the appropriate destination

        NOTE: should probably read destination from cfg file, hardcoding
        it here for time being.

        """
        #if dashboardInfo == None:
        #    logging.debug(
        #        "SubmitterInterface: No DashboardInfo available for job"
        #        )
        #    return
        #dashboardInfo['ApplicationVersion'] = self.listToString(self.parameters['AppVersions'])
        #dashboardInfo['TargetCE'] = self.listToString(self.parameters['Whitelist'])
        #dashboardInfo.addDestination("lxgate35.cern.ch", 8884)
        #dashboardInfo.publish(5)
        return
    
