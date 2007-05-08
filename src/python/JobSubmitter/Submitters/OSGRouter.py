#!/usr/bin/env python
"""
_OSGRouter_

Globus Universe Condor Submitter implementation.
Used for testing jobs in a batch environment, shouldnt be used generally
as it includes no job tracking.


"""

__revision__ = "$Id: OSGRouter.py,v 1.6 2006/11/28 21:58:23 evansde Exp $"

import os
import logging
import time

from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.SubmitterInterface import SubmitterInterface
from JobSubmitter.JSException import JSException
from JobSubmitter.Submitters.OSGUtils import standardScriptHeader


class OSGRouter(SubmitterInterface):
    """
    _OSGRouter_

    Globus Universe condor submitter. Generates a simple JDL file
    and condor_submits it using a dag wrapper and post script to generate
    a callback to the ProdAgent when the job completes.
    

    """
    def checkPluginConfig(self):
        """
        _checkPluginConfig_

        Make sure config has what is required for this submitter

        """
        if self.pluginConfig == None:
            msg = "Failed to load Plugin Config for:\n"
            msg += self.__class__.__name__
            raise JSException( msg, ClassInstance = self)
                               

        # expect globus scheduler in OSG block
        # 
        if not self.pluginConfig.has_key("OSG"):
            msg = "Plugin Config for: %s \n" % self.__class__.__name__
            msg += "Does not contain an OSG config block"
            raise JSException( msg , ClassInstance = self,
                               PluginConfig = self.pluginConfig)

            
        #  //
        # // Extract the mapping of SEName to Jobmanager from the
        #//  plugin config
        siteMapping = self.pluginConfig.get('SENameToJobmanager', None)
        if siteMapping == None:
            msg = "SENameToJobManager not provided in Submitter "
            msg += "pluging configuration\n"
            msg += "This is required for mapping merge jobs to the appropriate"
            msg += "Site based on fileblock name"
            raise JSException(msg, 
                              ClassInstance = self,
                              PluginConfig = self.pluginConfig)
        

    def generateWrapper(self, wrapperName, tarballName, jobname):
        """
        _generateWrapper_

        Use the default wrapper provided by the base class but
        overload this method to also generate a JDL file

        """
        logging.debug("OSGRouter.generateWrapper")

        globusScheduler = self.lookupGlobusScheduler()
        
        logging.debug("OSGRouter: globus scheduler is %s" % globusScheduler)

        
        jdlFile = "%s.jdl" % wrapperName
        logging.debug("OSGRouter.generateWrapper: JDL %s" % jdlFile)
        directory = os.path.dirname(wrapperName)
        jdl = []

        if globusScheduler != None:
            #  //
            # // Scheduler is set => Merge job, go via condor G
            #//
            logging.info("Dispatching job via CondorG")
            jdl.append("universe = globus\n")
            jdl.append("globusscheduler = %s\n" % globusScheduler)
            jdl.append("initialdir = %s\n" % directory)
            jdl.append("Executable = %s\n" % wrapperName)
            jdl.append("transfer_input_files = %s\n" % tarballName)
            jdl.append("transfer_output_files = FrameworkJobReport.xml\n")
            jdl.append("should_transfer_files = YES\n")
            jdl.append("when_to_transfer_output = ON_EXIT\n")
            jdl.append("log_xml = True\n" )
            jdl.append("prod_agent_job_spec_id = %s\n" % jobname)
            jdl.append("prod_agent_workflow_spec_id = %s\n" % (
                self.parameters['JobSpecInstance'].payload.workflow)
                       )
            jdl.append("Output = %s-condor.out\n" % jobname)
            jdl.append("Error = %s-condor.err\n" %  jobname)
            jdl.append("Log = %s-condor.log\n" % jobname)
            jdl.append("notification = NEVER\n")
            
            if self.pluginConfig['OSG']['GlobusScheduler'].endswith("jobmanager-pbs"):
                jdl.append("GlobusRSL=(jobtype=single)\n")


            #  //
            # // Add in parameters that indicate prodagent job types etc
            #//
            jdl.append("+ProdAgent_JobID = \"%s\"\n" % self.parameters['JobName'])
            jdl.append("+ProdAgent_JobType = \"%s\"\n" % self.parameters['JobSpecInstance'].parameters['JobType'])
            jdl.append("Queue\n")
        
        else:
            #  //
            # // No scheduler, so we submit vanilla to the router.
            #//
            logging.info("Dispatching job via Vanilla Condor")
            jdl.append("universe = vanilla\n")
            jdl.append("requirements = false\n")
            jdl.append("+WantJobRouter = True\n")
            jdl.append("X509UserProxy = $ENV(X509_USER_PROXY)\n")
            jdl.append("initialdir = %s\n" % directory)
            jdl.append("Executable = %s\n" % wrapperName)
            jdl.append("transfer_input_files = %s\n" % tarballName)
            jdl.append("transfer_output_files = FrameworkJobReport.xml\n")
            jdl.append("should_transfer_files = YES\n")
            jdl.append("notification = NEVER\n")
            jdl.append("log_xml = True\n" )
            jdl.append("prod_agent_job_spec_id = %s\n" % jobname)
            jdl.append("prod_agent_workflow_spec_id = %s\n" % (
                self.parameters['JobSpecInstance'].payload.workflow)
                       )
            jdl.append("when_to_transfer_output = ON_EXIT\n")
            jdl.append("Output = %s-condor.out\n" % jobname)
            jdl.append("Error = %s-condor.err\n" %  jobname)
            jdl.append("Log = %s-condor.log\n" % jobname)

            #  //
            # // Add in parameters that indicate prodagent job types etc
            #//
            jdl.append("+ProdAgent_JobID = \"%s\"\n" % self.parameters['JobName'])
            jdl.append("+ProdAgent_JobType = \"%s\"\n" % self.parameters['JobSpecInstance'].parameters['JobType'])
            
            jdl.append("Queue\n")
            
            
        
        handle = open(jdlFile, 'w')
        handle.writelines(jdl)
        handle.close()

        #  //
        # // Check logfiles, and back them up if they exist
        #//
        logFile = "%s/%s-condor.log" % (directory, jobname)
        outFile = "%s/%s-condor.out" % (directory, jobname)
        errFile = "%s/%s-condor.err" % (directory, jobname)

        for logfilePath in (logFile, outFile, errFile):
            if os.path.exists(logfilePath):
                logging.debug("Found file: %s" % logfilePath)
                newPath = "%s.backup.%s" % (logfilePath, int(time.time()))
                logging.debug("Backing up logfile to: %s" % newPath)
                os.system("/bin/cp %s %s" % (logfilePath, newPath))
                
        
        workflowName = self.parameters['JobSpecInstance'].payload.workflow               
        tarballBaseName = os.path.basename(tarballName)
        script = ["#!/bin/sh\n"]
        script.extend(standardScriptHeader(self.parameters['JobName'], workflowName))
        script.append(
            "tar -zxf $PRODAGENT_JOB_INITIALDIR/%s\n" % tarballBaseName 
            )
        script.append("cd %s\n" % jobname)
        script.append("./run.sh\n")
        script.append(
            "cp ./FrameworkJobReport.xml $PRODAGENT_JOB_INITIALDIR \n")
        script.append("if [ -e $PRODAGENT_JOB_INITIALDIR/FrameworkJobReport.xml ]; then echo 1; else touch $PRODAGENT_JOB_INITIALDIR/FrameworkJobReport.xml; fi; ")
        
        handle = open(wrapperName, 'w')
        handle.writelines(script)
        handle.close()
        return
    

    def doSubmit(self, wrapperScript, jobTarball):
        """
        _doSubmit_

        Build and run a condor_submit command

        """
        jdlFile = "%s.jdl" % wrapperScript
        
        command = "condor_submit %s" % jdlFile
        logging.debug("OSGRouter.doSubmit: %s" % command)
        output = self.executeCommand(command)
        logging.info("OSGRouter.doSubmit: %s " % output)
        return


    def lookupGlobusScheduler(self):
        """
        _lookupGlobusScheduler_

        If a whitelist is supplied in the job spec instance,
        match it to a globus scheduler using the SENameToJobmanager map.

        If no whitelist is present, the standard OSG GlobusSubmitter is used,

        If a whitelist is present and no match can be made, an exception
        is thrown

        """
        logging.debug("lookupGlobusScheduler:")
        if len(self.parameters['Whitelist']) == 0:
            logging.debug("lookupGlobusScheduler:No Whitelist")
            return None
        
        #  //
        # // We have a list, get the first one that matches
        #//  NOTE: Need some better selection process if more that one site
        #  //   Can we make condor match from a list??
        # //
        #//
        seMap = self.pluginConfig['SENameToJobmanager']

        matchedJobMgr = None
        for sitePref in  self.parameters['Whitelist']:
            if sitePref not in seMap.keys():
                logging.debug("lookupGlobusScheduler: No match: %s" % sitePref)
                continue
            matchedJobMgr = seMap[sitePref]
            logging.debug("lookupGlobusScheduler: Matched: %s => %s" % (
                sitePref, matchedJobMgr  )
                          )
            break

        if matchedJobMgr == None:
            msg = "Unable to match site preferences: "
            msg += "\n%s\n" % self.parameters['Whitelist']
            msg += "To any JobManager"
            raise JSException(msg, 
                              ClassInstance = self,
                              SENameToJobmanager = seMap,
                              Whitelist = self.parameters['Whitelist'])
        return matchedJobMgr
                
            

registerSubmitter(OSGRouter, OSGRouter.__name__)
