#!/usr/bin/env python
"""
_CondorDirect_

Globus Universe Condor Submitter implementation.

Pulls site information from the ResourceControlDB instead of the
XML file.


"""

__revision__ = "$Id: CondorDirect.py,v 1.3 2007/08/18 16:05:18 dmason Exp $"

import os
import logging


from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.BulkSubmitterInterface import BulkSubmitterInterface
from JobSubmitter.JSException import JSException




from JobSubmitter.Submitters.CondorDirectUtils import makeOSGScript
from JobSubmitter.Submitters.CondorDirectUtils import makeLCGScript
from JobSubmitter.Submitters.CondorDirectUtils import missingJobReportCheck

import ProdAgent.ResourceControl.ResourceControlAPI as ResConAPI


class CondorDirect(BulkSubmitterInterface):
    """
    _CondorDirect_

    Generates a direct to CE JDL and submission wrapper that
    pulls the input sandboxes in through the squid proxies.
    Requires that the JobCreator JobCache is visible via HTTP
    

    """
    def doSubmit(self):
        """
        _doSubmit_

        Perform bulk or single submission as needed based on the class data
        populated by the component that is invoking this plugin
        """
        logging.debug("<<<<<<<<<<<<<<<<<CondorDirect>>>>>>>>>>>>>>..")
        logging.debug("%s" % self.primarySpecInstance.parameters)
        self.workflowName = self.primarySpecInstance.payload.workflow
        self.mainJobSpecName = self.primarySpecInstance.parameters['JobName']
        self.mainSandbox = \
                   self.primarySpecInstance.parameters['BulkInputSandbox']
        self.mainSandboxName = os.path.basename(self.mainSandbox)
        self.mainSandboxUrl = self.mainSandbox.split(self.workflowName, 1)[1]
        self.mainSandboxUrl = "%s/%s" % (
            self.workflowName, self.mainSandboxUrl)

        #  //
        # // Site details from ResCon
        #//
        #  // jobmanager is the globusscheduler
        # //  gridType is OSG or LCG
        #//   queueName is the RSL name of the queue (LCG sites)
        self.jobmanager = None
        self.gridType   = None
        self.queueName  = None
        self.lookupSite()
        
        #  //
        # // Build a list of input files for every job
        #//
        self.specSandboxName = None
        self.specSandboxUrl = None
        self.singleSpecName = None
        self.singleSpecUrl = None
        #  //
        # // For multiple bulk jobs there will be a tar of specs
        #//
        if self.primarySpecInstance.parameters.has_key('BulkInputSpecSandbox'):
            fullPath = \
                self.primarySpecInstance.parameters['BulkInputSpecSandbox']
            self.specSandboxName = os.path.basename(fullPath)
            self.specSandboxUrl = fullPath.split(
                self.workflowName, 1)[1]
            self.specSandboxUrl = "%s/%s" % (self.workflowName,
                                             self.specSandboxUrl)
        #  //
        # // For single jobs there will be just one job spec
        #//
        if not self.isBulk:
            fullPath = self.specFiles[self.mainJobSpecName]
            self.singleSpecName = os.path.basename(fullPath)
            self.singleSpecUrl = fullPath.split(
                self.workflowName, 1)[1]
            self.singleSpecUrl = "%s/%s" % (self.workflowName,
                                            self.singleSpecUrl)
            
        
            
        #  //
        # // Start the JDL for this batch of jobs
        #//
        self.jdl = self.initJDL()
        
        for jobSpec, cacheDir in self.toSubmit.items():
            logging.debug("Submit: %s from %s" % (jobSpec, cacheDir) )
            logging.debug("SpecFile = %s" % self.specFiles[jobSpec])
            #  //
            # // For each job to submit, generate a JDL entry
            #//
            self.jdl.extend(
                self.makeJobJDL(jobSpec, cacheDir, self.specFiles[jobSpec]))
            
            
        msg = ""
        for line in self.jdl:
            msg += line

        logging.debug(msg)

        jdlFile = "%s/submit.jdl" % self.toSubmit[self.mainJobSpecName]
        handle = open(jdlFile, 'w')
        handle.writelines(self.jdl)
        handle.close()

        logging.debug("jdl File = %s" % jdlFile)
        

        condorSubmit = "condor_submit %s" % jdlFile
        logging.debug("OSGSubmitter.doSubmit: %s" % condorSubmit)
        output = self.executeCommand(condorSubmit)
        logging.info("OSGSubmitter.doSubmit: %s " % output)
        return



    def initJDL(self):
        """
        _initJDL_

        Make common JDL header
        """        
        jdl = []
        jdl.append("universe = globus\n")
        jdl.append("globusscheduler = %s\n" % self.jobmanager)

        if self.gridType == "LCG":
            jdl.append("globusrsl=(queue=%s)\n" % self.queueName)
        else:
            jdl.append("transfer_output_files = FrameworkJobReport.xml\n")
            jdl.append("should_transfer_files = YES\n")
            jdl.append("when_to_transfer_output = ON_EXIT\n")
        jdl.append("log_xml = True\n" )
        jdl.append("notification = NEVER\n")
        jdl.append("+ProdAgent_ID = \"%s\"\n" % self.primarySpecInstance.parameters['ProdAgentName'])
        return jdl
    
        
    def makeJobJDL(self, jobID, cache, jobSpec):
        """
        _makeJobJDL_

        For a given job/cache/spec make a JDL fragment to submit the job

        """
        # -- scriptFile & Output/Error/Log filenames shortened to 
        #    avoid condorg submission errors from > 256 character pathnames
        scriptFile = "%s/submit.sh" % cache
        self.makeWrapperScript(scriptFile, jobID)
        logging.debug("Submit Script: %s" % scriptFile)
        
        jdl = []
        jdl.append("Executable = %s\n" % scriptFile)
        jdl.append("initialdir = %s\n" % cache)
        jdl.append("Output = condor.out\n")
        jdl.append("Error = condor.err\n")
        jdl.append("Log = condor.log\n")

        
        #  //
        # // Add in parameters that indicate prodagent job types etc
        #//
        jdl.append("+ProdAgent_JobID = \"%s\"\n" % jobID)
        jdl.append("+ProdAgent_JobType = \"%s\"\n" % self.primarySpecInstance.parameters['JobType'])

        jdl.append("Arguments = %s-JobSpec.xml \n" % jobID)
        jdl.append("Queue\n")
        
        return jdl
        

    def makeWrapperScript(self, filename, jobName):
        """
        _makeWrapperScript_

        Make the main executable script for condor to execute the
        job
        
        """
        
        #  //
        # // Generate main executable script for job
        #//
        if self.gridType == "LCG":
            if self.isBulk:
                script = makeLCGScript(jobName, self.workflowName,
                                       self.httpServer,
                                       self.mainSandboxUrl,
                                       InputJobTar = self.specSandboxUrl
                                       )
            else:
                script = makeLCGScript(jobName, self.workflowName,
                                       self.httpServer,
                                       self.mainSandboxUrl,
                                       InputJobSpec = self.singleSpecUrl)
                
            script.append("cd %s\n" % self.workflowName)
            script.append("./run.sh $JOB_SPEC_FILE 1>&2\n")
            script.extend(missingJobReportCheck())
            script.append("echo \"DUMPING JOB REPORT\"\n")
            script.append("echo \"===CUT HERE===\"\n")
            script.append("cat FrameworkJobReport.xml >&2 \n")
            script.append("cat FrameworkJobReport.xml\n")
            
        else:
            if self.isBulk:
                script = makeOSGScript(jobName, self.workflowName,
                                       self.httpServer,
                                       self.mainSandboxUrl,
                                       InputJobTar = self.specSandboxUrl
                                       )
            else:
                script = makeOSGScript(jobName, self.workflowName,
                                       self.httpServer,
                                       self.mainSandboxUrl,
                                       InputJobSpec = self.singleSpecUrl)


                
            script.append("cd %s\n" % self.workflowName)
            script.append("./run.sh $JOB_SPEC_FILE\n")
            script.append(
                "cp ./FrameworkJobReport.xml $PRODAGENT_JOB_INITIALDIR \n")
            script.extend(missingJobReportCheck())
        

        handle = open(filename, 'w')
        handle.writelines(script)
        handle.close()
        return
    
    
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
        # self.pluginConfig['OSG']['GlobusScheduler']
        if not self.pluginConfig.has_key("OSG"):
            msg = "Plugin Config for: %s \n" % self.__class__.__name__
            msg += "Does not contain an OSG config block"
            raise JSException( msg , ClassInstance = self,
                               PluginConfig = self.pluginConfig)

        #  //
        # // Validate the value of the GlobusScheduler is present
        #//  and sane
        sched = self.pluginConfig['OSG'].get("GlobusScheduler", None)
        if sched in (None, "None", "none", ""):
            msg = "Invalid value for OSG GlobusScheduler in Submitter "
            msg += "plugin configuration: %s\n" % sched
            msg += "You must provide a valid scheduler"
            raise JSException( msg , ClassInstance = self,
                               PluginConfig = self.pluginConfig)
          
        
        
        data = self.pluginConfig.get("HttpServer", {})
        httpAddress = data.get("HTTPFrontendURL", None)
        if httpAddress == None:
            msg = "HTTP Server URL to expose JobCache for CondorDirect\n"
            msg += "is not present in the Submitter plugin config\n"
            msg += " You must provide a valid URL for the Http Frontend\n"
            raise JSException( msg , ClassInstance = self,
                               PluginConfig = self.pluginConfig)
        self.httpServer = httpAddress
        logging.debug("HTTPFrontend is %s" % self.httpServer)
        return


    def lookupSite(self):
        """
        _lookupSite_

        If a whitelist is supplied in the job spec instance,
        match it to a globus scheduler using the SENameToJobmanager map.

        If no whitelist is present, the standard OSG GlobusSubmitter is used,

        If a whitelist is present and no match can be made, an exception
        is thrown

        """
        logging.debug("lookupSite")
        if len(self.whitelist) == 0:
            #  //
            # //  No Preference, use plain GlobusScheduler
            #//
            logging.debug("lookupSite:No Whitelist")
            msg = "No Whitelist provided for job & not default\n"
            msg += "system is implemented yetm cannot submit\n"
            raise RuntimeError(msg)
        
      
            
        ceMap = ResConAPI.createCEMap()
        siteIndexMap = ResConAPI.createSiteIndexMap()
        
        matchedSiteIndex = None
        matchedJobMgr = None
        for sitePref in self.whitelist:
            try:
                intSitePref = int(sitePref)
                sitePref = intSitePref
            except ValueError:
                sitePref = siteIndexMap.get(sitePref, None)

            if sitePref == None:
                msg = "Unable to translate site preference %s\n"
                msg += "into a known site index\n"
                msg += "Known site identifiers: %s\n" % siteIndexMap.keys()
                logging.debug(msg)
                continue
            
            if sitePref not in ceMap.keys():
                logging.debug("lookupGlobusScheduler: No match: %s" % sitePref)
                continue
            matchedJobMgr = ceMap[sitePref]
            matchedSiteIndex = sitePref
            logging.debug("lookupGlobusScheduler: Matched: %s => %s" % (
                sitePref, matchedJobMgr  )
                          )
            break

        
        
        if matchedJobMgr == None:
            msg = "Unable to match site preferences: "
            msg += "\n%s\n" % self.whitelist
            msg += "To any JobManager"
            raise JSException(msg, 
                              ClassInstance = self,
                              SENameToJobmanager = ceMap,
                              Whitelist = self.whitelist)

        self.jobmanager = matchedJobMgr
        siteAttrs = ResConAPI.attributesByIndex(matchedSiteIndex)
        self.gridType = siteAttrs.get("grid-type", "OSG")
        self.queueName = siteAttrs.get("queue-name", None)
        
        msg = "Looked up site for submission:\n"
        msg += "Jobmanager = %s\n" % self.jobmanager
        msg += "Grid Type  = %s\n" % self.gridType
        msg += "Queue Name = %s\n" % self.queueName
        logging.info(msg)
        
        return 
                
            

registerSubmitter(CondorDirect, CondorDirect.__name__)
