#!/usr/bin/env python
"""
_GlideInWMS_

GlideInWMS Submitter plugin

Pulls site information from the ResourceControlDB to match site IDs
to GlideIn desired sites. If no site provided, then plain vanilla submit
to the glide in factory and let it go anywhere.

Note that all merge and cleanup type jobs will have a single destination
site set. Processing jobs may be unrestricted in site or have a list
of sites where they need to go


"""

__revision__ = "$Id: GlideInWMS.py,v 1.11 2009/08/21 21:19:43 khahn Exp $"

import os
import logging
import time
import string
import re

import ShREEK.CMSPlugins.DashboardInfo as DashboardUtils

from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.BulkSubmitterInterface import BulkSubmitterInterface
from JobSubmitter.JSException import JSException

from JobSubmitter.Submitters.OSGUtils import standardScriptHeader
from JobSubmitter.Submitters.OSGUtils import bulkUnpackerScript
from JobSubmitter.Submitters.OSGUtils import missingJobReportCheck

from ProdAgent.ResourceControl.ResourceControlAPI import createSiteNameMap

from ResourceMonitor.Monitors.CondorQ import condorQ

from ProdCommon.MCPayloads.JobSpec import JobSpec



class GlideInWMS(BulkSubmitterInterface):
    """
    _GlideInWMS_

    Submitter that takes a job spec (single or bulk) and creates
    submission JDL for the GlideIn WMS.

    Site Data is looked up from the PA side in the ResCon DB.
    

    """
    def doSubmit(self):
        """
        _doSubmit_

        Perform bulk or single submission as needed based on the class data
        populated by the component that is invoking this plugin
        """

        logging.debug("%s" % self.primarySpecInstance.parameters)
        self.workflowName = self.primarySpecInstance.payload.workflow
        self.mainJobSpecName = self.primarySpecInstance.parameters['JobName']
        self.mainSandbox = \
                   self.primarySpecInstance.parameters['BulkInputSandbox']

        self.mainSandboxName = os.path.basename(self.mainSandbox)
        self.mainSandboxDir = os.path.dirname(self.mainSandbox)
        self.mainSandboxLink = os.path.join(self.mainSandboxDir,
                                            "SandBoxLink.tar.gz")   
        # this is a workaround to avoid submission failures for pathnames
        # greater than 256 characters.  
        if not os.path.exists(self.mainSandboxLink):
           linkcommand = "ln -s %s %s" % (self.mainSandbox,
                                          self.mainSandboxLink)
           logging.debug("making link to sandbox: %s"%linkcommand)
           os.system(linkcommand)
        # then check if the link exists already, if not, make it and 
        # instead insert that...
        # do same with jobspecs...
        
        
        logging.debug("mainSandbox: %s" % self.mainSandbox)
        logging.debug("mainSandboxLink: %s" % self.mainSandboxLink)
        self.specSandboxName = None
        self.singleSpecName = None
        #  //
        # // Build a list of input files for every job
        #//
        self.jobInputFiles = []
        self.jobInputFiles.append(self.mainSandboxLink)
        
        #  //
        # // For multiple bulk jobs there will be a tar of specs
        #//
        if self.primarySpecInstance.parameters.has_key('BulkInputSpecSandbox'):
            self.specSandboxName = os.path.basename(
                self.primarySpecInstance.parameters['BulkInputSpecSandbox']
                )
            self.specSandboxDir = os.path.dirname(
                self.primarySpecInstance.parameters['BulkInputSpecSandbox']
                )
            self.jsLinkFileName = "JobSpecLink.%s.tar.gz" % int(time.time())
            self.specSandboxLink = os.path.join(
                self.specSandboxDir,self.jsLinkFileName)
            logging.debug("specSandboxLink: %s"% self.specSandboxLink)   
            linkcommand = "ln -s %s %s" % (
                self.primarySpecInstance.parameters['BulkInputSpecSandbox'],
                self.specSandboxLink)
            logging.debug("making link to jobspec: %s"%linkcommand)
            os.system(linkcommand)
            self.jobInputFiles.append(self.specSandboxLink)
            logging.debug("InputSpecSandbox: %s" % self.specSandboxLink) 
        #  //
        # // For single jobs there will be just one job spec
        #//
        if not self.isBulk:

            self.specSandboxDir = os.path.dirname(
                self.specFiles[self.mainJobSpecName])
            self.jsLinkFileName="JobSpecLink.xml" 
            self.specSandboxLink = os.path.join(self.specSandboxDir,
                                                self.jsLinkFileName)
            logging.debug("specSandboxLink: %s"% self.specSandboxLink)
            linkcommand="ln -s %s %s" % (
                self.specFiles[self.mainJobSpecName],self.specSandboxLink)
            logging.debug("making link to jobspec: %s"%linkcommand)
            os.system(linkcommand)
            self.jobInputFiles.append(self.specSandboxLink)

            self.singleSpecName = os.path.basename(
                self.specFiles[self.mainJobSpecName])
            
        #  //
        # // Start the JDL for this batch of jobs
        #//
        self.jdl = self.initJDL()
        submittedJobSpecs = []
        failureList = []
        for jobSpec, cacheDir in self.toSubmit.items():
            logging.debug("Submit: %s from %s" % (jobSpec, cacheDir) )
            logging.debug("SpecFile = %s" % self.specFiles[jobSpec])
            #  //
            # // For each job to submit, generate a JDL entry
            #//
            self.jdl.extend(
                self.makeJobJDL(jobSpec, cacheDir, self.specFiles[jobSpec]))
            # Storing jobSpecs in a list for ensure we keep te same order
            submittedJobSpecs.append(jobSpec) 
            
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
        logging.debug("GlideInWMS.doSubmit: %s" % condorSubmit)
        logging.debug("hacked to get around condor submitt failures.. jen")

        try:
           output = self.executeCommand(condorSubmit)
           logging.info("GlideInWMS.doSubmit: %s " % output)
        except RuntimeError, err:
           # hopefully when condor_submit fails on bulk submission all jobs fail
           for jobSpec, cacheDir in self.toSubmit.items():
               #logging.debug("... appending to failure list %s" % jobSpec )
               failureList.append(jobSpec)

        if len(failureList) > 0:
            raise JSException("Submission Failed", FailureList = failureList)
            #logging.debug("---> FailureList size = %s " % len(failureList))

        # For Dashboard reporting we are relying on the fact that the
        # condor_submit reports the cluster ids in the same order the jobs are
        # in the jdl file

        #  Reg. expr. for finding the cluster IDs from output
        reg_cluster = re.compile(r'.*cluster\s+(?P<clusterID>\d+)\W')
        clusterIds = reg_cluster.findall(output)
        if len(clusterIds) != len(submittedJobSpecs):
            msg = "Could not retrieve Cluster Ids."
            msg = " Mismatch between jobs submitted and"
            msg += " jobs reported by condor_submit."
            logging.info(msg)
        else:
            self.submittedJobs = {}
            for jobSpec, clusterId in zip(submittedJobSpecs, clusterIds):
                self.submittedJobs[jobSpec] = clusterId
                       
        return



    def initJDL(self):
        """
        _initJDL_

        Make common JDL header
        """
        self.desiredSites = self.lookupDesiredSites()
        
        inpFiles = []

        inpFiles.extend(self.jobInputFiles)
        inpFileJDL = ""
        for f in inpFiles:
            inpFileJDL += "%s," % f
        inpFileJDL = inpFileJDL[:-1]

        jdl = []
        jdl.append("universe = vanilla\n")
        # Specify where are the jobs to run
        jdl.append('+DESIRED_Archs="INTEL,X86_64"\n')
        if self.desiredSites!=None:
            desiredSitesJDL = string.join(list(self.desiredSites),',')
            jdl.append('+DESIRED_Sites = "%s"\n' % desiredSitesJDL)
            jdl.append("Requirements = stringListMember(GLIDEIN_Site,DESIRED_Sites)&& stringListMember(Arch, DESIRED_Archs)\n")
        else:
            jdl.append("Requirements = stringListMember(Arch, DESIRED_Archs)\n")
            
        # log which glidein ran the job
        jdl.append('+JOB_Site = "$$(GLIDEIN_Site:Unknown)"\n')
        jdl.append('+JOB_GLIDEIN_Factory = "$$(GLIDEIN_Factory:Unknown)"\n')
        jdl.append('+JOB_GLIDEIN_Name = "$$(GLIDEIN_Name:Unknown)"\n')
        jdl.append('+JOB_GLIDEIN_Schedd = "$$(GLIDEIN_Schedd:Unknown)"\n')
        jdl.append('+JOB_GLIDEIN_ClusterId = "$$(GLIDEIN_ClusterId:Unknown)"\n')
        jdl.append('+JOB_GLIDEIN_ProcId = "$$(GLIDEIN_ProcId:Unknown)"\n')
        jdl.append('+JOB_GLIDEIN_Frontend = "$$(GLIDEIN_Client:Unknown)"\n')
        jdl.append('+JOB_GLIDEIN_Gatekeeper = "$$(GLIDEIN_Gatekeeper:Unknown)"\n')
        jdl.append('environment = CONDOR_JOBID=$$([GlobalJobId])\n')
        jdl.append('+JOB_Slot = "$$(Name:Unknown)"\n')

        # log glidein benchmark numbers
        jdl.append('+JOB_Machine_KFlops = "$$(KFlops:Unknown)"\n')
        jdl.append('+JOB_Machine_Mips = "$$(Mips:Unknown)"\n')

        jdl.append("transfer_input_files = %s\n" % inpFileJDL)
        jdl.append("transfer_output_files = FrameworkJobReport.xml\n")
        jdl.append("should_transfer_files = YES\n")
        jdl.append("when_to_transfer_output = ON_EXIT\n")
        jdl.append("log_xml = True\n" )
        jdl.append("notification = NEVER\n")
        jdl.append(
            "+ProdAgent_ID = \"%s\"\n" % (
            self.primarySpecInstance.parameters['ProdAgentName'],))

      
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
        # // Add in parameters that indicate prodagent job types, priority etc
        #//
        jdl.append("+ProdAgent_JobID = \"%s\"\n" % jobID)
        jdl.append("+ProdAgent_JobType = \"%s\"\n" % self.primarySpecInstance.parameters['JobType'])

	if self.primarySpecInstance.parameters['JobType'].lower() == "merge":
            jdl.append("priority = 10\n")

        if self.primarySpecInstance.parameters['JobType'].lower() == "cleanup":
            jdl.append("priority = 5\n")

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
        script = ["#!/bin/sh\n"]

        placeholderScript = \
"""
echo '<FrameworkJobReport JobSpecID=\"%s\" Name=\"cmsRun1\" WorkflowSpecID=\"%s\" Status=\"Failed\">' > FrameworkJobReport.xml
echo ' <ExitCode Value=\"60998\"/>' >> FrameworkJobReport.xml
echo ' <FrameworkError ExitStatus=\"60998\" Type=\"ErrorBootstrappingJob\">' >> FrameworkJobReport.xml
echo "   hostname=`hostname -f` " >> FrameworkJobReport.xml
echo "   site=$OSG_SITE_NAME " >> FrameworkJobReport.xml
echo " </FrameworkError>"  >> FrameworkJobReport.xml
echo "</FrameworkJobReport>" >> FrameworkJobReport.xml
""" % (jobName, self.workflowName)
        script.append(placeholderScript)

        script.extend(standardScriptHeader(jobName, self.workflowName))
        
        spec_file="$PRODAGENT_JOB_INITIALDIR/%s" % self.jsLinkFileName
        if self.isBulk:
            script.extend(bulkUnpackerScript(spec_file))
        else:
            script.append("JOB_SPEC_FILE=%s\n" %spec_file)
            
        script.append(
             "tar -zxf $PRODAGENT_JOB_INITIALDIR/%s\n" % "SandBoxLink.tar.gz"
            )
        script.append("cd %s\n" % self.workflowName)
        
        
                      
        script.append("./run.sh $JOB_SPEC_FILE\n")
        script.append(
            "cp ./FrameworkJobReport.xml $PRODAGENT_JOB_INITIALDIR \n")
        script.extend(missingJobReportCheck(jobName))


        handle = open(filename, 'w')
        handle.writelines(script)
        handle.close()
        return
    
    
    def checkPluginConfig(self):
        """
        _checkPluginConfig_

        Make sure config has what is required for this submitter (if anything)
        
        """
        if self.pluginConfig == None:
            msg = "Failed to load Plugin Config for:\n"
            msg += self.__class__.__name__
            raise JSException( msg, ClassInstance = self)
                             
        
   
     




    def lookupDesiredSites(self):
        """
        _lookupDesiredSites_

        If a whitelist is supplied in the job spec instance,
        match it to a desired site value (ce name) in the rescon DB

        If no whitelist is present, return None to trigger a vanilla
        submission

        If a whitelist is present and no match can be made, an exception
        is thrown

        """
        logging.debug("lockupDesiredSites:")
        if len(self.whitelist) == 0:
            #  //
            # //  No Preference, return None to trigger vanilla
            #//   submission
            logging.debug("lockupDesiredSites:No Whitelist")
            return None
      
  
        
        #  //
        # // Map to Site Name (desired sites attr) in ResCon DB
        #//
        siteMap = createSiteNameMap()

        #  //
        # // Match a list of desired sites
        #//
        matchedSites = set()
        for sitePref in self.whitelist:
            try:
                intSitePref = int(sitePref)
                sitePref = intSitePref
            except ValueError:
                pass
                
            if sitePref not in siteMap.keys():
                logging.debug("lockupDesiredSites: No match: %s" % sitePref)
                continue
            matchedSites.add(siteMap[sitePref])
            logging.debug("lockupDesiredSites: Matched: %s => %s" % (
                sitePref, matchedSites  )
                          )
            break

        if len(matchedSites) == 0:
            msg = "Unable to match site preferences: "
            msg += "\n%s\n" % self.whitelist
            msg += "To any Desired site in ResConDB"
            raise JSException(msg, 
                              ClassInstance = self,
                              Whitelist = self.whitelist)
        return matchedSites


    def publishSubmitToDashboard(self):
        """
        _publishSubmitToDashboard_

        Publish the dashboard info to the appropriate destination
            
        NOTE: should probably read destination from cfg file, hardcoding
        it here for time being.
                
        """
        #  // 
        # // Check for dashboard usage
        #//
        self.usingDashboard = {'use' : 'True',
                               'address' : 'cms-pamon.cern.ch',
                               'port' : 8884}
        try:
            dashboardCfg = self.pluginConfig.get('Dashboard', {})
            self.usingDashboard['use'] = dashboardCfg.get(
                "UseDashboard", "False")
            self.usingDashboard['address'] = dashboardCfg.get(
                "DestinationHost")
            self.usingDashboard['port'] = int(dashboardCfg.get(
                "DestinationPort"))
            logging.debug("dashboardCfg = " + str(self.usingDashboard) )
        except:
            logging.info("No Dashboard section in SubmitterPluginConfig")
            logging.info("Taking default values:")
            logging.info("dashboardCfg = " + str(self.usingDashboard))
            
        if self.usingDashboard['use'].lower().strip() == "false":
            logging.info("Skipping Dasboard report.")
            return

        # Preliminary details
        appData = ",".join(self.applicationVersions)
        targetCE = ", ".join(list(self.desiredSites))

        # Storing GlobalJobIds
        try:
            classads = condorQ("\"ProdAgent_JobID =!= UNDEFINED\"")
        except:
            classads = {}

        # Getting ProdAgent Version
        paVersion = os.environ.get('PRODAGENT_VERSION', '')
        if not paVersion and os.environ.get('PAVERSION', ''):
            paVersion = 'PRODAGENT_%s' % os.environ.get('PAVERSION')

        # Composing dashboard information for each submitted job
        for jobSpecId, jobCache in self.toSubmit.items():
            jobSpecFile = self.specFiles[jobSpecId]
            # Local Batch Id
            clusterId = self.submittedJobs.get(jobSpecId)
            # Finding GlobalJobId
            globalJobId = ""
            submissionTime = ""
            for classad in classads:
                if classad['ProdAgent_JobID'] == jobSpecId:
                    globalJobId = classad.get('GlobalJobId', "")
                    submissionTime = classad.get('QDate', "")
                    break
            # Finding executable
            try:
                jobSpec = JobSpec()
                jobSpec.load(jobSpecFile)
                executable = jobSpec.payload.application.get('Executable', '')
            except:
                executable = ''
            dashInfoFile = os.path.join(jobCache, "DashboardInfo.xml")
            if not os.path.exists(dashInfoFile):
                msg = "Dashboard Info file not found\n"
                msg += "%s\n" % dashInfoFile
                msg += "Skipping publication for %s\n" % jobId
                logging.warning(msg)
                continue
            dashData = DashboardUtils.DashboardInfo()
            dashData.read(dashInfoFile)
            dashData.task, dashData.job = \
                           self.generateDashboardID(jobSpecFile, globalJobId)

            dashData['ApplicationVersion'] = appData
            dashData['TargetCE'] = targetCE
            dashData['Scheduler'] = 'GLIDEINS'
            dashData['JSToolUI'] = os.environ['HOSTNAME']
            dashData['GridJobID'] = globalJobId
            dashData['SubTimeStamp'] = submissionTime
            dashData['RBname'] = os.environ.get('HOSTNAME', 'ProdAgent')
            dashData['LocalBatchID'] = clusterId
            dashData['JSToolVersion'] = paVersion
            dashData['Executable'] = executable

#            # Number of steps (only for processing Jobs)
#            typeList = []
#            getType = lambda x: typeList.append(x.type)
#            jobSpec.payload.operate(getType)
#            dashData['NSteps'] = typeList.count('CMSSW')

            dashData.addDestination(self.usingDashboard['address'],
                int(self.usingDashboard['port']))

            logging.debug("Information published in Dashboard:")
            msg = "\n - task: %s\n - job: %s" % (dashData.task, dashData.job)
            for key, value in dashData.items():
                msg += "\n - %s: %s" % (key, value)
            logging.debug(msg)

            # update dashboard info file
            try:
                dashData.write(dashInfoFile)
                logging.info("Updated dashboardInfoFile " + dashInfoFile)
            except Exception, ex:
                logging.error("Error writing %s" % dashInfoFile)

            # Publish to Dashboard
            dashData.publish(1)

        return


    def generateDashboardID(self, jobSpecFile, globalJobId):
        """
        _generateDashboardID_

        Generate a global job ID for the dashboard

        """
        jobSpec = JobSpec()
        try:
            jobSpec.load(jobSpecFile)
        except Exception, ex:
            msg = "Error loading JobSpec File: %s\n" % jobSpecFile
            msg += str(ex)
            logging.debug(msg)
            return (None, None)

        prodAgentName = jobSpec.parameters['ProdAgentName']
        jobSpecId = jobSpec.parameters['JobName']
        jobName = jobSpecId.replace("_", "-")    
        jobName = "ProdAgent_%s_%s" %(
            prodAgentName,
            jobName,
            )

        workflowId = jobSpec.payload.workflow
        workflowId = workflowId.replace("_", "-")
        taskName = "ProdAgent_%s_%s" % (workflowId,
                                         prodAgentName)
        subCount = jobSpec.parameters.get('SubmissionCount', 0)
        jobName = "%s_%s" % (jobName, subCount)
        jobName = "_".join([jobName, globalJobId])
    
        return taskName, jobName


registerSubmitter(GlideInWMS, GlideInWMS.__name__)
