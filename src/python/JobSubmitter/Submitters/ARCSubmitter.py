#!/usr/bin/env python
"""
_ARCSubmitter_

Submitter for ARC submissions


"""
import os
import re
import logging
import string
import time
import random

from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.BulkSubmitterInterface import BulkSubmitterInterface
from JobSubmitter.JSException import JSException
from ProdAgent.Resources import ARC
from ProdAgentCore.PluginConfiguration import loadPluginConfig

import ProdAgent.ResourceControl.ResourceControlAPI as ResConAPI
import ShREEK.CMSPlugins.DashboardInfo  as DashboardUtils

bulkUnpackerScriptMain = \
"""

echo "===Available JobSpecs:==="
/bin/ls `pwd`/BulkSpecs
echo "========================="


JOB_SPEC_FILE="`pwd`/BulkSpecs/$JOB_SPEC_NAME"

PROCEED_WITH_SPEC=0

if [ -e "$JOB_SPEC_FILE" ]; then
   echo "Found Job Spec File: $JOB_SPEC_FILE"
   PROCEED_WITH_SPEC=1
else
   echo "Job Spec File Not Found: $JOB_SPEC_NAME"
   PROCEED_WITH_SPEC=0
fi

if [ $PROCEED_WITH_SPEC != 1 ]; then
   echo "Unable to proceed without JobSpec File"
   echo '<FrameworkJobReport Name="$JOB_SPEC_NAME" Status="Failed">' > FrameworkJobReport.xml
   echo '<ExitCode Value="60998"/>' >> FrameworkJobReport.xml
   echo '<FrameworkError ExitStatus="60998" Type="MissingJobSpecFile">' >> FrameworkJobReport.xml
   echo "  hostname=`hostname -f` " >> FrameworkJobReport.xml
   echo "  jobspecfile=$JOB_SPEC_FILE " >> FrameworkJobReport.xml
   echo "  available_specs=`/bin/ls ./BulkSpecs` " >> FrameworkJobReport.xml
   echo "</FrameworkError>"  >> FrameworkJobReport.xml
   echo "</FrameworkJobReport>" >> FrameworkJobReport.xml
   exit 60998
fi

"""


def bulkUnpackerScript(bulkSpecTarName):
    """
    _bulkUnpackerScript_

    Unpacks bulk spec tarfile, searches for required spec passed to
    script as argument $1

    If file not found, it generates a failure report and exits
    Otherwise, JOB_SPEC_FILE will be set to point to the script
    to invoke the run.sh command
    
    """
    lines = [
        "JOB_SPEC_NAME=$1\n", 
        "BULK_SPEC_NAME=\"%s\"\n" % bulkSpecTarName,
        "echo \"This Job Using Spec: $JOB_SPEC_NAME\"\n",
        "tar -zxf $BULK_SPEC_NAME\n",
        ]
    lines.append(bulkUnpackerScriptMain)
    return lines


class ARCSubmitter(BulkSubmitterInterface):
    """
    _ARCSubmitter_

    Submitter Plugin to submit jobs to sites running NorduGrid/ARC.

    """

    def __init__(self):
        BulkSubmitterInterface.__init__(self)
        self.jobIdCEMap = {}
        self.subTime = {}
        self.gridJobId = {}


    def checkPluginConfig(self):
        """
        _checkPluginConfig_

        Make sure config has what is required for this submitter

        """
        if self.pluginConfig == None:
            msg = "Creator Plugin Config could not be loaded for:\n"
            msg += self.__class__.__name__
            raise JSException(msg, ClassInstance = self)

        if not self.pluginConfig.has_key("ARC"):
            setup = self.pluginConfig.newBlock("ARC")
            setup['CmsRunLogDir'] = "None"
            setup['NodeType'] = "None"

        return

    
    def doSubmit(self):
        """
        _doSubmit_

        Main method to generate job submission.

        Attributes in Base Class BulkSubmitterInterface are populated with
        details of what to submit in terms of the job spec and
        details contained therein

        """
        logging.debug("<<<<<<<<<<<<ARCSubmitter>>>>>>>>>>")

        self.workflowName = self.primarySpecInstance.payload.workflow
        self.mainJobSpecName = self.primarySpecInstance.parameters['JobName']
        self.mainSandbox = \
                   self.primarySpecInstance.parameters.get('BulkInputSandbox',
                                                            None)
        self.mainSandboxName = os.path.basename(self.mainSandbox)
        self.specSandboxName = None
        self.singleSpecName = None

        # Build a list of input files for every job
        self.jobInputFiles = []
        self.jobInputFiles.append(self.mainSandbox)
        
        # For multiple bulk jobs there will be a tar of specs
        if self.primarySpecInstance.parameters.has_key('BulkInputSpecSandbox'):
            self.specSandboxName = os.path.basename(
                self.primarySpecInstance.parameters['BulkInputSpecSandbox']
                )
            self.jobInputFiles.append(
                self.primarySpecInstance.parameters['BulkInputSpecSandbox'])

        # For single jobs there will be just one job spec
        if not self.isBulk:
            self.jobInputFiles.append(self.specFiles[self.mainJobSpecName])
            self.singleSpecName = os.path.basename(
                self.specFiles[self.mainJobSpecName])

        
        # So now we have a list of input files for each job
        # which have to be available to the job at runtime.
        # So we may need some copy operation here to a drop
        # box area visible on the WNs etc.

        # We have a list of job IDs to submit.  If this is a single job,
        # there will be just one entry. If it is a bulk submit, there will
        # be multiple entries, plus self.isBulk will be True (?)
        failureList = []
        subRe = re.compile("Job submitted with jobid: +(\w+://([a-zA-Z0-9.-]+)(:\d+)?(/.*)?/\d+)")
        for jobId, cacheDir in self.toSubmit.items():
            logging.debug("Submit: %s from %s" % (jobId, cacheDir) )
            logging.debug("SpecFile = %s" % self.specFiles[jobId])
            self.makeWrapperScript(cacheDir, "submit.sh", jobId)

            t = time.time()
            self.subTime[jobId] = int(t)

            submitCommand = "ngsub -e '"
            submitCommand += self.xrslCode(cacheDir, "submit.sh", jobId)
            submitCommand += "'"
            try:
                logging.debug("Submission command: %s" % submitCommand)
                output = ARC.executeCommand(submitCommand)
                logging.debug("Submission command output: %s " % output)

                # For Dashboard, we'll need unique a job id ('GridJobId' in
                # Dashboard) of the form Can URL.
                # We'll use the ARC job ID
                m = re.match(subRe, output)
                if not m:
                    raise ARC.CommandExecutionError("Unexpected output og ngsub command: %s" % output)
                self.gridJobId[jobId] = m.group(1)
                logging.info("self.gridJobId[jobId] = %s for jobId = %s" % (self.gridJobId[jobId], jobId))

            except ARC.CommandExecutionError, s:
                msg = "Submitting with command\n"
                msg += "'%s'\n" % submitCommand
                msg += "failed with exit status %s" % str(s)
                logging.warning(msg)
                failureList.append(jobId)

            logging.info("%s submitted with GridJobId %s" % (jobId,
                                                         self.gridJobId[jobId]))

        if len(failureList) > 0:
            raise JSException("Submission Failed", FailureList = failureList)


    def getSite(self):
        """
        Return tuple of name and CE of preferred site, if such exist,
        or randomly chosen otherwise.

        """
        if self.parameters['JobSpecInstance'].siteWhitelist:
            choice = "preferred"

            prefSite = self.parameters['JobSpecInstance'].siteWhitelist[0]
            logging.debug("Site %s whitelisted" % str(prefSite))
            nameMap = ResConAPI.createSiteNameMap()

            if prefSite in nameMap.keys():
                siteName = nameMap[prefSite]
            elif str(prefSite).isdigit() and long(prefSite) in nameMap.keys():
                # prefSite should be in nameMap.keys(), but if it isn't, it's
                # possible that it's an index (number) given as a string
                # (should be long).
                siteName = nameMap[long(prefSite)]
            else:
                msg = "WARNING: Preferred site %s unknown!" % str(prefSite)
                logging.warning(msg)
                for k in nameMap.keys():
                    logging.debug("nameMap[%s] = %s" % (k, nameMap[k]))
                return (None, None)

            ceMap = ResConAPI.createCEMap()
            ceName = ceMap.get(siteName, None)  
        else:
            choice = "randomly chosen"
            logging.debug("No preferred site; choosing a site at random")

            siteList = ResConAPI.activeSiteData()
            if not siteList:
                logging.warning("No active sites found!")
                return (None, None)
            site = random.choice(siteList)
            siteName = site['SiteName']
            ceName = site['CEName']

        logging.info("Using %s site %s with CE %s" % (choice, siteName,
                                                      str(ceName)))
        return (siteName, ceName)



    def xrslCode(self, scriptDir, scriptName, jobId):
        """
        _xrslCode_

        Produce the Xrsl code needed to submit the job to ARC.

        'scriptDir' is the directory of the local system where the wrapper
        script to be executed resides, and 'scriptName' it's basename.

        """
        code = "&(executable=%s)" % scriptName

        # Input files to submit with the job
        scriptPath = os.path.join(scriptDir, scriptName)
        code += "(inputFiles="
        code += "(%s %s)" % (scriptName, scriptPath)
        for fname in self.jobInputFiles:
            code += "(%s %s)" % (os.path.basename(fname), fname)
        code += ")"

        # Output files; leave everything at the CE until explicitely
        # retrieved/removed by ngget/ngclean. (Needed mainly for
        # debugging.)
        code += '(outputFiles=("/" ""))'

        # Choose an ScramArch runtime env.
        creatorPluginConfig = loadPluginConfig("JobCreator", "Creator")
        if creatorPluginConfig['SoftwareSetup'].has_key('ScramArch'):
            rte = "(runTimeEnvironment=VO-cms-%s)" % creatorPluginConfig['SoftwareSetup']['ScramArch']
            code += rte
            logging.debug("Added '%s' to xRSL code" % rte)
        else:
            logging.warning("No ScramArch!")

        # Choose CMSSW runtime env.
        if self.applicationVersions:
            for v in self.applicationVersions:
                w = re.sub("_", "-", v, 1)      # CMSSW_X_Y_Z  ->  CMSSW-X_Y_Z
                w = re.sub("_", ".", w)         #              ->  CMSSW-X.Y.Z
                rte = "(runTimeEnvironment=APPS/HEP/%s)" % w.upper()
                code += rte
                logging.debug("Added '%s' to xRSL code" % rte)
        else:
            logging.warning("No applicationVersions!")

        code += "(jobName=%s)" % jobId
        code += "(stdout=output)(stderr=errors)(gmlog=gridlog)"

        envVars = ""
        site, ce = self.getSite()
        if ce:
            code += "(cluster=%s)" % ce
            envVars += "(NORDUGRID_CE %s)" % ce
            self.jobIdCEMap[jobId] = ce

        code += "(environment=%s)" % envVars

        # Set wallTime to arbitrary large value, assumed to be enough for all jobs
        code += '(wallTime="2 days")'

        return code


    def makeWrapperScript(self, scriptDir, scriptName, jobId):
        """
        _makeWrapperScript_

        Make the main executable script for ARC to execute the
        job
        
        """
        # Generate main executable script for job
        script = ["#!/bin/sh\n"]

        # Some code useful for debugging
        script.append("ulimit -a\n")
        script.append("echo pwd: `pwd`\n")
        script.append("ls -la\n")
        script.append("export\n")
        script.append("python -V 2>&1\n")
        script.append("echo PYTHONPATH: $PYTHONPATH\n")

        script.append("export PRODAGENT_JOB_INITIALDIR=`pwd`\n")
        script.append("cd $TMPDIR\n")

        if self.isBulk:
            script.extend(bulkUnpackerScript(self.specSandboxName))
        else:
            script.append("JOB_SPEC_FILE=$PRODAGENT_JOB_INITIALDIR/%s\n" %
                          self.singleSpecName)   
            
        script.append("tar -zxvf $PRODAGENT_JOB_INITIALDIR/%s\n" % self.mainSandboxName)
        script.append("cd %s\n" % self.workflowName)
        script.append("./run.sh $JOB_SPEC_FILE > ./run.log 2>&1 \n")


        # ARCTracker.py will retrieve FrameworkJobReport and run.log from
        # $PRODAGENT_JOB_INITIALDIR.
        script.append("cd $PRODAGENT_JOB_INITIALDIR\n")
        script.append("cp $TMPDIR/%s/FrameworkJobReport.xml ./\n" % self.workflowName)
        script.append("cp $TMPDIR/%s/run.log ./\n" % self.workflowName)

        # If we, in a debug session, want to "ngget" the (entire) job, we
        # don't want huge number of "extra" files around; let's remove the
        # certificates directory:
        script.append("rm -rf certificates\n")

        scriptPath = os.path.join(scriptDir, scriptName)
        logging.debug("Writing script to '%s'" % scriptPath)
        handle = open(scriptPath, 'w')
        handle.writelines(script)
        handle.close()

        return
    

    def publishSubmitToDashboard(self):
        """
        _publishSubmitToDashboard_

        Publish the dashboard info to the appropriate destination

        """
        dashboardCfg = self.pluginConfig.get('Dashboard', {})
        useDashboard = dashboardCfg.get("UseDashboard", "False")

        if not useDashboard: return

        appData = str(self.applicationVersions)
        appData = appData.replace("[", "")
        appData = appData.replace("]", "")
        whitelist = str(self.whitelist)
        whitelist = whitelist.replace("[", "")
        whitelist = whitelist.replace("]", "")
        
        for jobId, jobCache in self.toSubmit.items():
            jobSpec = self.specFiles[jobId]
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
                           DashboardUtils.extractDashboardID(jobSpec)
            
            dashData['ApplicationVersion'] = appData
            dashData['TargetCE'] = self.jobIdCEMap.get(jobId, whitelist)
            t = time.gmtime(self.subTime[jobId])
            dashData['SubTimeStamp'] = time.strftime('%Y-%m-%d %H:%M:%S', t)
            dashData['GridJobID'] = self.gridJobId[jobId]
            dashData['JSToolUI'] = os.environ['HOSTNAME']
            dashData['Scheduler'] = self.__class__.__name__

            DashboardAddress = dashboardCfg.get("DestinationHost")
            DashboardPort=dashboardCfg.get("DestinationPort")
            dashData.addDestination(DashboardAddress, int(DashboardPort))
            dashData.publish(5)
            logging.debug("Dashboard data for %s: %s" % (jobId, str(dashData)))
        return
    
    
registerSubmitter(ARCSubmitter, ARCSubmitter.__name__)
