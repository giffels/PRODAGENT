#!/usr/bin/env python
"""
_ARCSubmitter_

Submitter for ARC submissions


"""
import os
import logging
import string

from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.BulkSubmitterInterface import BulkSubmitterInterface
from JobSubmitter.JSException import JSException
from ProdAgent.Resources import ARC

import ProdAgent.ResourceControl.ResourceControlAPI as ResConAPI

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
            setup['Queue'] = "8nh"
            setup['LsfLogDir'] = "None"
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
        #  //
        # // Build a list of input files for every job
        #//
        self.jobInputFiles = []
        self.jobInputFiles.append(self.mainSandbox)
        
        #  //
        # // For multiple bulk jobs there will be a tar of specs
        #//
        if self.primarySpecInstance.parameters.has_key('BulkInputSpecSandbox'):
            self.specSandboxName = os.path.basename(
                self.primarySpecInstance.parameters['BulkInputSpecSandbox']
                )
            self.jobInputFiles.append(
                self.primarySpecInstance.parameters['BulkInputSpecSandbox'])

        #  //
        # // For single jobs there will be just one job spec
        #//
        if not self.isBulk:
            self.jobInputFiles.append(self.specFiles[self.mainJobSpecName])
            self.singleSpecName = os.path.basename(
                self.specFiles[self.mainJobSpecName])

        
        #  // So now we have a list of input files for each job
        # //  which have to be available to the job at runtime.
        #//   So we may need some copy operation here to a drop
        #  // box area visible on the WNs etc.
        # //
        #//

        #  //
        # // We have a list of job IDs to submit.
        #//  If this is a single job, there will be just one entry
        #  //If it is a bulk submit, there will be multiple entries,
        # // plus self.isBulk will be True
        #//
        for jobSpec, cacheDir in self.toSubmit.items():
            logging.debug("Submit: %s from %s" % (jobSpec, cacheDir) )
            logging.debug("SpecFile = %s" % self.specFiles[jobSpec])
            self.makeWrapperScript(os.path.join(cacheDir,"submit.sh"),
                                   jobSpec,cacheDir)

        #  //
        # // Submit ARC job
        #//
        submitCommand = "ngsub -e '%s'" % \
                               self.xrslCode(os.path.join(cacheDir,"submit.sh"),
                                             jobSpec, cacheDir)

        submitCommand += self.preferredSite()

        logging.debug("ARCSubmitter.doSubmit: %s" % submitCommand)
        try:
            output = ARC.executeNgCommand(submitCommand)
        except CommandExecutionError, emsg:
            msg = "Submitting with command\n"
            msg += "    '%s'\n" % submitCommand
            msg += "failed: " + str(emsg)
            logging.warning(msg)
        logging.debug("ARCSubmitter.doSubmit: %s " % output)


    def preferredSite(self):
        """
        Generate command line option for ngsub for submitting to a
        preferred site, if such exist.

        """
        if not self.parameters['JobSpecInstance'].siteWhitelist:
           logging.debug("No preferred site")
           return ""

        prefSite = self.parameters['JobSpecInstance'].siteWhitelist[0]
        logging.debug("Site %s whitelisted" % prefSite)
        ceMap = ResConAPI.createCEMap()

        if prefSite in ceMap.keys():
            logging.info("Using preferred CE " + ceMap[prefSite])
            return " -c " + ceMap[prefSite]

        # If prefSite wasn't in ceMap.keys(), it's possible that it's an
        # index given as a string (should be long).
        elif str(prefSite).isdigit() and long(prefSite) in ceMap.keys():
            logging.info("Using preferred CE " + ceMap[long(prefSite)])
            return " -c " + ceMap[long(prefSite)]

        else:
            logging.warning("WARNING: Preferred site %s unknown!" % str(prefSite))
            for k in ceMap.keys():
                logging.debug("ceMap[%s] = %s" % (k, ceMap[k]))
            return ""


    def xrslCode(self, wrapperscript, jobName, cacheDir):
        """
        _xrslCode_

        Produce the Xrsl code needed to submit the job to ARC

        """
        code = "&(executable=%s)" % os.path.basename(wrapperscript)

        #  //
        # // Input files to submit with the job
        #//
        code += "(inputFiles="
        code += "(%s %s)" % (os.path.basename(wrapperscript), wrapperscript)
        for fname in self.jobInputFiles:
            code += "(%s %s)" % (os.path.basename(fname), fname)
        code += ")"

        # Output files; leave everything at the CE until explicitely
        # retrieved/removed by ngget/ngclean. (Needed mainly for
        # debugging.)
        code += "(outputFiles=(\"/\" \"\"))"

        code += "(runTimeEnvironment=APPS/HEP/CMSSW-PA)"
        code += "(jobName=%s)" % jobName
        code += "(stdout=output)(stderr=errors)(gmlog=gridlog)"
        return code


    def makeWrapperScript(self, filename, jobName, cacheDir):
        """
        _makeWrapperScript_

        Make the main executable script for ARC to execute the
        job
        
        """
        #  //
        # // Generate main executable script for job
        #//
        script = ["#!/bin/sh\n"]
        #script.extend(standardScriptHeader(jobName))

        #  // 
        # // Some code useful for debugging
        #//
        script.append("ulimit -a\n")
        script.append("echo pwd: `pwd`\n")
        script.append("ls -la\n")
        script.append("export\n")
        script.append("python -V 2>&1\n")
        script.append("echo PYTHONPATH: $PYTHONPATH\n")

        script.append("export PRODAGENT_JOB_INITIALDIR=`pwd`\n")

        if self.isBulk:
            script.extend(bulkUnpackerScript(self.specSandboxName))
        else:
            script.append("JOB_SPEC_FILE=$PRODAGENT_JOB_INITIALDIR/%s\n" %
                          self.singleSpecName)   
            
        script.append("tar -zxf $PRODAGENT_JOB_INITIALDIR/%s\n" % 
                                                           self.mainSandboxName)
        script.append("cd %s\n" % self.workflowName)
        script.append("./run.sh $JOB_SPEC_FILE > ./run.log 2>&1 \n")


        # ARCTracker.py will retrieve FrameworkJobReport and run.log from
        # $PRODAGENT_JOB_INITIALDIR.
        script.append("cd $PRODAGENT_JOB_INITIALDIR\n")
        script.append("cp %s/FrameworkJobReport.xml ./\n" % self.workflowName)
        script.append("cp %s/run.log ./\n" % self.workflowName)

        #  Gather all the files in a tar.bz2 file for easier retrieval --
        #  something we may want to do if the job failed and we want to
        #  find out why.
        script.append("tar jcvf %s.tar.bz2 %s\n" % (self.workflowName,self.workflowName))
        script.append("rm -rf %s\n" % self.workflowName)
        script.append("rm -rf certificates\n")

        #script.extend(missingJobReportCheck(jobName))

        handle = open(filename, 'w')
        handle.writelines(script)
        handle.close()

        return
    
registerSubmitter(ARCSubmitter, ARCSubmitter.__name__)
