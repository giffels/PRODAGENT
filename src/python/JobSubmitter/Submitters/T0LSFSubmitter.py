#!/usr/bin/env python
"""
_T0LSFSubmitter_

Submitter for T0 LSF submissions that is capable of handling both
Bulk and single LSF submissions


"""
import os
import logging

from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.BulkSubmitterInterface import BulkSubmitterInterface
from JobSubmitter.JSException import JSException


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


class T0LSFSubmitter(BulkSubmitterInterface):
    """
    _T0LSFSubmitter_

    Submitter Plugin to submit jobs to the Tier-0 LSF system.

    Can generate bulk or single type submissions.

    """
    def doSubmit(self):
        """
        _doSubmit_

        Main method to generate job submission.

        Attributes in Base Class BulkSubmitterInterface are populated with
        details of what to submit in terms of the job spec and
        details contained therein

        """
        logging.debug("<<<<<<<<<<<<T0LSFSubmitter>>>>>>>>>>")

        self.workflowName = self.primarySpecInstance.payload.workflow
        self.mainJobSpecName = self.primarySpecInstance.parameters['JobName']
        self.mainSandbox = \
                   self.primarySpecInstance.parameters.get('BulkInputSandbox',None)
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
            self.makeWrapperScript(os.path.join(cacheDir,"lsfsubmit.sh"),jobSpec,cacheDir)

        #  //
        # // Submit LSF job
        #//
        lsfSubmit = "bsub -q 8nm -g /groups/tier0/reconstruction -J %s < %s" % (jobSpec, os.path.join(cacheDir,"lsfsubmit.sh"))
        logging.debug("T0LSFSubmitter.doSubmit: %s" % lsfSubmit)
        output = self.executeCommand(lsfSubmit)
        logging.info("T0LSFSubmitter.doSubmit: %s " % output)
        

    def makeWrapperScript(self, filename, jobName, cacheDir):
        """
        _makeWrapperScript_

        Make the main executable script for LSF to execute the
        job
        
        """
        
        #  //
        # // Generate main executable script for job
        #//
        script = ["#!/bin/sh\n"]
        #script.extend(standardScriptHeader(jobName))
                      
        script.append("export PRODAGENT_JOB_INITIALDIR=`pwd`\n")

        for fname  in self.jobInputFiles:
            script.append("rfcp lxgate39.cern.ch:%s . \n" % fname)
            
        if self.isBulk:
            script.extend(bulkUnpackerScript(self.specSandboxName))
        else:
            script.append("JOB_SPEC_FILE=$PRODAGENT_JOB_INITIALDIR/%s\n" %
                          self.singleSpecName)   
            
        script.append(
            "tar -zxf $PRODAGENT_JOB_INITIALDIR/%s\n" % self.mainSandboxName 
            )
        script.append("cd %s\n" % self.workflowName)

        script.append("echo START ENVIRONMENT\n")
        script.append("printenv\n")
        script.append("echo STOP ENVIRONMENT\n")
        script.append("./run.sh $JOB_SPEC_FILE\n")
        script.append(
            "rfcp ./FrameworkJobReport.xml lxgate39.cern.ch:%s \n" % cacheDir)
        #script.extend(missingJobReportCheck(jobName))


        handle = open(filename, 'w')
        handle.writelines(script)
        handle.close()

        return
    

#  //
# // BRAINSTORMING:
#//
#
#  For bulk submission, we compile a list of run numbers
#  These can be used as the Job Array for bulk submission
#   -J "WorkflowSpecID-[minRun-maxRun]"
#
#  In the exe script we generate and submit, the job spec
#  ID & file can be constructed using the $LSB_JOBINDEX which
#  will be the run number. The JobSpec file to use will then
#  be WorkflowSpecID-$LSB_JOBINDEX which means we can find the
#  file in the spec tarball.
#
#  We need to use a group to make tracking easy:       
#  -g /groups/tier0/reconstruction
#        
#        
#  The Job needs to run and drop off the FrameworkJobReport somewhere  
#  ultimately this needs to be the JobCreator cache dir for the JobSpecID
#  but an intermediate drop box and migration by the tracking component would
#  work.
#
#  Logfiles probably should be redirected to the Job Cache area as well
#
#  We need to turn off the bloody emails.
#
#
#
#

registerSubmitter(T0LSFSubmitter, T0LSFSubmitter.__name__)
