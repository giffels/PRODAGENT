#!/usr/bin/env python
"""
_T0LSFSubmitter_

Submitter for T0 LSF submissions that is capable of handling both
Bulk and single LSF submissions


"""
import os
import errno
import socket
import datetime
import logging

from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.BulkSubmitterInterface import BulkSubmitterInterface
from JobSubmitter.JSException import JSException

from ProdAgentCore.Configuration import loadProdAgentConfiguration

from ProdAgent.Resources.LSF import LSFConfiguration


bulkUnpackerScriptMain = \
"""

#echo "===Available JobSpecs:==="
#/bin/ls `pwd`/BulkSpecs
#echo "========================="


JOB_SPEC_FILE="`pwd`/BulkSpecs/$JOB_SPEC_NAME"

PROCEED_WITH_SPEC=0

if [ -e "$JOB_SPEC_FILE" ]; then
   #echo "Found Job Spec File: $JOB_SPEC_FILE"
   PROCEED_WITH_SPEC=1
else
   #echo "Job Spec File Not Found: $JOB_SPEC_NAME"
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



def bulkUnpackerScript(bulkSpecTarName,jobSpecName):
    """
    _bulkUnpackerScript_

    Unpacks bulk spec tarfile,

    for real bulk submission we have to use job arrays and
    construct the jobSpecName from the job index

    If file not found, it generates a failure report and exits
    Otherwise, JOB_SPEC_FILE will be set to point to the script
    to invoke the run.sh command
    
    """
    lines = [
        "JOB_SPEC_NAME=%s-JobSpec.xml\n" % jobSpecName, 
        "BULK_SPEC_NAME=\"%s\"\n" % bulkSpecTarName,
        "echo \"This Job Using Spec: $JOB_SPEC_NAME\"\n",
        "tar -zxf $BULK_SPEC_NAME >& /dev/null\n",
        ]
    lines.append(bulkUnpackerScriptMain)
    return lines


class T0LSFSubmitter(BulkSubmitterInterface):
    """
    _T0LSFSubmitter_

    Submitter Plugin to submit jobs to the Tier-0 LSF system.

    Can generate bulk or single type submissions.

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

        if not self.pluginConfig.has_key("LSF"):
            self.pluginConfig.newBlock("LSF")

        if not self.pluginConfig['LSF'].has_key('Queue'):
            self.pluginConfig['LSF']['Queue'] = "8nh"

        if not self.pluginConfig['LSF'].has_key('LsfLogDir'):
            self.pluginConfig['LSF']['LsfLogDir'] = "None"

        if not self.pluginConfig['LSF'].has_key('CmsRunLogDir'):
            self.pluginConfig['LSF']['CmsRunLogDir'] = "None"

        if not self.pluginConfig['LSF'].has_key('NodeType'):
            self.pluginConfig['LSF']['NodeType'] = "None"

        if not self.pluginConfig['LSF'].has_key('Resource'):
            self.pluginConfig['LSF']['Resource'] = "None"

        if not self.pluginConfig['LSF'].has_key('ResourceRepack'):
            self.pluginConfig['LSF']['ResourceRepack'] = "None"
        return

    
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
        # // Check if directory for LSF log exists, create if it doesn't
        #//
        lsfLogDir = self.pluginConfig['LSF']['LsfLogDir']
        if ( lsfLogDir != 'None' ):
            now = datetime.datetime.today()
            lsfLogDir += '/%s' % now.strftime("%Y%m%d%H")
            try:
                os.mkdir(lsfLogDir, 0755)
                logging.debug("Created directory %s" % lsfLogDir)
            except OSError, err:
                # suppress LSF log unless it's about an already exisiting directory
                if err.errno != errno.EEXIST or not os.path.isdir(lsfLogDir):
                    logging.debug("Can't create directory %s, turning off LSF log" % lsfLogDir)
                    lsfLogDir = 'None'

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
        failureList = []
        for jobSpec, cacheDir in self.toSubmit.items():
            logging.debug("Submit: %s from %s" % (jobSpec, cacheDir) )
            logging.debug("SpecFile = %s" % self.specFiles[jobSpec])
            self.makeWrapperScript(os.path.join(cacheDir, "lsfsubmit.sh"), jobSpec, cacheDir, lsfLogDir)

            # //
            # // Submit LSF job
            # //
            lsfSubmitCommand = 'bsub'

            lsfSubmitCommand += ' -q %s' % self.pluginConfig['LSF']['Queue']

            if ( jobSpec.startswith("Repack-") and self.pluginConfig['LSF']['ResourceRepack'] != "None" ):
                lsfSubmitCommand += ' -R "%s"' % self.pluginConfig['LSF']['ResourceRepack']
            elif ( self.pluginConfig['LSF']['Resource'] != "None" ):
                lsfSubmitCommand += ' -R "%s"' % self.pluginConfig['LSF']['Resource']
            elif ( self.pluginConfig['LSF']['NodeType'] != "None" ):
                lsfSubmitCommand += ' -R "type==%s"' % self.pluginConfig['LSF']['NodeType']

            lsfSubmitCommand += ' -g %s' % LSFConfiguration.getGroup()
            lsfSubmitCommand += ' -J %s' % jobSpec

            if ( lsfLogDir == "None" ):
                lsfSubmitCommand += ' -oo /dev/null'
            else:
                lsfSubmitCommand += ' -oo %s/%s.lsf.%%J.log' % (lsfLogDir, jobSpec)

            # lsfSubmitCommand += ' -oo /tmp/%s.log' % jobSpec
            # lsfSubmitCommand += ' -f "%s < /tmp/%s.log"' % ( os.path.join(cacheDir,"lsfsubmit.log"), jobSpec )

            lsfSubmitCommand += ' < %s' % os.path.join(cacheDir,"lsfsubmit.sh")

            try:
                output = self.executeCommand(lsfSubmitCommand)
                logging.info("T0LSFSubmitter.doSubmit: %s " % output)
            except RuntimeError, err:
                failureList.append(jobSpec)

        if len(failureList) > 0:
            raise JSException("Submission Failed", FailureList = failureList)


    def makeWrapperScript(self, filename, jobName, cacheDir, lsfLogDir):
        """
        _makeWrapperScript_

        Make the main executable script for LSF to execute the
        job
        
        """

        # need to know this host name
        hostname = socket.getfqdn()

        #  //
        # // Generate main executable script for job
        #//
        script = ["#!/bin/sh\n"]
        #script.extend(standardScriptHeader(jobName))

	# workaround for problem with LSF loosing kerberos token (no longer needed)
        #script.append("export KRB5_CONFIG=/etc/krb5.conf")

        script.append("export PRODAGENT_JOB_INITIALDIR=`pwd`\n")

        # needed at some point to acces SLC5 head node over rfio
        #script.append("export RFIO_PORT=5001\n")

        for fname in self.jobInputFiles:
            script.append("rfcp %s:%s . \n" % (hostname,fname))

        if self.isBulk:
            script.extend(bulkUnpackerScript(self.specSandboxName, jobName))
        else:
            script.append("JOB_SPEC_FILE=$PRODAGENT_JOB_INITIALDIR/%s\n" %
                          self.singleSpecName)   

        script.append("tar -zxf $PRODAGENT_JOB_INITIALDIR/%s > /dev/null 2>&1\n" % self.mainSandboxName)
        script.append("cd %s\n" % self.workflowName)
        stageHost = os.getenv( "STAGE_HOST" )
        if stageHost:
            script.append("export STAGE_HOST=%s\n" % stageHost )
        script.append("( /usr/bin/time ./run.sh $JOB_SPEC_FILE 2>&1 ) | gzip > ./run.log.gz\n")
        script.append("rfcp ./FrameworkJobReport.xml %s:%s/FrameworkJobReport.xml\n" % (hostname,cacheDir))

        # get back a lot of debug information to the head node
        #script.append("find . -type f -name '*.log' -exec rfcp {} %s:%s/ \;\n" % (hostname,cacheDir))

        outputlogfile = jobName
        outputlogfile += '.`date +%s`.log.gz'

        if ( self.pluginConfig['LSF']['CmsRunLogDir'] != "None" ):
            script.append("rfcp ./run.log.gz %s/%s\n" % (self.pluginConfig['LSF']['CmsRunLogDir'],outputlogfile))

        #script.append("sleep 3600")

        #script.extend(missingJobReportCheck(jobName))

        # debug information to track down kerberos problems
        script.append("date +%s\n")
        script.append("klist -aef\n")
        script.append("tokens\n")

        # debug information to see if LSF log file exists
        if ( lsfLogDir != "None" ):
            script.append("find %s -type f -name %s.lsf.%%J.log -exec stat {} \;\n" % (lsfLogDir, jobName))

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
