#!/usr/bin/env python
"""
_CondorGSubmitter_

Globus Universe Condor Submitter implementation.
Used for testing jobs in a batch environment, shouldnt be used generally
as it includes no job tracking.


"""

__revision__ = "$Id:$"

import os

from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.SubmitterInterface import SubmitterInterface

#  //
# //  Tweak the name of the jobmanager to be used here
#//
GlobusScheduler = "cmsosgce.fnal.gov/jobmanager-condor"

CallbackScript = \
"""
#!/usr/bin/env python

import os
import sys
import xmlrpclib

logfile = open("callback.log", 'w')

currentDir = os.getcwd()
jobReport = os.path.join(currentDir, "FrameworkJobReport.xml")
logfile.write("Report:%s\\n" % jobReport)

if not os.path.exists(jobReport):
    logfile.write("Report Not Found..\\n")
    logfile.close()
    sys.exit(0)

try:
    logfile.write("Connecting to ProdAgent...\\n")
    server = xmlrpclib.Server("http://127.0.0.1:8081")
    server.prodAgentAlive()
    logfile.write("Connected...\\n")
except:
    logfile.write("Connection Failed...\\n")
try:
    logfile.write("Publishing JobSuccess...\\n")
    server.publishEvent("JobSuccess", jobReport)
    logfile.write("Published...\\n")
except:
    logfile.write("Publishing Failed...\\n")

logfile.close()
sys.exit(0)

"""


class CondorGSubmitter(SubmitterInterface):
    """
    _CondorGSubmitter_

    Globus Universe condor submitter. Generates a simple JDL file
    and condor_submits it using a dag wrapper and post script to generate
    a callback to the ProdAgent when the job completes.
    

    """

    def generateWrapper(self, wrapperName, tarballName, jobname):
        """
        _generateWrapper_

        Use the default wrapper provided by the base class but
        overload this method to also generate a JDL file

        """
        jdlFile = "%s.jdl" % wrapperName
        print "CondorGSubmitter.generateWrapper:", jdlFile
        directory = os.path.dirname(wrapperName)
        jdl = []
        jdl.append("universe = globus\n")
        jdl.append("globusscheduler = %s\n" % GlobusScheduler)
        jdl.append("initialdir = %s\n" % directory)
        jdl.append("Executable = %s\n" % wrapperName)
        jdl.append("transfer_input_files = %s\n" % tarballName)
        jdl.append("transfer_output_files = FrameworkJobReport.xml\n")
        jdl.append("should_transfer_files = YES\n")
        jdl.append("when_to_transfer_output = ON_EXIT\n")
        jdl.append("Output = %s-condor.out\n" % jobname)
        jdl.append("Error = %s-condor.err\n" %  jobname)
        jdl.append("Log = %s-condor.log\n" % jobname)
        jdl.append("Queue\n")
        
        
        handle = open(jdlFile, 'w')
        handle.writelines(jdl)
        handle.close()

        
        postFile = os.path.join(directory, "prodAgentCallback.py")
        handle = open(postFile, 'w')
        handle.write(CallbackScript)
        handle.close()
        os.system("chmod +x %s" % postFile)
        print "CondorGSubmitter.generateWrapper:", postFile

        dagFile = "%s.dag" % wrapperName
        print "CondorGSubmitter.generateWrapper:", dagFile
        dag = []
        dag.append("JOB APP %s\n" % jdlFile)
        dag.append("Script POST APP /usr/bin/python2 %s\n" % postFile) 

        handle = open(dagFile, 'w')
        handle.writelines(dag)
        handle.close()
        
        tarballBaseName = os.path.basename(tarballName)
        script = ["#!/bin/sh\n"]
        script.append("PRODAGENT_JOB_INITIALDIR=`pwd`\n")
        script.append("cd $_CONDOR_SCRATCH_DIR\n")
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
        dagFile = "%s.dag" % wrapperScript
        directory = os.path.dirname(wrapperScript)
        command = "cd %s; condor_submit_dag %s" % (
            directory,
            os.path.basename(dagFile),
            )
        print "CondorGSubmitter.doSubmit:", command
        output = self.executeCommand(command)
        print "CondorGSubmitter.doSubmit:", output
        return


registerSubmitter(CondorGSubmitter, "condorg")
