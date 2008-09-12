#!/usr/bin/env python
"""
_LCGSubmitter_

SubmitterInterface implementation for simple BOSS Submission.

Configuration of the submitter is done via the configuration variables
in this module, for simplicity in the prototype.

"""

#  //
# // Configuration variables for this submitter
#//
#bossJobType = ""  # some predetermined type value from boss install here
bossScheduler = "edg"
#bossScheduler = "fork"


#  //
# // End of Config variables.
#//

import os

from MCPayloads.JobSpec import JobSpec
from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.SubmitterInterface import SubmitterInterface


class LCGSubmitter(SubmitterInterface):
    """
    _LCGSubmitter_

    Simple BOSS Submission wrapper for testing.

    """
    
    def __init__(self):
        SubmitterInterface.__init__(self)
        #  //
        # // BOSS installation consistency check.
        #//
        if not os.environ.has_key("BOSSDIR"):
            msg = "Error: BOSS environment BOSSDIR not set:\n"
            raise RuntimeError, msg

        if not os.environ.has_key("BOSSVERSION"):
            msg = "Error: BOSS environment BOSSVERSION not set:\n"
            raise RuntimeError, msg

        #self.bossJobType=""

        # BOSS supported versions (best red from configration)
        supportedBossVersions = ["v3_6_1","v3_6_2","v3_6_3","v4_0_0"]

        # test if version is in supported versions list
        if not supportedBossVersions.__contains__(os.environ["BOSSVERSION"]):
            msg = "Error: BOSS version " +  os.environ["BOSSVERSION"] + " not supported:\n"
            msg += "supported versions are " + supportedBossVersions.__str__()
            raise RuntimeError, msg
        
        if os.environ["BOSSVERSION"]=="v4_0_0":
            pass
        else:
            inf,outf=os.popen4("boss SQL -query \"select name from JOBTYPE where name = 'cmssw'\"")
            outp=outf.read()
            self.bossJobType="cmssw"
            #print outp.find("cmssw")
            if outp.find("cmssw")<0:
                self.bossJobType="stdjob"
        #print bossJobType

    #  //
    # //  Initially start with the default wrapper script
    #//   provided by the SubmitterInterface base class
    #  //
    # //  If this needs to be customised, implement the 
    #//   generateWrapper method
    def generateWrapper(self, wrapperName, tarballName, jobname):
        """
        override default wrapper to generate stdout file

        """
        script = ["#!/bin/sh\n"]
        script.append("tar -zxf %s\n" % os.path.basename(tarballName))
        script.append("cd %s\n" % jobname)
        script.append("./run.sh \n")
        script.append("cd ..\n")
        script.append("cp %s/FrameworkJobReport.xml . \n" % jobname)
        script.append("cp %s/*/*.root .\n" % jobname )

        handle = open(wrapperName, 'w')
        handle.writelines(script)
        handle.close()

        return 
        
        
    
    def doSubmit(self, wrapperScript, jobTarball):
        """
        _doSubmit_


        Override Submission action to construct a BOSS submit command
        and run it

        Initial tests: No FrameworkJobReport yet, stage back stdout log
        
        """
        #print "LCGSubmitter.doSubmit: %s"  % wrapperScript
        #print "LCGSubmitter.doSubmit: %s"  % jobTarball
        TarballDir,JobName=os.path.split(jobTarball)
        JobName=JobName.replace(".tar.gz",'')
        jobSpec = JobSpec()
        jobSpec.load(TarballDir+"/%s-JobSpec.xml"%JobName)
        if jobSpec == None:
           print "Unable to read JobSpec :TarballDir/%s-JobSpec.xml"%JobName
        swversion=jobSpec.payload.application['Version']

        #  //
        # // Build BOSS Declare command
        #//
        cladfile="%s.clad"%JobName
        bossDeclare = "boss declare -classad %s "%cladfile
        declareClad=open(cladfile,"w")
        declareClad.write("executable = %s;\n" % os.path.basename(wrapperScript))
        declareClad.write("jobtype = %s;\n"%self.bossJobType)
        declareClad.write("stdout = %s.stdout;\n"%JobName)
        declareClad.write("stderr = %s.stderr;\n"%JobName)
        declareClad.write("infiles = %s,%s;\n" % (wrapperScript, jobTarball))
#        declareClad.write("outfiles = *.root,stdout.log,stderr.log,FrameworkJobReport.xml;\n")
        declareClad.write("outfiles = %s.stdout,%s.stderr,FrameworkJobReport.xml;\n"%(JobName,JobName))
#        declareClad.write("queue = 1nh;\n")
        declareClad.close()
        declareClad=open("scheduler.clad","w")
        declareClad.write("Requirements = Member(\"VO-cms-%s\", other.GlueHostApplicationSoftwareRunTimeEnvironment);\n"%swversion)
        declareClad.write("VirtualOrganisation = \"cms\";\n")
        declareClad.close()
        #bossDeclare += "-classad declare.clad"
        #bossDeclare += "-infiles %s,%s " % (wrapperScript, jobTarball) 

        #  //
        # // Execute BOSS Declare command to get boss job id 
        #//
        
       # print "LXB1125Submitter.doSubmit: %s" % bossDeclare
        bossJobId = self.executeCommand(bossDeclare)
       # print "-jobid %s " % bossJobId.split("Job ID")[1].strip()
        #  //
        # // Build BOSS Submit command using job id
        #//
        bossJobId = bossJobId.split("Job ID")[1].strip()
        bossSubmit = "boss submit "
        bossSubmit += "-jobid %s " % bossJobId
        bossSubmit += "-scheduler %s -schclassad scheduler.clad" % bossScheduler
        #  //
        # // Executing BOSS Submit command
        #//
        print "LCGSubmitter.doSubmit:", bossSubmit
        output = self.executeCommand(bossSubmit)
        print "LCGSubmitter.doSubmit: %s" % output
        os.remove(cladfile)
        return
    

registerSubmitter(LCGSubmitter, "lcg")
