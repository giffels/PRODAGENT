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
import sys
import logging
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

        # Hard-code this for now, as a 2nd step will remove support for v3
        self.BossVersion = "v4"

        # BOSS supported versions (best red from configration)
        supportedBossVersions = ["v3","v4"]


        # test if version is in supported versions list
        if not supportedBossVersions.__contains__(self.BossVersion):
            msg = "Error: BOSS version " +  os.environ["BOSSVERSION"] + " not supported:\n"
            msg += "supported versions are " + supportedBossVersions.__str__()
            raise RuntimeError, msg
        
        self.parameters['Scheduler']="edg"
        self.bossSubmitCommand={"v3":self.BOSS3submit,"v4":self.BOSS4submit}


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
##         script.append("cp %s/*/*.root .\n" % jobname )

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
        bossJobId=self.isBOSSDeclared()
        if bossJobId==None:
            self.declareToBOSS()
            bossJobId=self.isBOSSDeclared()
        #bossJobId=self.getIdFromFile(TarballDir, JobName)
        print "bossJobId = %s"%bossJobId
        JobName=self.parameters['JobName']
        swversion=self.parameters['AppVersions'][0]  # only one sw version for now

        schedulercladfile = "%s/%s_scheduler.clad" %  (self.parameters['JobCacheArea'],self.parameters['JobName'])
        declareClad=open(schedulercladfile,"w")
        declareClad.write("Requirements = Member(\"VO-cms-%s\", other.GlueHostApplicationSoftwareRunTimeEnvironment);\n"%swversion)
        declareClad.write("VirtualOrganisation = \"cms\";\n")
        declareClad.close()

        try:
            output=self.executeCommand("grid-proxy-info")
            output=output.split("timeleft :")[1].strip()
            if output=="0:00:00":
                #logging.info( "You need a grid-proxy-init")
                logging.error("grid-proxy-init expired")
                #sys.exit()
        except StandardError,ex:
            #print "You need a grid-proxy-init"
            logging.error("grid-proxy-init does not exist")
            sys.exit()
            
        bossSubmit = self.bossSubmitCommand[self.BossVersion](bossJobId)  
        bossSubmit += "-scheduler %s -schclassad %s" % (self.parameters['Scheduler'],schedulercladfile)

        #  //
        # // Executing BOSS Submit command
        #//
        print "LCGSubmitter.doSubmit:", bossSubmit
        output = self.executeCommand(bossSubmit)
        print "LCGSubmitter.doSubmit: %s" % output
        #os.remove(cladfile)
        return



registerSubmitter(LCGSubmitter, "lcg")
