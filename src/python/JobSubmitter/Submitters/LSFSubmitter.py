#!/usr/bin/env python
"""
_LSFSubmitter_

SubmitterInterface implementation for simple BOSS Submission.

Configuration of the submitter is done via the configuration variables
in this module, for simplicity in the prototype.

"""

__revision__ = "$Id:$"

#  //
# // Configuration variables for this submitter
#//
bossJobType = "cmsRun"  # some predetermined type value from boss install here
bossScheduler = "lsf" # batch scheduler as registered to BOSS

#  //
# // End of Config variables.
#//

import os

from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.SubmitterInterface import SubmitterInterface

class LSFSubmitter(SubmitterInterface):
    """
    _LSFSubmitter_

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

        self.BossVersion=os.environ["BOSSVERSION"].split('_')[0]

        # BOSS supported versions (best red from configration)
        supportedBossVersions = ["v3","v4"]

        # test if version is in supported versions list
        if not supportedBossVersions.__contains__(self.BossVersion):
            msg = "Error: BOSS version " +  os.environ["BOSSVERSION"] + " not supported:\n"
            msg += "supported versions are " + supportedBossVersions.__str__()
            raise RuntimeError, msg
        self.bossSubmitCommand={"v3":self.BOSS3submit,"v4":self.BOSS4submit}
        self.parameters['Scheduler']="lsf"


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
        print "bossJobId = %s"%bossJobId
        JobName=self.parameters['JobName']

        #  //
        # // Build BOSS Declare command
        #//
        bossSubmit = self.bossSubmitCommand[self.BossVersion](bossJobId)

        #print "LXB1125Submitter.doSubmit: %s" % bossSubmit
        #  //
        # // Executing BOSS Submit command
        #//
        output = self.executeCommand(bossSubmit)
        print "LSFSubmitter.doSubmit: %s" % output
        return







    

registerSubmitter(LSFSubmitter, "lsf")
