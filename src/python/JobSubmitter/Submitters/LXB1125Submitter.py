#!/usr/bin/env python
"""
_LXB1125Submitter_

SubmitterInterface implementation for simple BOSS Submission.

Configuration of the submitter is done via the configuration variables
in this module, for simplicity in the prototype.

"""

#  //
# // Configuration variables for this submitter
#//
bossJobType = "test"  # some predetermined type value from boss install here
bossScheduler = "fork"
#bossScheduler = "lsf" # batch scheduler as registered to BOSS


#  //
# // End of Config variables.
#//

import os
import logging

from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.SubmitterInterface import SubmitterInterface



class LXB1125Submitter(SubmitterInterface):
    """
    _LXB1125Submitter_

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


        # BOSS supported versions (best red from configration)
        supportedBossVersions = ["v3","v4"]
        self.BossVersion=os.environ["BOSSVERSION"].split('_')[0]
#        logging.info("Boss Version %s"% self.BossVersion)
        # test if version is in supported versions list
        if not supportedBossVersions.__contains__(self.BossVersion):
            msg = "Error: BOSS version " +  os.environ["BOSSVERSION"] + " not supported:\n"
            msg += "supported versions are " + supportedBossVersions.__str__()
            raise RuntimeError, msg
        

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
        script.append("./run.sh > ../stdout.log\n")
        script.append("cp cmsRun1/FrameworkJobReport.xml ../\n")
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
        #print "LXB1125Submitter.doSubmit: %s"  % wrapperScript
        #print "LXB1125Submitter.doSubmit: %s"  % jobTarball

        #  //
        # // Build BOSS Declare command
        #//
        #bossDeclare = "boss declare -jobtype %s " % bossJobType
        bossDeclare = "boss declare -classad declare.clad"
        declareClad=open("declare.clad","w")
        declareClad.write("executable = %s;\n" % os.path.basename(wrapperScript))
        declareClad.write("infiles = %s,%s;\n" % (wrapperScript, jobTarball))
        #declareClad.write("outfiles = FrameworkJobReport.xml;\n")
        declareClad.write("outfiles = stdout.log,FrameworkJobReport.xml;\n")
        #%os.path.basename(wrapperScript).split("-submit")[0].strip())
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
        bossSubmit += "-scheduler %s " % bossScheduler
        #print "LXB1125Submitter.doSubmit: %s" % bossSubmit
        #  //
        # // Executing BOSS Submit command
        #//
        output = self.executeCommand(bossSubmit)
        print "LXB1125Submitter.doSubmit: %s" % output
        return

# try:
#     if os.access(compConfig.parameter("BOSSPATH")+"/boss",os.R_OK and os.X_OK):
#         args["BOSSDIR"]=compConfig.parameter("BOSSDIR")
#         args["BOSSVERSION"]=compConfig.parameter("BOSSVERSION")
#         args["BOSSPATH"]=compConfig.parameter("BOSSPATH")
#         os.environ["BOSSVERSION"]=args["BOSSVERSION"]
# except StandardError, ex:
#     pass
    

registerSubmitter(LXB1125Submitter, "lxb1125")
