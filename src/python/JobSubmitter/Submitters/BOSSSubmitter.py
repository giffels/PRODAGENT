#!/usr/bin/env python
"""
_BOSSSubmitter_

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

from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.SubmitterInterface import SubmitterInterface



class BOSSSubmitter(SubmitterInterface):
    """
    _BOSSSubmitter_

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
        supportedBossVersions = ["v3_6_1","v3_6_2","v4_0_0"]

        # test if version is in supported versions list
        if not supportedBossVersions.__contains__(os.environ["BOSSVERSION"]):
            msg = "Error: BOSS version " +  os.environ["BOSSVERSION"] + " not supported:\n"
            msg += "supported versions are " + supportedBossVersions.__str__()
            raise RuntimeError, msg
        

    #  //
    # //  Initially start with the default wrapper script
    #//   provided by the SubmitterInterface base class
    #  //
    # //  If this needs to be customised, implement the 
    #//   generateWrapper method
    #def generateWrapper(self, wrapperName, tarballName, jobname):
    
        
    
    def doSubmit(self, wrapperScript, jobTarball):
        """
        _doSubmit_


        Override Submission action to construct a BOSS submit command
        and run it
        
        """
        #print "BOSSSubmitter.doSubmit: %s"  % wrapperScript
        #print "BOSSSubmitter.doSubmit: %s"  % jobTarball

        #  //
        # // Build BOSS Declare command
        #//
        #bossDeclare = "boss declare -jobtype %s " % bossJobType
        bossDeclare = "boss declare -classad declare.clad"
        declareClad=open("declare.clad","w")
        declareClad.write("executable = %s;\n" % os.path.basename(wrapperScript))
        declareClad.write("infiles = %s,%s;\n" % (wrapperScript, jobTarball))
        declareClad.write("outfiles = FrameworkJobReport.xml;\n")
        declareClad.close()
        #bossDeclare += "-classad declare.clad"
        #bossDeclare += "-infiles %s,%s " % (wrapperScript, jobTarball) 

        #  //
        # // Execute BOSS Declare command to get boss job id 
        #//
        
       # print "BOSSSubmitter.doSubmit: %s" % bossDeclare
        bossJobId = self.executeCommand(bossDeclare)
       # print "-jobid %s " % bossJobId.split("Job ID")[1].strip()
        #  //
        # // Build BOSS Submit command using job id
        #//
        bossJobId = bossJobId.split("Job ID")[1].strip()
        bossSubmit = "boss submit "
        bossSubmit += "-jobid %s " % bossJobId
        bossSubmit += "-scheduler %s " % bossScheduler
        #print "BOSSSubmitter.doSubmit: %s" % bossSubmit
        #  //
        # // Executing BOSS Submit command
        #//
        output = self.executeCommand(bossSubmit)
        print "BOSSSubmitter.doSubmit: %s" % output
        return

# try:
#     if os.access(compConfig.parameter("BOSSPATH")+"/boss",os.R_OK and os.X_OK):
#         args["BOSSDIR"]=compConfig.parameter("BOSSDIR")
#         args["BOSSVERSION"]=compConfig.parameter("BOSSVERSION")
#         args["BOSSPATH"]=compConfig.parameter("BOSSPATH")
#         os.environ["BOSSVERSION"]=args["BOSSVERSION"]
# except StandardError, ex:
#     pass
    

registerSubmitter(BOSSSubmitter, "boss")
