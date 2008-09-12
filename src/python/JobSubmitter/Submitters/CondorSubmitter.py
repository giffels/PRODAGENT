#!/usr/bin/env python
"""
_CondorSubmitter_

Vanilla Universe Condor Submitter implementation.
Used for testing jobs in a batch environment, shouldnt be used generally
as it includes no job tracking.


"""


import os

from JobSubmitter.Registry import registerSubmitter
from JobSubmitter.Submitters.SubmitterInterface import SubmitterInterface




class CondorSubmitter(SubmitterInterface):
    """
    _CondorSubmitter_

    Vanilla Universe condor submitter. Generates a simple JDL file
    and condor_submits it

    """

    def generateWrapper(self, wrapperName, tarballName, jobname):
        """
        _generateWrapper_

        Use the default wrapper provided by the base class but
        overload this method to also generate a JDL file

        """
        jdlFile = "%s.jdl" % wrapperName
        print "CondorSubmitter.generateWrapper:", jdlFile
        jdl = []
        jdl.append("universe = vanilla\n")
        jdl.append("Executable = %s\n" % wrapperName)
        jdl.append("transfer_input_files = %s\n" % tarballName)
        jdl.append("should_transfer_files = YES\n")
        jdl.append("when_to_transfer_output = ON_EXIT\n")
        jdl.append("Output = %s/%s-condor.out\n" % (
            os.path.dirname(wrapperName),
            jobname)
                   )
        jdl.append("Error = %s/%s-condor.err\n" % (
            os.path.dirname(wrapperName),
            jobname)
                   )
        jdl.append("Log = %s/%s-condor.log\n" % (
            os.path.dirname(wrapperName),
            jobname)
                   )
        
        jdl.append("Queue\n")
        
        handle = open(jdlFile, 'w')
        handle.writelines(jdl)
        handle.close()

        SubmitterInterface.generateWrapper(self, wrapperName, tarballName,
                                           jobname)
        return
    

    def doSubmit(self, wrapperScript, jobTarball):
        """
        _doSubmit_

        Build and run a condor_submit command

        """
        jdlFile = "%s.jdl" % wrapperScript

        command = "condor_submit %s" % jdlFile
        print "CondorSubmitter.doSubmit:", command
        output = self.executeCommand(command)
        print "CondorSubmitter.doSubmit:", output
        return


registerSubmitter(CondorSubmitter, "condor")
