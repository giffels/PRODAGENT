#!/usr/bin/env python
"""
_NoSubmit_

Submitter implementation that doesnt submit.
Used for testing job creation.

"""
__revision__ = "$Id: NoSubmit.py,v 1.3 2006/06/01 16:46:08 evansde Exp $"

from JobSubmitter.Submitters.SubmitterInterface import SubmitterInterface
from JobSubmitter.Registry import registerSubmitter

class NoSubmit(SubmitterInterface):
    """
    _NoSubmit_

    Override Submitter Interface Methods to just print a message

    """


    def doSubmit(self, wrapperScript, jobTarball):
        """
        _doSubmit_


        Override Submission action

        """
        print "NoSubmit.doSubmit: %s"  % wrapperScript
        print "NoSubmit.doSubmit: %s"  % jobTarball
        return

    def editDashboardInfo(self, dashboardInfo):
        print "NoSubmit.editDashboardInfo: %s " % dashboardInfo
    
    def generateWrapper(self, wrapperName, tarballName, jobname):
        print "NoSubmit.generateWrapper: %s" % wrapperName
        print "NoSubmit.generateWrapper: %s" % tarballName
        print "NoSubmit.generateWrapper: %s" % jobname
        
        
registerSubmitter(NoSubmit, NoSubmit.__name__)
