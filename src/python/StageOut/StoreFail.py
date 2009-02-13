#!/usr/bin/env python
# pylint: disable-msg=W0142,R0903
"""
_StoreFail_

Util class to stage out a set of files from a job report to the /store/fail
namespace

"""
__version__ = "$Revision$"
__revision__ = "$Id$"

from StageOut.StageOutError import StageOutFailure
from StageOut.StageOutMgr import StageOutMgr


def modifyLFN(inputLfn):
    """
    _modifyLFN_

    Util to tweak a normal LFN to be a /store/fail LFN
    Simple algorithm to start with, split of /store/whatever and replace
    it with /store/fail

    """
    lfnSplit = [ x for x in inputLfn.split("/") if len(x) != 0 ]
    lfnSplit[1] = "fail"
    newLfn = "/%s" % "/".join(lfnSplit)
    return newLfn


class StoreFailMgr:
    """
    _StoreFailMgr_

    """
    def __init__(self, jobReport):
        self.report = jobReport
        self.mgr = StageOutMgr()
        self.failures = {}




    def __call__(self):
        """
        _operator()_

        Invoke stage out of files to /store/fail based on
        information in the report provided.

        Generate a list of the LFNs that were staged out as a
        return value

        """
        stagedOutFiles = []
        for fileToStage in self.report.files:
            lfn = fileToStage['LFN']
            newLfn = modifyLFN(lfn)
            print "Remapping LFN:\n%s\n" % lfn
            print " -TO-\n%s\n" % newLfn
            print "Staging Out: %s" % newLfn
            fileToStage['LFN'] = newLfn
            try:
                result = self.mgr(**fileToStage)
                fileToStage.update(result)
                stagedOutFiles.append(newLfn)
            except StageOutFailure, ex:
                msg = "Unable to stage out %s " % newLfn
                msg += " due to Stage Out Failure:\n"
                msg += str(ex)
                self.failures[newLfn] = msg
                continue

        return stagedOutFiles








