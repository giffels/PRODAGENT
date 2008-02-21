#!/usr/bin/env python


import os
import sys
from ProdCommon.FwkJobRep.TaskState import TaskState


class OfflineDQMHarvester:
    """
    _OfflineDQMHarvester_

    Util to trawl through a Framework Job Report to find analysis
    files and copy them to some DQM server

    """
    def __init__(self):
        self.state = TaskState(os.getcwd())
        self.state.loadRunResDB()        

        try:
            self.state.loadJobReport()
        except Exception, ex:
            print "Error Reading JobReport:"
            print str(ex)
            self.state._JobReport = None

        

    def __call__(self):
        """
        _operator()_

        Invoke this object to find files and do stage out

        """
        if self.state._JobReport == None:
            msg = "No Job Report available\n"
            msg += "Unable to process analysis files for offline DQM\n"
            print msg
            return 1

        jobRep = self.state._JobReport

        for aFile in jobRep.analysisFiles:
            print "Found Analysis File: %s" % aFile
            self.copyOut(aFile)

        return 0

    
    def copyOut(self, filename):
        """
        _copyOut_

        Copy the given DQM file out to some DQM Server.

        """
        pass

if __name__ == '__main__':

    
    harvester = OfflineDQMHarvester()
    status = harvester()

    sys.exit(status)
