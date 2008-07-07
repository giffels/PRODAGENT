#!/usr/bin/env python
"""
_RuntimeOfflineDQM_

Harvester script for DQM Histograms.

Will do one/both of the following:
1. Copy the DQM Histogram file to the local SE, generating an LFN name for it
2. Post the DQM Histogram file to a Siteconf discovered DQM Server URL

"""
import os
import sys
from ProdCommon.FwkJobRep.TaskState import TaskState
from ProdCommon.MCPayloads.UUID import makeUUID

from StageOut.StageOutMgr import StageOutMgr
from StageOut.StageOutError import StageOutInitError
import StageOut.Impl

_DoHTTPPost = False

class OfflineDQMHarvester:
    """
    _OfflineDQMHarvester_

    Util to trawl through a Framework Job Report to find analysis
    files and copy them to some DQM server

    """
    def __init__(self):
        self.state = TaskState(os.getcwd())
        self.state.loadRunResDB()
        self.config = self.state.configurationDict()
        self.workflowSpecId = self.config['WorkflowSpecID'][0]
        self.jobSpecId = self.config['JobSpecID'][0]

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
            self.stageOut(aFile['FileName'])
            if _DoHTTPPost:
                self.httpPost(aFile['FileName'])

        return 0


    def stageOut(self, filename):
        """
        _stageOut_

        stage out the DQM Histogram to local storage

        """
        try:
            stager = StageOutMgr()
        except Exception, ex:
            msg = "Unable to stage out log archive:\n"
            msg += str(ex)
            print msg
            return

	filebasename = os.path.basename(filename)
	filebasename = filebasename.replace(".root", "")
	
        fileInfo = {
            'LFN' : "/store/unmerged/dqm/%s/%s/%s" % (self.workflowSpecId,
                                                      self.jobSpecId,
                                                      filename),
            'PFN' : os.path.join(os.getcwd(), filename),
            'SEName' : None,
            'GUID' : filebasename,
            }
        try:
            stager(**fileInfo)
        except Exception, ex:
            msg = "Unable to stage out DQM File:\n"
            msg += str(ex)
            print msg
            return




    def httpPost(self, filename):
        """
        _httpPost_

        perform an HTTP POST operation to a webserver

        """
        print "HTTP Post: %s\n Not yet implemented" % filename

if __name__ == '__main__':


    harvester = OfflineDQMHarvester()
    status = harvester()

    sys.exit(status)
