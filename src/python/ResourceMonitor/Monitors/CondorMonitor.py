#!/usr/bin/env python
"""
_CondorMonitor_

ResourceMonitor plugin that monitors a condor Q

"""

from ResourceMonitor.Monitors.MonitorInterface import MonitorInterface
from ResourceMonitor.Registry import registerMonitor


from ResourceMonitor.Monitors.CondorQ import processingJobs, mergeJobs

# TODO: Hard code maximum number of queued jobs at each site
# TODO: of each type
# TODO: The following variables should all be configurable
_MaxQueuedMergeJobs = 10
_MaxQueuedProcJobs = 100
_DefaultGatekeepers = [
    "cmsosgce.fnal.gov/jobmanager-condor-opt",
    "cmsgrid02.hep.wisc.edu/jobmanager-condor",
    "red.unl.edu/jobmanager-pbs",
    "cit-gatekeeper.ultralight.org/jobmanager-condor",
    "osg.rcac.purdue.edu/jobmanager-pbs",
    "osg-gw-2.t2.ucsd.edu/jobmanager-condor",
    "ce01.cmsaf.mit.edu/jobmanager-condor",
    "ufloridapg.phys.ufl.edu/jobmanager-condor",
    ]


class CondorMonitor(MonitorInterface):
    """
    _CondorMonitor_

    Poll condor_q on the local machine and get details of all the ProdAgent
    jobs in there split by processing and merge type.

    Generate a per site constraint for each distinct site being used

    """
    
    def __call__(self):
        mergeInfo = mergeJobs(*_DefaultGatekeepers)
        processingInfo = processingJobs(*_DefaultGatekeepers)

        result = []
        for gatekeeper, jobcounts in mergeInfo.items():
            # TODO: Map jobmanager to SE Name since we use
            # TODO: SE Names for site prefs
            # TODO: not done yet for testing
            idle = jobcounts["Idle"]

            test = idle - _MaxQueuedMergeJobs
            if test < 0:
                constraint = self.newConstraint()
                constraint['count'] = abs(test)
                constraint['type'] = "Merge"
                constraint['site'] = gatekeeper
                print str(constraint)
                result.append(constraint)

        for gatekeeper, jobcounts in processingInfo.items():
            # TODO: Map jobmanager to SE Name since we use
            # TODO: SE Names for site prefs
            # TODO: not done yet for testing
            idle = jobcounts["Idle"]
            test = idle - _MaxQueuedProcJobs
            if test < 0:
                constraint = self.newConstraint()
                constraint['count'] = abs(test)
                constraint['type'] = "Processing"
                constraint['site'] = gatekeeper
                print str(constraint)
                result.append(constraint)
                
                
        return result
            

    
registerMonitor(CondorMonitor, CondorMonitor.__name__)
