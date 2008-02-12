#!/usr/bin/env python
"""
_JobAllocationPluginInterface_

Interface for plugins to the Job Emulator
that allocate jobs submitted to the Job
Emulator.

"""

__revision__ = "$Id: $"
__version__ = "$Revision: $"

class JobAllocationPluginInterface:
    """
    _JobAllocationPluginInterface_

    Interface for plugins to the Job Emulator
    that allocate jobs submitted to the Job
    Emulator.
    """

    def __init__(self):
        pass
        #self.args = {}

    def allocateJob(self, jobSpec=None):
        """
        _allocateJob_

        Determine where job will be run, and return WorkerNodeInfo class
        
        In general the sub class will provide following information
        possibly add some other information about nodes (CPU power, etc)
        self.args['SiteName']
        self.args['HostID']
        self.args['HostName']
        self.args['se-name']  
        self.args['ce-name']                    
        """
             
        fname = "JobEmulator.JobAllocationPluginInterface.processJobs"
        raise NotImplementedError, fname
    

