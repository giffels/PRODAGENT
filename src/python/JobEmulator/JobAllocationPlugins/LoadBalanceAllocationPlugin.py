#!/usr/bin/env python
"""
_LoadBalanceAllocationPlugin_

Find the node which has less jobs than others and Allocate a job
This plugin will need to be removed since Random Allocation might not be used at all 
"""
__revision__ = "$Id: $"
__version__ = "$Revision: $"

from JobEmulator.JobEmulatorAPI import getLessBusySiteAndNode
from JobEmulator.Registry import registerPlugin
from JobEmulator.JobAllocationPlugins.JobAllocationPluginInterface \
    import JobAllocationPluginInterface

class LoadBalanceAllocationPlugin(JobAllocationPluginInterface):
    """
    _LoadBalanceAllocationPlugin_

    Find the node which has less jobs than and Allocate a job

    """
    
    def allocateJob(self, jobSpec=None):
        """
        _allocateJob_

        Determine where job will be run, and return self.args 
        randomly allocating job among 50 nodes       
        provide following information
        self.args['SiteName']
        self.args['HostID']
        self.args['HostName']
        self.args['se-name']  
        self.args['ce-name']                    
        """
        
        return getLessBusySiteAndNode()
    
registerPlugin(LoadBalanceAllocationPlugin, LoadBalanceAllocationPlugin.__name__)