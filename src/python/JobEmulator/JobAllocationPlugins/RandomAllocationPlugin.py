#!/usr/bin/env python
"""
_RandomAllocationPlugin_

Randomly Allocate a job

"""
__revision__ = "$Id: $"
__version__ = "$Revision: $"


from JobEmulator.JobEmulatorAPI import getRandomSiteAndNode
from JobEmulator.Registry import registerPlugin
from JobEmulator.JobAllocationPlugins.JobAllocationPluginInterface \
    import JobAllocationPluginInterface

class RandomAllocationPlugin(JobAllocationPluginInterface):
    """
    _RandomAllocationPlugin_

    Randomly Allocate a job

    """
   
    
    def allocateJob(self, jobSpec=None):
        """
        _allocateJob_

        Determine where job will be run, and return getRandomSiteAndNode
        randomly allocating job among 50 nodes       
        provide following information
        self.args['SiteName']
        self.args['HostID']
        self.args['HostName']
        self.args['se-name']  
        self.args['ce-name']                    
        """
        
        return getRandomSiteAndNode()
    
registerPlugin(RandomAllocationPlugin, RandomAllocationPlugin.__name__)