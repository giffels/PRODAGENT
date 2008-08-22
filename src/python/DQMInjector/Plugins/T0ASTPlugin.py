#!/usr/bin/env python
"""
_T0ASTPlugin_

Plugin to pull in files for a dataset/run from the Tier 0 DB
and generate a DQM Harvesting workflow/job

"""

from DQMInjector.Plugins.BasePlugin import BasePlugin

class T0ASTPlugin(BasePlugin):

    def __init__(self):
        BasePlugin.__init__(self)
        
        

    def __call__(self, collectPayload):
        """
        _operator(collectPayload)_

        Given the dataset and run in the payload, callout to T0AST
        to find the files to be harvested

        """
        #TODO:
        # Import T0AST Connection libs
        # Pull in RunConfig for run in payload
        # Pull in list of files for that run/dataset from T0AST
        # Generate Workflow and harvesting job spec

        pass
    
