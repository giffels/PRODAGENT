#!/usr/bin/env python
"""
_BasePlugin_

Base class for plugins with standard tools embedded in it

"""
import logging
import ProdAgent.WorkflowEntities.Workflow as WEWorkflow

class BasePlugin:
    """
    _BasePlugin_

    Base class and common tools for plugins

    """
    def __init__(self):
        self.args = {}
        self.msRef = None

        
    def publishWorkflow(self, workflowPath, workflowId = None):
        """
        _publishWorkflow_

        Register Workflow Entity & Publish NewWorkflow events for the
        workflow provided

        """
        if workflowId != None:
            msg = "Registering Workflow Entity: %s" % workflowId
            logging.debug(msg)
            WEWorkflow.register(
                workflowId,
                {"owner" : "DQMInjector",
                 "workflow_spec_file" : workflowPath,
                 
                 })
            
        msg = "Publishing NewWorkflow for: %s" % workflowPath
        logging.debug(msg)
        self.msRef.publish("NewWorkflow", workflowPath)
        self.msRef.commit()
        return

