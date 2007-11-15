#!/usr/bin/env python
"""
_GeneratorInterface_

Common API for Job Generator Plugins

"""




class GeneratorInterface:
    """
    _GeneratorInterface_

    Standard Interface for Job Generator Plugins

    """
    def __init__(self):
        self.creator = None
        self.workflowCache = None
        self.workflowFile = None
        self.jobCache = None
        self.componentConfig = {}





    def actOnWorkflowSpec(self, workflowSpec, workflowCache):
        """
        _actOnWorkflow_

        The JobCreator Component responds by generating a
        workflow cache area for each new workflow it encounters.

        It will invoke this interface and call it with the cache
        area and workflow spec instance

        """
        pass

    def actOnJobSpec(self, jobSpec, jobCache):
        """
        _actOnJobSpec_

        For each job spec to be created, the spec instance and the
        cache area to create the job is passed to this method.

        """
        pass

    
        

    
