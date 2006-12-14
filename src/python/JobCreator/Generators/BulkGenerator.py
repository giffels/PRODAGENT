#!/usr/bin/env python
"""

_BulkGenerator_

JobGenerator that generates a common workflow sandbox that is
parametrized on the job spec

"""
import logging
from JobCreator.GeneratorInterface import GeneratorInterface
from JobCreator.Registry import registerGenerator


class BulkGenerator(GeneratorInterface):
    """
    _BulkGenerator_

    """
    def actOnWorkflow(self, workflowSpec, workflowCache):
        """
        Create the workflow wide job template for jobs
        """
        logging.info(
            "BulkGenerator.actOnWorkflowSpec(%s, %s)" % (
               workflowSpec, workflowCache)
            )
        return


    def actOnJobSpec(self, jobSpec, jobCache):
        """
        Populate the cache for the individual JobSpec

        """
        logging.info(
            "BulkGenerator.actOnJobSpec(%s, %s)" % (jobSpec, jobCache)
            )
        jobname = jobSpec.parameters['JobName']
        jobSpec.save("%s/%s-JobSpec.xml" % (jobCache, jobname))
        

        return


registerGenerator(BulkGenerator, "Bulk")

