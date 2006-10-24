#!/usr/bin/env python
"""
_PrioritiserInterface_

Define interface for Prioroty algorithm plugins



"""
import logging
from JobQueue.JobQueueDB import retrieveJobs, eraseJobs

class PrioritiserInterface:
    """
    _PrioritiserInterface_

    Base class for all Prioritiser plugins.

    Objects should inherit this and override the prioritise method

    """
    def __init__(self):
        pass


    retrieveJobsFromDB = staticmethod(retrieveJobs)
    


    def __call__(self, constraint):
        """
        _operator()_

        Takes the Constraint provided by the ResourcesAvailable event,
        and uses it to generate a list of resources to be created.


        """
        try:
            jobs = self.prioritise(constraint)
        except Exception, ex:
            msg = "Error invoking Prioritiser plugin: "
            msg += "%s\n" % self.__class__.__name__
            msg += "On Constraint: %s\n" % constraint
            msg += str(ex)
            logging.error(msg)
            return

        toErase = []
        result = []
        for job in jobs:
            toErase.append(job['job_index'])
            result.append(job['job_spec_file'])

        eraseJobs(*toErase)
        
        return result
        
    
    def prioritise(self, constraint):
        """
        _prioritise_

        Given the constraint, generate a list of jobs that can be released.


        Override this method to implement whatever algorithm for utilising
        resources you want

        Should return a list of job information dictionaries as returned
        by the JobQueueDB.retrieveJobs method.
        
        """
        raise NotImplementedError, "%s.prioritise" % self.__class__.__name__
    
    
