#!/usr/bin/env python
"""
_PrioritiserInterface_

Define interface for Prioroty algorithm plugins



"""
import logging
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session
from JobQueue.JobQueueDB import JobQueueDB


class PrioritiserInterface:
    """
    _PrioritiserInterface_

    Base class for all Prioritiser plugins.

    Objects should inherit this and override the prioritise method

    """
    def __init__(self):
        self.matchedJobs = []
        
    
    def findMatchedJobs(self, constraint):
        """
        _findMatchedJobs_

        Method that finds jobs matching the constraint provided
        and stores the list in self.matchedJobs
        
        """
        Session.set_database(dbConfig)
        Session.connect()
        Session.start_transaction()
        jobQ = JobQueueDB()

        
        if constraint['site'] != None:
            #  //
            # // site based job match
            #//
            site = int(constraint['site']) 
            #jobQ.loadSiteMatchData()
            jobIndices = jobQ.retrieveJobsAtSites(constraint['count'],
                                                  constraint["type"],
                                                  constraint['workflow'],
                                                  * [site])
            jobs = jobQ.retrieveJobDetails(*jobIndices)
            
            [ x.__setitem__("Site", site) for x in jobs ]
            
        else:
            #  //
            # // non-site based job match
            #//
            jobIndices = jobQ.retrieveJobs(constraint['count'],
                                           constraint["type"],
                                           constraint['workflow'])
            jobs = jobQ.retrieveJobDetails(*jobIndices)
            
            [ x.__setitem__("Site", None) for x in jobs ]

        Session.commit_all()
        Session.close_all()
        logging.info("Matched %s jobs for constraint %s" % (
            len(jobs), constraint))
        self.matchedJobs = jobs
        return


    def __call__(self, constraint):
        """
        _operator()_

        Takes the Constraint provided by the ResourcesAvailable event,
        and uses it to generate a list of resources to be created.


        """
        self.findMatchedJobs(constraint)
        try:
            jobs = self.prioritise(constraint)
        except Exception, ex:
            msg = "Error invoking Prioritiser plugin: "
            msg += "%s\n" % self.__class__.__name__
            msg += "On Constraint: %s\n" % constraint
            msg += str(ex)
            logging.error(msg)
            return

        
        return jobs
        
    
    def prioritise(self, constraint):
        """
        _prioritise_

        Given the constraint, generate a list of jobs that can be released.

        Override this method to implement whatever algorithm for utilising
        resources you want.

        This list of potential matched jobs is stored as self.matchedJobs

        Should return a (subset) list of job information dictionaries from
        self.matchedJobs that were selected for submission
        
        """
        raise NotImplementedError, "%s.prioritise" % self.__class__.__name__
    
    
