#!/usr/bin/env python
"""
_ProcTopUp_

ProcTopUp prioritisation algorithm.

Works as follows:

1. Given a constraint, use contraint values to query DB for matching
jobs.

2. releases all matches to be created

3. If the number of jobs is less than the count, it will attempt
to get more processing jobs to top up

"""

import logging
from JobQueue.Prioritisers.PrioritiserInterface import PrioritiserInterface
from JobQueue.Registry import registerPrioritiser



class Filter:
    def __init__(self, known):
        self.known = known
    def __call__(self, entry):
        return entry['job_index'] not in self.known

class ProcTopUp(PrioritiserInterface):
    """
    _ProcTopUp_

    Does a straight DB query and trys to top up with processing jobs

    """
    def __init__(self):
        PrioritiserInterface.__init__(self)


    def prioritise(self, constraint):
        """
        _prioritise_

        Get jobs from DB matching constraint

        """
        count = constraint['count']
        jobtype = constraint['type']
        workflow = constraint['workflow']
        sites = []
        if constraint['site'] != None:
            sitelist = sites.split(",")
            for site in sitelist:
                if site.strip() != "":
                    sites.append(site.strip())


        jobs = self.retrieveJobsFromDB(count, jobtype, workflow, *sites)

        msg = "Retrieved %s jobs matching %s" % (len(jobs), constraint)
        logging.info(msg)

        knownIndices = [ i['job_index'] for i in jobs ]

        if len(jobs) < count:
            #  //
            # // Try and top up with processing jobs.
            #// 
            procJobs = self.retrieveJobsFromDB(count*2, "Processing")
            procJobs = filter(Filter(knownIndices), procJobs) 
            remainder = count - len(jobs)
            if len(procJobs) > remainder:
                # remove any excess
                procJobs = procJobs[:totalSoFar]
            jobs.extend(procJobs)
            
        return jobs


registerPrioritiser(ProcTopUp, ProcTopUp.__name__)


