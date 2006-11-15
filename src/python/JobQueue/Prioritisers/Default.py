#!/usr/bin/env python
"""
_Default_

Default prioritisation algorithm.

Works as follows:

1. Given a constraint, use contraint values to query DB for matching
jobs.

2. releases all matches to be created

3. Ignores any undershoot in resources.

"""

import logging
from JobQueue.Prioritisers.PrioritiserInterface import PrioritiserInterface
from JobQueue.Registry import registerPrioritiser



class Default(PrioritiserInterface):
    """
    _Default_

    Does a straight DB query and returns what it gets.
    

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
            sitelist = constraint['site'].split(",")
            for site in sitelist:
                if site.strip() != "":
                    sites.append(site.strip())
                    

        jobs = self.retrieveJobsFromDB(count, jobtype, workflow, *sites)
        msg = "Retrieved %s jobs matching %s" % (len(jobs), constraint)
        logging.info(msg)

        return jobs


registerPrioritiser(Default, Default.__name__)


