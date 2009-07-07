#!/usr/bin/env python
"""
_LCGAdvanced_

Advanced LCG prioritisation algorithm.


"""

import logging
from JobQueue.Prioritisers.PrioritiserInterface import PrioritiserInterface
from JobQueue.Registry import registerPrioritiser
from JobQueue.JobQueueDB import JobQueueDB
from ProdCommon.Database import Session
from ProdAgentDB.Config import defaultConfig as dbConfig

from ResourceMonitor.Monitors.WorkflowConstraints import constraintID2WFname

class LCGAdvanced(PrioritiserInterface):
    """
    _LCGAdvanced_

    Returns exactly what matches the constraint

    """
    def __init__(self):
        PrioritiserInterface.__init__(self)
        logging.info("LCGAdvanced started.")

    def findMatchedJobs(self, constraint):
        """
        _findMatchedJobs_

        Method that finds jobs matching the constraint provided
        and stores the list in self.matchedJobs

        """
        logging.debug("LCGAdvanced findMatchedJobs started.")
        Session.set_database(dbConfig)
        Session.connect()
        Session.start_transaction()
        jobQ = JobQueueDB()
        jobs = []

        ## check if JobSubmitter still needs to process jobs
        sqlStr = '''
        SELECT count(*) FROM ms_process,ms_message WHERE
          ( ms_process.procid = ms_message.dest
            AND ms_process.name IN ('JobSubmitter','JobCreator'));
        '''
        Session.execute(sqlStr)
        result = Session.fetchall()
        js_is_ok = True
        ## allowed number of messages (could be other messages
        ## for JobSubmitter)
        ## in principle this should also check for messages for
        ## JobCreator, as CreateJob messages will result in SubmitJob messages
        ## take number of jobs the JobSubmitter can handle in one
        ## ResourceMonitor:Poll interval 
        allowed_nr_of_ms = 600
        if int(result[0][0]) > allowed_nr_of_ms:
            js_is_ok = False
            msg = "LCGAdvanced: JobSubmitter still need to process "
            msg += str(result[0][0])
            msg += " messages, which is more than number of allowed messages "
            msg += str(allowed_nr_of_ms)
            msg += ". Currently not releasing anything."
            logging.info(msg)
            Session.commit_all()
            return
        
        
        #What extra code do we want here
        # is workflow max not taken into account by other methods somewhere 
        # how does PrioritiserInterface do it?
            
        constraint['workflow'] = constraintID2WFname(constraint['workflow'])

        if js_is_ok and (constraint['site'] != None):
            # site based job match
            site = int(constraint['site']) 
            
            jobIndices = jobQ.retrieveJobsAtSitesNotWorkflowSitesMax(
                constraint['count'],
                constraint['type'],
                constraint['workflow'],
                * [site])

            jobs = jobQ.retrieveJobDetails(*jobIndices)

            [ x.__setitem__("Site", site) for x in jobs ]

        else:
            ## not implemented yet
            pass

        Session.commit_all()
        Session.close_all()
        logging.info("LCGAdvanced: Matched %s jobs for constraint %s" % (
                len(jobs), constraint))
        self.matchedJobs = jobs
        return


    def prioritise(self, constraint):
        """
        _prioritise_

        Get jobs from DB matching constraint

        """
        logging.info("LCGAdvanced prioritise called.")
        return self.matchedJobs


registerPrioritiser(LCGAdvanced, LCGAdvanced.__name__)