#!/usr/bin/env python
"""
_PoolScheduler_

Implements the pool thread scheduler

"""

__revision__ = "$Id$"
__version__ = "$Revision$"

from threading import Thread
from time import sleep
from sets import Set
import logging

from JobTracking.JobStatus import JobStatus
from ProdAgentBOSS.BOSSCommands import BOSS

from ProdCommon.ThreadTools.WorkQueue import WorkQueue

###############################################################################
# Class: PoolScheduler                                                        #
############################################################################### 
        
class PoolScheduler(Thread):
    """
    An instance of this class performs a pool thread scheduler
    """
    
    def __init__(self, pool, params = {}):
        """
        initialize the pool instance and start the scheduler thread.
        """
        
        # initialize thread
        Thread.__init__(self)

        # store link to threads pool
        self.pool = pool

        # set control parameteres
        self.threadsWorking = 0
        try:
            self.delay = params['delay']
        except KeyError:
            self.delay = 30
        try:
            self.maxJobs = params['jobsToPoll']
        except KeyError:
            self.maxJobs = 100
        self.groupsUnderProcessing = Set([])

        # start scheduler thread
        self.setDaemon(1)
        self.start()

    def run(self):
        """
        main body of the scheduler.
        """

        logging.info("Pool scheduler started")

        # do forever
        while True:

            # get job information about new jobs
            self.getNewJobs()

            # apply policy
            groups = self.applyPolicy()

            # any job to check?
            if len(groups) == 0:

                # no, wait for jobs to arrive
                logging.info("No work to do, scheduler goes to sleep for " + \
                             str(self.delay) + " seconds")
                sleep(self.delay)
                continue

            # new threads to start?
            if len(groups) >= self.threadsWorking:

                # yes, start threads
                for grp in groups:

                    # but only for new groups
                    if grp not in self.groupsUnderProcessing:

                        # insert group ID into queue to trigger thread start
                        self.groupsUnderProcessing.add(grp)
                        self.pool.enqueue(grp, grp)

            # wait for a thread to finish
            (group, result) = self.pool.dequeue()
            logging.info("Thread processing group " + str(group) + \
                         " has finished")

            # decrement threads counter 
            self.threadsWorking = self.threadsWorking - 1
                
            # remove its ID from groups
            self.groupsUnderProcessing.remove(group)
                
            # remove all finished jobs from this group
            JobStatus.removeFinishedJobs(group)

    def getNewJobs(self):
        """
        get information about new jobs.
        """

        JobStatus.addNewJobs()


    def applyPolicy(self):
        """
        apply policy.
        """

        # get a BOSS session
        adminSession = BOSS.getBossAdminSession()

        # set policy parameters
        groups = {}

        # get list of groups under processing 
        grlist = ",".join(["%s" % k for k in self.groupsUnderProcessing])

        # build query to get information about tasks associated to these
        # groups
        if grlist == '':

            # no groups under processing
            query = """
                    select task_id, count(job_id)
                      from jt_group
                     where group_id is not null
                     group by task_id
                     order by count(job_id) desc
                     """
        else:

            # some groups with threads working on
            query = """
                    select task_id, count(job_id)
                      from jt_group where group_id not in (""" + grlist + """)
                     group by task_id
                     order by count(job_id) desc
                     """

        # query BOSS for task information   
        (adminSession, out) = BOSS.performBossQuery(adminSession, query)

        # build structure
        jobPerTask = {}
        jobPerTask = out.split('\n')[1:]

        # process all groups
        grid = 0

        while len(jobPerTask) !=0:
            grid = grid + 1

            # ignore groups under processing
            if grid in self.groupsUnderProcessing:
                logging.debug( "skipping group " + str(grid))
                continue

            # build group information
            groups[grid] = ''
            jobsReached = 0

            logging.debug('filling group ' + str(grid) + ' with largest tasks')

            # fill group with the largest tasks
            while len(jobPerTask) != 0:
                try:
                    tj = jobPerTask[0].split()
                    task = tj[0]
                    jobs = int(tj[1])

                    # stop when there are enough jobs
                    if jobsReached + int(jobs) > self.maxJobs \
                           and jobsReached != 0:
                        break

                    # add task to group
                    groups[grid] += task + ','
                    jobsReached += int(jobs)
                    jobPerTask.pop(0)

                # go to next task
                except IndexError:
                    jobPerTask.pop(0)
                    continue

            logging.debug('filling group ' + str(grid) + \
                          ' with the smallest tasks')

            # fill group with the smallest tasks
            while len(jobPerTask) != 0:
                try:
                    tj = jobPerTask[-1].split()
                    task = tj[0]
                    jobs = int(tj[1])

                    # stop when there are enough jobs
                    if jobsReached + int(jobs)  > self.maxJobs:
                        break

                    # add task to group
                    groups[grid] += task + ','
                    jobsReached += int(jobs)
                    jobPerTask.pop()

                # go to next task
                except IndexError:
                    jobPerTask.pop()
                    continue

            logging.debug("group " + str(grid) + " filled with tasks " \
                          + groups[grid] + " and total jobs " \
                          + str(jobsReached))
        
        # process all groups
        for group, tasks in groups.iteritems():

            # ignore empty tasks
            if tasks == '':
                continue

            # update group
            query = """
                    update jt_group
                       set group_id='""" + str(group) + """'
                     where task_id in (""" + tasks[:-1]  + ")"

            # perform BOSS query
            (adminSession, out) = \
                           BOSS.performBossQuery(adminSession, query)

            logging.debug("Adding tasks " + tasks[:-1] + ' to group ' + \
                          str(group))

        # build list of groups 
        ret = groups.keys()
        ret.extend(self.groupsUnderProcessing)

        logging.debug("returning groups " + ret.__str__())

        # and return it
        return ret

