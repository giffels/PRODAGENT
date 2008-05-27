#!/usr/bin/env python
"""
_PoolScheduler_

Implements the pool thread scheduler

"""

__revision__ = "$Id: PoolScheduler.py,v 1.1.2.8 2008/04/29 08:39:29 gcodispo Exp $"
__version__ = "$Revision: 1.1.2.8 $"

from threading import Thread
from time import sleep
from sets import Set
import logging
from random import shuffle

from JobTracking.JobStatus import JobStatus
from JobTracking.TrackingDB import TrackingDB
from ProdCommon.ThreadTools.WorkQueue import WorkQueue
from ProdCommon.BossLite.API.BossLiteDB import BossLiteDB

###############################################################################
# Class: PoolScheduler                                                        #
###############################################################################

class PoolScheduler(Thread):
    """
    An instance of this class performs a pool thread scheduler
    """

    def __init__(self, pool, params = None):
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
        try:
            self.database = params['dbConfig']
        except KeyError:
            from ProdAgentDB.Config import defaultConfig as dbConfig
            self.database = dbConfig
        self.groupsUnderProcessing = Set([])
        self.jobPerTask = None

        # start scheduler thread
        self.setDaemon(1)
        self.start()

    def run(self):
        """
        __run__

        main body of the scheduler.
        """

        logging.info("Pool scheduler started")

        # do forever
        while True:

            # get job information about new jobs
            self.getNewJobs()

            # apply policy
            try:
                groups = self.applyPolicy()
            except:
                import traceback
                logging.error("Policy problems for thrad tokens: %s" \
                              % traceback.format_exc() )
                raise

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
        __getNewJobs__

        get information about new jobs.
        """

        JobStatus.addNewJobs()


    def applyPolicy(self):
        """
        __applyPolicy__

        apply policy.
        """

        # get DB session
        session = BossLiteDB ("MySQL", self.database )
        db = TrackingDB( session )

        # set policy parameters
        groups = {}

        # get list of groups under processing
        grlist = ",".join(["%s" % k for k in self.groupsUnderProcessing])

        # get information about tasks associated to these groups
        self.jobPerTask = db.getUnprocessedJobs( grlist )

        # process all groups
        grid = 0
        while self.jobPerTask != [] :
            grid = grid + 1

            # ignore groups under processing
            if grid in self.groupsUnderProcessing:
                logging.info( "skipping group " + str(grid))
                continue

            # build group information
            groups[grid] = ''
            jobsReached = 0

            logging.info('filling group ' + str(grid) + ' with largest tasks')

            # fill group with the largest tasks
            while self.jobPerTask != []:
                try:
                    task, jobs = self.jobPerTask[0]

                    # stop when there are enough jobs
                    if jobsReached + int(jobs) > self.maxJobs \
                           and jobsReached != 0:
                        break

                    # add task to group
                    groups[grid] += str(task) + ','
                    jobsReached += int(jobs)
                    self.jobPerTask.pop(0)

                # go to next task
                except IndexError, ex:
                    self.jobPerTask.pop(0)
                    logging.info("\n\n" + str(ex) + "\n\n")
                    continue

            logging.info('filling group ' + str(grid) + \
                          ' with the smallest tasks')

            # fill group with the smallest tasks
            while self.jobPerTask != []:
                try:
                    task, jobs = self.jobPerTask[0]

                    # stop when there are enough jobs
                    if jobsReached + int(jobs)  > self.maxJobs:
                        break

                    # add task to group
                    groups[grid] += task + ','
                    jobsReached += int(jobs)
                    self.jobPerTask.pop()

                # go to next task
                except IndexError:
                    self.jobPerTask.pop()
                    continue

            logging.info("group " + str(grid) + " filled with tasks " \
                          + groups[grid] + " and total jobs " \
                          + str(jobsReached))

        del self.jobPerTask[:]
        # process all groups
        for group, tasks in groups.iteritems():

            # ignore empty tasks
            if tasks == '':
                continue

            # update group
            db.setTaskGroup( str(group), tasks[:-1] )
            logging.debug("Adding tasks " + tasks[:-1] + ' to group ' + \
                          str(group))

        # close db session
        session.close()

        # build list of groups
        ret = groups.keys()
        ret.extend(self.groupsUnderProcessing)

        logging.debug("returning groups " + ret.__str__())

        # and return it
        shuffle( ret )
        return ret

