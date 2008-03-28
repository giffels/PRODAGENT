#!/usr/bin/env python
"""
_PoolScheduler_

Implements the pool thread scheduler

"""

__revision__ = "$Id: PoolScheduler.py,v 1.1.2.2 2007/11/21 11:28:37 gcodispo Exp $"
__version__ = "$Revision: 1.1.2.2 $"

from threading import Thread
from time import sleep
from sets import Set
import logging
from random import shuffle

from JobTracking.JobStatus import JobStatus
# from ProdAgentBOSS.BOSSCommands import BOSS
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
from ProdCommon.ThreadTools.WorkQueue import WorkQueue

# Fixes for BossLite
from ProdCommon.Database import Session
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database.MysqlInstance import MysqlInstance
from ProdCommon.Database.SafeSession import SafeSession

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

        # init BossLite session #Fabio
        self.database = dbConfig
        Session.set_database(dbConfig)
        Session.connect()
        self.blDBinstance = BossLiteAPI('MySQL', self.database)
        self.BossLiteAdminSession = SafeSession( dbInstance = MysqlInstance(self.database) )
 
        # start scheduler thread
        # self.setDaemon(1)
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
        ## adminSession = BOSS.getBossAdminSession()
        # already there in bossLite

        # set policy parameters
        groups = {}

        # get list of groups under processing 
        grlist = ",".join(["%s" % k for k in self.groupsUnderProcessing])

        # build query to get information about tasks associated to these
        # groups # Modified, Fabio

        query_addin = "where group_id is not null "
        if grlist != '':
            query_addin = "where group_id not in (%s) "%str(grlist)
            
        # some groups with threads working on
        query = " select task_id, count(job_id) from jt_group %s"%query_addin
        query += " group by task_id order by count(job_id) desc"

        # query BOSS for task information   
        # (adminSession, out) = BOSS.performBossQuery(adminSession, query)
        jobPerTask = []
        count = self.BossLiteAdminSession.execute(query) 
        if (count > 0):
            jobPerTask = self.BossLiteAdminSession.fetchall()

        # process all groups
        grid = 0
        while len(jobPerTask) !=0:
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
            while len(jobPerTask) != 0:
                try:
                    tj = jobPerTask[0]#.split() 
                    task = tj[0]
                    jobs = int(tj[1])

                    # stop when there are enough jobs
                    if jobsReached + int(jobs) > self.maxJobs \
                           and jobsReached != 0:
                        break

                    # add task to group
                    groups[grid] += str(task) + ','
                    jobsReached += int(jobs)

                    #MATTY's fix
                    jobPerTask = jobPerTask[1:len(jobPerTask)]
                    #jobPerTask.pop(0)

                # go to next task
                except IndexError, ex:
                    #MATTY's fix
                    jobPerTask = jobPerTask[1:len(jobPerTask)]
                    #jobPerTask.pop(0)
                    logging.info("\n\n" + str(ex) + "\n\n")
                    continue

            logging.info('filling group ' + str(grid) + \
                          ' with the smallest tasks')

            # fill group with the smallest tasks
            while len(jobPerTask) != 0:
                try:
                    tj = jobPerTask[-1]#.split()
                    task = tj[0]
                    jobs = int(tj[1])

                    # stop when there are enough jobs
                    if jobsReached + int(jobs)  > self.maxJobs:
                        break

                    # add task to group
                    groups[grid] += task + ','
                    jobsReached += int(jobs)

                    #MATTY's fix
                    jobPerTask = jobPerTask[1:len(jobPerTask)]
                    #jobPerTask.pop()

                # go to next task
                except IndexError:
                    #MATTY's fix
                    jobPerTask = jobPerTask[1:len(jobPerTask)]
                    #jobPerTask.pop()
                    continue

            logging.info("group " + str(grid) + " filled with tasks " \
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
            ##(adminSession, out) = BOSS.performBossQuery(adminSession, query)
            self.BossLiteAdminSession.startTransaction() 
            self.BossLiteAdminSession.execute(query)
            self.BossLiteAdminSession.commit() 

            logging.debug("Adding tasks " + tasks[:-1] + ' to group ' + \
                          str(group))

        # build list of groups 
        ret = groups.keys()
        ret.extend(self.groupsUnderProcessing)

        logging.debug("returning groups " + ret.__str__())

        # and return it
        shuffle( ret )
        return ret

