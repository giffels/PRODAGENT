#!/USr/bin/env python
"""
_JobStatus_

Deals with job status operations.

In principle a single instance of this class is created to call the method
doWork() from all threads in the pool.

All methods in the class can assume that no more than one thread is working
on the subset of jobs assigned to them.

"""

__revision__ = "$Id: JobStatus.py,v 1.1.2.15 2008/04/17 16:15:17 gcodispo Exp $"
__version__ = "$Revision: 1.1.2.15 $"

from ProdAgentBOSS.BOSSCommands import directDB
from GetOutput.TrackingDB import TrackingDB
from ProdCommon.BossLite.API.BossLiteAPI import parseRange
from ProdAgentCore.ProdAgentException import ProdAgentException

import traceback
import logging
from time import sleep
from os import popen4


###############################################################################
# Class: JobStatus                                                            #
###############################################################################

class JobStatus:
    """
    A static instance of this class deals with job status operations
    """

    params = {'delay' : 30, 'jobsToPoll' : 100}     # parameters

    def __init__(self):
        """
        Attention: in principle, no instance of this static class is created!
        """
        pass

    @classmethod
    def setParameters(cls, params):
        """
        set parameters
        """

        cls.params = params

    @classmethod
    def doWork(cls, group):
        """
        get the status of the jobs in the group.

        jobs assigned: all jobs in the group.

        """

        logging.info("Getting job status for jobs in group " + str(group))

        # get DB session
        session = directDB.getDbSession()
        db = TrackingDB( session )
        tasks = db.getGroupTasks(group)
        session.close()

        # process by certificate
        ntask = int(len(tasks))
        tasklist = ''
        prevcert = ''
        i = 0
        while i < ntask:
            logging.info('cycle: '+str(i)+' out of ' + str(ntask - 1))
            task, cert = tasks[i]
            i += 1

            # if no proxy associated with the task
            if cert == None:
                cert = ''

            logging.info("   task = " + str(task) )
            logging.info("   cert = " + str(cert) )
            #logging.info("using cert: " +str(cert) )

            # if same certificate group or just first entry, append
            if cert == prevcert or tasklist == '' :
                #logging.info("cert == prevcert of tasklist == ''")
                #logging.info("preC = " + str(prevcert))
                #logging.info("list = " + str(tasklist))
                tasklist += str(task) + ','
                prevcert = cert

                # if not last task, get next, otherwise process
                if i != ntask :
                    logging.info("....................")
                    continue
                
            # else if not same certificate, but anyway the last,
            # process current tasklist and step back to process the last
            elif cert != prevcert and i == ntask :
                i -= 1
                #logging.info(" cert != prevcert and i == ntask")
                #logging.info("preC = " + str(prevcert))
                #logging.info("list = " + str(tasklist))
            #else:
                #logging.info("else")
                #logging.info("preC = " + str(prevcert))
                #logging.info("list = " + str(tasklist))

            # evaluate valid certificates and perform the query
            try :
                #logging.info("preC = " + str(prevcert))
                #logging.info("list = " + str(tasklist))
                #logging.info("checking user proxy: " + str(prevcert))
                # cls.checkUserProxy( prevcert )
                #logging.info("checked.")
                tasklist = tasklist[:-1]

                # ask BOSS for LB query
                logging.info('query for tasks ' + tasklist)
                cls.bossQuery( tasklist, ntask, prevcert )
            except ProdAgentException, exc:
                logging.debug(str(exc))
                logging.info( \
                        "cert path " + prevcert + \
                        " does not exists: skipping tasks " + tasklist \
                        )
            except StandardError, exg:
                logging.error(str(exg))
                logging.error( traceback.format_exc() )

            # if reached this point, there is at least one task left
            # the current task goes anyway in the next query
            tasklist = str(task) + ','
            prevcert = str(cert)

        sleep(cls.params['delay'])


    @classmethod
    def bossQuery( cls, tasklist, taskn, cert ):
        """
        Perform the LB query through BOSS
        """

        subQuery = 1
        jobRange = "all"
        jobs = int ( cls.params['jobsToPoll'] )

        session = directDB.getDbSession()
        db = TrackingDB( session )        

        # if just one task, evaluate if the size requires further splits
        if taskn == 1:
            val = db.getTaskSize( tasklist )
            if val < jobs :
                jobs = val
            else :
                subQuery = int( int( val ) / jobs )
                if  val % jobs != 0:
                    subQuery += 1

        # close db session
        session.close()

        # perform the query for the task range or for the job range in the task
        for i in range ( subQuery ) :
            if taskn == 1:
                jobRange = str( i * jobs + 1 ) + ':' + str( (i + 1) * jobs)

            logging.debug( 'LB query jobs ' + jobRange \
                           +  ' of task ' + tasklist )
            # query group of tasks
            for taskId in parseRange( tasklist ) :
                try :
                    scheduler = db.getTaskScheduler( taskId )
                    command = \
                            'python ' + \
                            '$PRODAGENT_ROOT/lib/JobTracking/QueryStatus.py ' \
                            + str(taskId) + ' ' + jobRange + ' ' \
                            + scheduler + ' ' + cert
                    logging.info('EXECUTING: ' + str(command))
                    pin, pout = popen4( command )
                    msg = pout.read()
                    logging.info( "SUBPROCESS MESSAGE : " + msg )
                    logging.info("LB status retrieved for jobs " + jobRange \
                                 + ' of task ' + str(taskId) )
                except TypeError, e:
                    logging.error("Failed to retrieve status for jobs " \
                                  + jobRange + ' of task ' + str(taskId) \
                                  + ' : ' + str( e ) )
                except StandardError, e:
                    logging.error("Failed to retrieve status for jobs " \
                                  + jobRange + ' of task ' + str(taskId) \
                                  + ' : ' + str( e ) )
                    logging.error( traceback.format_exc() )



    @classmethod
    def addNewJobs(cls):
        """
        include new jobs in the set of jobs to be watched for.

        jobs assigned: all new jobs.

        """

        try:

            session = directDB.getDbSession()
            db = TrackingDB( session )
            joblist = db.getUnassociatedJobs()

            # in case of empty results
            if joblist is None:
                return

            for pair in joblist:
                db.addForCheck( pair[0],  pair[1] )
 
                logging.debug(\
                    "Adding jobs to queue with BOSS id "\
                    +  str( pair[0] ) + '.' + str( pair[1] )\
                    )
            session.close()

        except StandardError, ex:
            logging.error( ex.__str__() )
            logging.error( traceback.format_exc() )


    @classmethod
    def removeFinishedJobs(cls, group):
        """
        remove all finished jobs from a specific group.

        jobs assigned: all jobs in the group

        """

        try:
            session = directDB.getDbSession()
            db = TrackingDB( session )
            joblist = db.getAssociatedJobs()

            # in case of empty results
            if joblist is None:
                return

            for pair in joblist:
                db.removeFromCheck( pair[0],  pair[1] )
                logging.debug(
                    "Removing jobs for group " + str(group) \
                    + " with BOSS id " +  str( pair[0] ) + '.' \
                    + str( pair[1] )\
                    )
            session.close()
        except StandardError, ex:
            logging.error( ex.__str__() )
            logging.error( traceback.format_exc() )

