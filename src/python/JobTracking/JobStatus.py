#!/USr/bin/env python
"""
_JobStatus_

Deals with job status operations.

In principle a single instance of this class is created to call the method
doWork() from all threads in the pool.

All methods in the class can assume that no more than one thread is working
on the subset of jobs assigned to them.

"""

__revision__ = "$Id: JobStatus.py,v 1.1.2.8 2007/10/26 10:09:35 gcodispo Exp $"
__version__ = "$Revision: 1.1.2.8 $"

import logging

#from ProdAgentBOSS.BOSSCommands import BOSS, checkUserProxy
#from BossSession import BossError, SchedulerError
## MATTY
from ProdAgentDB.Config import defaultConfig as dbConfig
# Blite API import
from ProdCommon.BossLite.API.BossLiteAPI import  BossLiteAPI
from ProdCommon.BossLite.API.BossLiteAPISched import BossLiteAPISched
from ProdCommon.BossLite.Common.Exceptions import TaskError

from ProdCommon.Database.MysqlInstance import MysqlInstance
from ProdCommon.Database.SafeSession import SafeSession
from ProdCommon.Database import Session

from ProdAgentCore.ProdAgentException import ProdAgentException

#from BossSession import SUBMITTED

import os, traceback
from time import sleep
from copy import deepcopy
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
        
#        query = """
#        select distinct(g.task_id),t.TASK_INFO from jt_group g,TASK t 
#        where g.group_id=%s and g.task_id=t.ID order by t.TASK_INFO,t.ID
#        """ % str(group)
        
        query = """
        select distinct(g.task_id),t.user_proxy from jt_group g,bl_task t 
        where g.group_id=%s and g.task_id=t.ID order by t.user_proxy,t.ID
        """ % str(group)

        ## creating a new session: MATTY 
        Session.set_database(dbConfig)
        Session.connect()
        blDBinstance = BossLiteAPI('MySQL', dbConfig)
        BossLiteAdminSession = SafeSession( dbInstance = MysqlInstance(dbConfig) )

        ## MATTY
        out = []
        if (BossLiteAdminSession.execute(query) > 0):
            out = BossLiteAdminSession.fetchall()

        # process by certificate
        tasks = out #.split()[2:] # now it is a table # Fabio
        ntask = int(len(tasks)/2)
        #ntask = int(len(tasks))
        tasklist = ''
        prevcert = ''
        i = 0
        while i < ntask:
            logging.info('cycle: '+str(i)+' out of ' + str(ntask))
            task, cert = tasks[i*2]
            logging.info("   task = " + str(task) )
            logging.info("   cert = " + str(cert) )
            #logging.info("using cert: " +str(cert) )
            i += 1
            # if no proxy associated with the task
            if cert == None:
                logging.error("   -> skipping task "+str(task))
                continue
            # if same certificate group or just first entry, append
            elif cert == prevcert or tasklist == '' :
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
                #logging.info(" cert != prevcert and i == ntask")
                #logging.info("preC = " + str(prevcert))
                #logging.info("list = " + str(tasklist))
                i -= 1
            #else:
                #logging.info("else")
                #logging.info("preC = " + str(prevcert))
                #logging.info("list = " + str(tasklist))

            # evaluate valid certificates and perform the query
            try :
                #logging.info("preC = " + str(prevcert))
                #logging.info("list = " + str(tasklist))
                #logging.info("checking user proxy: " + str(prevcert))
                cls.checkUserProxy( prevcert )
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
            except Exception, exg:
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

        ## Creating a new session: MATTY
        database = deepcopy(dbConfig)
        database['dbType'] = 'mysql'
        mysqlDBInstance = MysqlInstance(database)
        #poolDb = SafePool(dbInstance, 1)

        # initialize Session
        Session.set_database(dbConfig)
        Session.connect()

        # initialize BossLite API and the session to interact with bl Database
        # TODO: Fix with the correct configurations for the DB
        BossLiteAdminSession = SafeSession( dbInstance = mysqlDBInstance)#pool = poolDb )

        # if just one task, evaluate if the size requires further splits
        if taskn == 1:
            query = "select max(job_id) from  jt_group where task_id=" \
                    + tasklist
            logging.info(str(query))

            ## MATTY
            out = None
            if (BossLiteAdminSession.execute(query) > 0):
                out = BossLiteAdminSession.fetchone()

            # define number of LB query
            ## ORRIBLE PATCH ##
            val = str(out).split(',')[0].split('(')[1].split('L')[0].strip()
	    subQuery = int( int( val ) / jobs ) + 1

        # perform the query for the task range or for the job range in the task
        for i in range ( subQuery ) :
            if subQuery > 1:
                jobRange = str( i * jobs ) + ':' + str( (i + 1) * jobs)

            logging.debug( 'LB query jobs ' + jobRange \
                           +  ' of task ' + tasklist )
            # query group of tasks
            try :
                command = 'python $PRODAGENT_ROOT/lib/JobTracking/QueryStatus.py ' \
                          + tasklist + ' ' + jobRange + ' ' \
                          + 'SchedulerGLiteAPI' + ' ' + cert
                logging.info('EXECUTING: ' + str(command))
                pin, pout = popen4( command )
                msg = pout.read()
                logging.info( "SUBPROCESS MESSAGE : " + msg )
                logging.info("LB status retrieved for jobs " + jobRange \
                             + ' of task ' + tasklist )
            except Exception,e:
                logging.error("Failed to retrieve status for jobs " \
                             + e.__str__() + ' of task ' + tasklist )
                logging.error( traceback.format_exc() )


    @classmethod
    def addNewJobs(cls):
        """
        include new jobs in the set of jobs to be watched for.

        jobs assigned: all new jobs.

        """
         
#        query = \
#              'select j.TASK_ID,j.CHAIN_ID from JOB j left join jt_group g' \
#              + ' on (j.TASK_ID=g.task_id and j.CHAIN_ID=g.job_id) ' \
#              + ' where g.job_id IS NULL and j.CHAIN_ID IS NOT NULL' \
#              + " and j.STATUS not in ('SE','SD','SA')" \
#              + ' order by j.TASK_ID'
        
        ## MATTY: replaced JOB with bl_runningjob and related field
        query = \
              'select j.task_id,j.job_id from bl_runningjob j left join jt_group g' \
              + ' on (j.task_id=g.task_id and j.job_id=g.job_id) ' \
              + ' where g.job_id IS NULL and j.job_id IS NOT NULL' \
              + " and j.status not in ('SE','SD','SA')" \
              + ' order by j.task_id'

        ## creating a new session: MATTY
        database = deepcopy(dbConfig)
        database['dbType'] = 'mysql'
        mysqlDBInstance = MysqlInstance(database)
        #poolDb = SafePool(dbInstance, 1)

        # initialize Session
        Session.set_database(dbConfig)
        Session.connect()

        # initialize BossLite API and the session to interact with bl Database
        # TODO: Fix with the correct configurations for the DB
        blDBinstance = BossLiteAPI('MySQL', database)
        BossLiteAdminSession = SafeSession( dbInstance = mysqlDBInstance )#pool = poolDb )

        try:
            ## MATTY
            out = None
            if (BossLiteAdminSession.execute(query) > 0):
                out = BossLiteAdminSession.fetchall()
            elif out is None:
                return

            # Fabio
            joblist = out #.split()[2:]
            for i in xrange(len(joblist)/2):
                query = "insert into jt_group(group_id, task_id, job_id)" + \
                        " values(''," + str(joblist[i*2][0]) + ',' + str(joblist[i*2][1]) + \
                        ') on duplicate key update group_id=group_id'

                BossLiteAdminSession.startTransaction()
                BossLiteAdminSession.execute(query)
                BossLiteAdminSession.commit()
 
                logging.debug(\
                    "Adding jobs to queue with BOSS id "\
                    +  str(joblist[i*2][0]) + '.' + str(joblist[i*2][1])\
                    )
        except StandardError, ex:
            logging.error( ex.__str__() )
            logging.error( traceback.format_exc() )


    @classmethod
    def removeFinishedJobs(cls, group):
        """
        remove all finished jobs from a specific group.

        jobs assigned: all jobs in the group

        """
        
#        query = \
#              'select g.task_id,g.job_id from JOB j right join jt_group g' \
#              + ' on (j.TASK_ID=g.task_id and j.CHAIN_ID=g.job_id) ' \
#              + ' where j.CHAIN_ID IS NULL ' \
#              + " or j.STATUS in ('SE','SD','SA')"
        
        ## MATTY: replaced JOB with bl_runningjob and related field
        query = \
              'select g.task_id,g.job_id from bl_runningjob j right join jt_group g' \
              + ' on (j.task_id=g.task_id and j.job_id=g.job_id) ' \
              + ' where j.job_id IS NULL ' \
              + " or j.status in ('SE','SD','SA')"
        ## creating a new session: MATTY
        database = deepcopy(dbConfig)
        database['dbType'] = 'mysql'
        mysqlDBInstance = MysqlInstance(database)
        #poolDb = SafePool(dbInstance, 1)

        # initialize Session
        Session.set_database(dbConfig)
        Session.connect()

        # initialize BossLite API and the session to interact with bl Database
        # TODO: Fix with the correct configurations for the DB
        blDBinstance = BossLiteAPI('MySQL', database)
        BossLiteAdminSession = SafeSession( dbInstance = mysqlDBInstance )#pool = poolDb )

        try:
            ## MATTY
            out = None
            if (BossLiteAdminSession.execute(query) > 0):
                out = BossLiteAdminSession.fetchall()
   
#            adminSession = BOSS.getBossAdminSession()
            # perform BOSS query
#            (adminSession, out) = \
#                           BOSS.performBossQuery(adminSession, query)

            # Fabio 
            joblist = out #.split()[2:]
            if joblist != None:
                for i in xrange(len(joblist)/2):
                    query = 'delete from jt_group where group_id=' + str(group) \
                            + ' and task_id=' + joblist[i*2][0] \
                            + ' and job_id=' + joblist[i*2][1]
                    #(adminSession, out) = BOSS.performBossQuery(adminSession, query)
                    BossLiteAdminSession.startTransaction()
                    BossLiteAdminSession.execute(query)
                    BossLiteAdminSession.commit()
    
                    logging.debug(
                        "Removing jobs for group " + str(group) \
                        + " with BOSS id " +  joblist[i*2][0] + '.' + joblist[i*2][1]
                        )
#        except BossError,e:
#            logging.error( "BOSS Error : " + e.__str__() )
        except StandardError, ex:
            logging.error( ex.__str__() )
            logging.error( traceback.format_exc() )

    @classmethod
    def checkUserProxy(cls, cert='' ):
        """
        Retrieve the user proxy for the task
        """

        command = 'voms-proxy-info'
 
        if cert != '' :
            command += ' --file ' + cert
        import commands
        statuso, output = commands.getstatusoutput( command )

        try:
            output = output.split("timeleft  :")[1].strip()
        except IndexError:
            logging.error(output)
            logging.error("user proxy does not exist")
            raise ProdAgentException("Missing Proxy")

        if output == "0:00:00":
            logging.error(output)
            logging.error("user proxy expired")
            raise ProdAgentException("Proxy Expired")

