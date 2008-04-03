#!/usr/bin/env python
"""
_BOSSCommands_

A set of function to deal with BOSS features
and in general with OS and scheduler features

"""

__revision__ = "$Revision: 1.14.2.10 $"
__version__ = "$Id: BOSSCommands.py,v 1.14.2.10 2008/04/02 15:26:13 gcodispo Exp $"

import time
from popen2 import Popen4
import select
import fcntl
import string
import os
import signal
import re
from ProdAgent.WorkflowEntities import JobState
from ProdAgent.WorkflowEntities import Job as WEJob
import logging
import shutil
from ProdAgentCore.ProdAgentException import ProdAgentException

# BossLite support 
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
from ProdCommon.BossLite.Scheduler import Scheduler
from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task

from ProdCommon.Database.MysqlInstance import MysqlInstance
from ProdCommon.Database.SafeSession import SafeSession
from ProdCommon.Database.SafePool import SafePool
from ProdCommon.Database import Session

from time import sleep

class directDB:

    """
    A static instance of this class deals with job status operations
    """

    def __init__(self):
        """
        Attention: in principle, only class variables should be initialized
        here.
        """
        
        pass

    @classmethod
    def getDbSession(cls):
        """
        gets a session.
        """
        dbInstance = MysqlInstance(dbConfig)
        session = SafeSession(dbInstance = dbInstance)
        return session

    @classmethod
    def select(cls, dbSession, query):
        """
        execute a query.
        """

        if (dbSession.execute(query) > 0):
            out = dbSession.fetchall()
        else :
            out = None

        # return query results
        return out

    @classmethod
    def selectOne(cls, dbSession, query):
        """
        execute a query.with only one result expected
        """

        if (dbSession.execute(query) > 0):
            out = dbSession.fetchone()[0]
        else :
            out = None

        # return query results
        return out


    @classmethod
    def modify(cls, dbSession, query):
        """
        execute a query which does not return such as insert/update/delete
        """

        # return query results
        dbSession.execute( query )
        dbSession.commit()

    @classmethod
    def close(cls, dbSession):
        """
        close session.
        """
        dbSession.close()


def fullId( job ):
    """
    compose job primary keys in a string
    """

    return str( job['taskId'] ) + '.' \
           + str( job['jobId'] ) + '.' \
           + str( job['submissionNumber'] )


def guessDashboardInfo(job, bossLiteSession):
    """
    guess dashboard info file
    """

    # dashboard information
    from ShREEK.CMSPlugins.DashboardInfo import DashboardInfo
    dashboardInfo = DashboardInfo()

    jobSpecId = job['name']

    # define dashboard file name
    try :
        jobCacheDir = JobState.general(jobSpecId)['CacheDirLocation']
        logging.info("js cache_dir = " + jobCacheDir )
    except StandardError:
        logging.info("failed to get cache_dir from js, trying with we" )
        try :
            WEjobState = WEJob.get( jobSpecId )
            jobCacheDir = WEjobState['cache_dir']
            logging.info("we cache_dir = " + jobCacheDir )
        except StandardError:
            logging.info("failed to get cache_dir from we" )
            logging.info("failed to get cache_dir for job " + jobSpecId)
            return dashboardInfo, ''
    dashboardInfoFile = os.path.join( jobCacheDir, "DashboardInfo.xml" )

    # check it
    if os.path.exists(dashboardInfoFile):

        try:
            # it exists, get dashboard information
            dashboardInfo.read(dashboardInfoFile)

            # get rid of old info, keep just the identifier
#            dashboardInfo.clear()
            
            # it does not work, abandon
        except StandardError, msg:
            logging.error("Reading dashboardInfoFile " + \
                          dashboardInfoFile + " failed (jobId=" \
                          + fullId( job ) + ")\n" + str(msg))
            return dashboardInfo, ''
        except:
            return dashboardInfo, ''

    else :
        # if this is a crab job read from mlCommonInfo
        tmpdict = {}
        try:
            task = bossLiteSession.loadTask(job['taskId'])
            mlInfoFile = task['startDirectory'].split('.boss_cache' )[0] + \
                         '/mlCommonInfo'
            logging.info( "guessing dashboardID from " + mlInfoFile )
            fh = open( mlInfoFile, 'r' )
            for line in fh.readlines() :
                (tag, value) = line.split(':')
                tmpdict[ tag ] = value.strip()
        except IOError:
            logging.error( "Missing " + mlInfoFile )
            # guess job dashboardID

        try :
            dashboardInfo.task = tmpdict['taskId']
            dashboardInfo.job = ''
        except KeyError:
            logging.error( "unable to get dashboardID for job : " + fullId( job ) )
            return dashboardInfo, ''
        try :
            dashboardInfo['JSTool'] =  tmpdict['tool']
        except KeyError:
            pass
        try :
            dashboardInfo['User'] = tmpdict['user']
        except KeyError:
            pass
        try :
            dashboardInfo['JSToolUI'] = tmpdict['tool_ui']
        except KeyError:
            pass
        try :
            dashboardInfo['TaskType'] = tmpdict['taskType']
        except KeyError:
            pass

    return dashboardInfo, dashboardInfoFile


#####################################
#
# OLD STUFF : need to be evaluated for submitters
#
#####################################



def resubmit(jobId, bossCfgDir):
    """
    BOSSsubmit
    
    BOSS command to resubmit jobs from a task
    """

    task  = jobId.split('.')[0]
    chain = jobId.split('.')[1]
    bossSubmit = "boss submit "
    bossSubmit += "-taskid %s " % task
    bossSubmit += "-jobid %s " % chain
    bossSubmit += " -reuseclad -c " + bossCfgDir + " "

    try:
        # get a BOSS session
        adminSession = BOSS.getBossAdminSession()
    
        # get user proxy
        cert = getUserProxy( adminSession, task )
    except StandardError :
        cert = ""

    return bossSubmit, cert


def chainTemplate(parameters, bossJobType):
    """
    BOSS4createXML

    create an xml declare a task in BOSS
    """

    chain = "   <chain name=\"%s\">\n" % parameters['JobName']
    chain += "      <program>\n"
    chain += "         <exec>          <![CDATA[ %s ]]> </exec>\n" \
                      % os.path.basename(parameters['Wrapper'] )
    chain += "         <args>          <![CDATA[ %s ]]> </args>\n" \
                      % parameters['JobName']
    chain += "         <stdin>         <![CDATA[ "" ]]> </stdin>\n"
    chain += "         <stdout>        <![CDATA[ %s.stdout ]]> </stdout>\n" \
                      % parameters['JobName']
    chain += "         <stderr>        <![CDATA[ %s.stderr ]]> </stderr>\n" \
                      % parameters['JobName']
    chain += "         <program_types> <![CDATA[ %s ]]> </program_types>\n" \
                      % bossJobType
    chain += "         <infiles>       <![CDATA[ %s,%s ]]> </infiles>\n" \
                      % (parameters['Wrapper'],parameters['Tarball'] )
    chain += "         <outfiles>      <![CDATA[ *.root,%s.stdout,%s.stderr,FrameworkJobReport.xml ]]> </outfiles>\n" \
                      % (parameters['JobName'], parameters['JobName'])
    chain += "      </program>\n"
    chain += "   </chain>\n"
    
    return chain



def declare(bossCfgDir, parameters):
    """
    BOSS4declare

    BOSS 4 command to declare a task
    """


    # jobType retrieval
    bossQuery = "boss showProgramTypes -c " + bossCfgDir
    queryOut = executeCommand(bossQuery)
    bossJobType = "cmssw"
    if queryOut.find("cmssw") < 0:
        bossJobType = ""

    xmlfile = "%s/%sdeclare.xml"% (
        os.path.dirname(parameters['Wrapper']) , parameters['JobName']
        )

    bossDeclare = "boss declare -xmlfile " + xmlfile + "  -c " + bossCfgDir
    declareClad = open(xmlfile,"w")
    declareClad.write(
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n"
        )

    declareClad.write("<task name=\"%s\">\n" \
                      % parameters['JobSpecInstance'].payload.workflow )
    declareClad.write( chainTemplate(parameters, bossJobType) )
    declareClad.write("</task>\n")
    
    declareClad.close()

    #  //
    # // Do BOSS Declare
    #//
    bossJobId = executeCommand(bossDeclare)
    #logging.debug( bossJobId)
    try:
        bossJobId = bossJobId.split(":")[1].split("\n")[0].strip()
    except StandardError:
#        logging.debug(
#            "SubmitterInterface:BOSS Job ID: %s. BossJobId set to 0\n" \
#            % bossJobId
#            )
        raise ProdAgentException("Job Declaration Failed")

    if (bossJobId == "") or ( bossJobId == "None" ) :
        raise ProdAgentException(
            "Job Declaration Failed : issuing %s" % bossDeclare
            )
    return bossJobId


def declareBulk(bossCfgDir, jobList, inpSandbox, workingDir, workflow, mainJobSpecName):
    """
    BOSS4declareBulk

    BOSS 4 command to declare a task from a list of jobSpec paths
    """

    # xml file name
    xmlfile = "%s/%s-declare.xml"% ( workingDir , mainJobSpecName )
    declareClad = open( xmlfile,"w" )
    declareClad.write(
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n"
        )
    declareClad.write( "<task name=\"%s\">\n" % workflow )

    # jobType retrieval
    bossQuery = "boss showProgramTypes -c " + bossCfgDir
    queryOut = executeCommand(bossQuery)
    bossJobType = "cmssw"
    if queryOut.find("cmssw") < 0:
        bossJobType = ""

    # wrapper filename
    wrapperName = "%s/%s-submit" % (workingDir, mainJobSpecName )
    parameters = { 'Wrapper' : wrapperName }

    jobSpecUsedList = []

    for jobSpecId, cacheDir in jobList.items():
        if len(jobSpecId) == 0 or  jobSpecId in jobSpecUsedList : continue
        jobSpecUsedList.append( jobSpecId )
        parameters['JobName'] = jobSpecId
        parameters['Tarball'] = inpSandbox
        declareClad.write( chainTemplate(parameters, bossJobType) )

    declareClad.write("</task>\n")
    declareClad.close()

    # actual BOSS declaration
    bossDeclare = "boss declare -xmlfile " + xmlfile + "  -c " + bossCfgDir
    bossJobId = executeCommand(bossDeclare)
#    print bossJobId
    logging.info( "BOSS DECLARATION " +  bossJobId)
    try:
        #bossJobId = bossJobId.split("TASK_ID:")[1].split("\n")[0].strip()
        logging.info( bossJobId.split(":")[1])
        logging.info( bossJobId.split(":")[1].split("\n")[0])
        bossJobId = bossJobId.split(":")[1].split("\n")[0].strip()
#        print "bossJobId", bossJobId
    except StandardError:
#        logging.debug(
#            "SubmitterInterface:BOSS Job ID: %s. BossJobId set to 0\n" \
#            % bossJobId
#            )
        raise ProdAgentException("Job Declaration Failed")

    os.remove(xmlfile)
    return bossJobId


def declareToBOSS(bossCfgDir, parameters):
    """
    _declareToBOSS_
    
    Writes a file to allow an outside-boss knolwledge of the job
    Parameters are extracted from this instance
    
    """

    bossJobId = declare(bossCfgDir, parameters)

    idFile = "%s/%sid" \
             % (os.path.dirname(parameters['Wrapper']), parameters['JobName'])

    handle = open(idFile, 'w')
    handle.write("JobId=%s" % bossJobId)
    handle.close()

    return


def isBOSSDeclared(Wrapper, JobName):
    """
    _isBOSSDeclared_
    
    If this job has been declared to BOSS, return the BOSS ID
    from the cache area. If it has not, return None
    
    """
    idFile = "%s/%sid" % (os.path.dirname(Wrapper), JobName)

    if not os.path.exists(idFile):
        #  //
        # // No BOSS Id File ==> not declared
        #//
        return None
    content = file(idFile).read().strip()
    content = content.replace("JobId=", "")
    try:
        jobId = content
    except ValueError:
        jobId = None
    return jobId


def executeCommand( command, timeout = 600, userProxy = "" ):
    """
    _executeCommand_

    Util it execute the command provided in a popen object with a timeout

    """

    if userProxy != "" and userProxy != 'NULL':
        logging.info("export X509_USER_PROXY=" + userProxy + " ; " + command)
        command = "export X509_USER_PROXY=" + userProxy + " ; " + command
    
#    f.write( command)
    p = Popen4(command)
    p.tochild.close()
    outfd = p.fromchild
    outfno = outfd.fileno()
#    f.write("\npoint 1")
    
#     signal.signal(signal.SIGCHLD,signal.SIG_IGN)
    fl = fcntl.fcntl(outfd, fcntl.F_GETFL, 0)
#    f.write("\npoint 2")
    fcntl.fcntl(outfd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
#    f.write("\npoint 2")
    err = -1
    outc = []
    outfeof = 0
    maxt = time.time() + timeout
    #logging.debug("from time %d to time %d"%(time.time(),maxt))
    pid = p.pid
    #logging.debug("process id of %s = %d"%(command,pid))
    timeout = max(1, timeout/10 )
#    f.write("timeout=%s"%timeout)
    timedOut = True
    outch = None
    while 1:
        try :
            (r, w, e) = select.select([outfno], [], [], timeout)
        except :
            break
        if len(r) > 0:
            outch = outfd.read()
            if outch == '':
                timedOut = False
                break
            outc.append(outch)
            # f.write("outch=%s"%outch)
        if time.time() > maxt:
            break

    del( p )
    # time.sleep(.1)

    if timedOut:
        #logging.error("command %s timed out. timeout %d\n"%(command,timeout))
        # f.write("timedOut")
        os.kill(pid, signal.SIGTERM)
        stoppid(pid, signal.SIGTERM)
        return ""
#    if err > 0:
 #       logging.error("command %s gave %d exit code"%(command,err))
    #    p.wait()
        #ogging.error(p.fromchild.read())

        #eturn ""
        
    try:
        output = string.join(outc,"")
    except StandardError :
        output = ""
    #logging.debug("command output \n %s"%output)
    #print "command output \n %s"%output
    # f.write("output=%s"%output)
    # f.close()
    return output


# Stijn suggestion for child processes. Thanks.
def stoppid(pid, sig):
    """
    stoppid

    Function to find and kill child processes.
    """
    parent_id = []
    done = []
    parent_id.append( str(pid) )

    ## collect possible children

    regg = re.compile(r"\s*(\d+)\s+(\d+)\s*$")
    while len(parent_id)>0:
        pi = parent_id.pop()
        done.append(pi)
        ## not on 2.4 kernels
        ## cmd= "ps -o pid --ppid "+pi
        cmd = "ps -axo pid,ppid"
        out = Popen4(cmd)
        for line in out.fromchild.readlines():
            line = line.strip('\n')
            if regg.search(line) and (regg.search(line).group(2) == pi):
                pidfound = regg.search(line).group(1)
                parent_id.append(pidfound)
        out.fromchild.close()
    ## kill the pids
    while len(done) > 0 :
        nextpid = done.pop()
        try:
            os.kill(int(nextpid), sig)
        except StandardError :
            pass
        ## be nice, children signal their parents mostly
        time.sleep(float(1))




def checkUserProxy( cert='' ):
    """
    Retrieve the user proxy for the task
    """

    command = 'voms-proxy-info'

    if cert != '' :
        command += ' --file ' + cert

    output = executeCommand( command, userProxy = cert )

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



def taskEnded(jobId, bossCfgDir):
    """
    taskEnded

    This Function tests if all jobs of a Task are ended
    """

    taskid  = jobId.split('.')[0]

    # build query 
    query = \
          "select count(*) Jobs from CHAIN where TASK_ID=" + taskid

    # set BOSS path
    BOSS.setBossCfgDir(bossCfgDir)

    # get a BOSS session
    adminSession = BOSS.getBossAdminSession()
    
    # execute query
    (adminSession, out) = BOSS.performBossQuery(adminSession, query)

    initialJobs = 0
    try:
        initialJobs = int(out.split("Jobs")[1])
    except StandardError :
        return False
    if initialJobs == 1:
        return True
    return False


def FailedSubmission(jobId, bossCfgDir):
    """
    taskEnded

    Handles a failed submission, removing task files if no more needed
    """
    
    taskid = jobId.split('.')[0]
    try:
        jobMaxRetries = \
                      JobState.general(jobSpecId(jobId, bossCfgDir))['MaxRetries']
        Retries = JobState.general(jobSpecId(jobId, bossCfgDir))['Retries']
    except StandardError :
        jobMaxRetries = 10
        Retries = 0

    if Retries >= (jobMaxRetries) and taskEnded(jobId, bossCfgDir):
        try:
            submissionDir = subdir(taskid + ".1.1", bossCfgDir)
            shutil.rmtree(submissionDir)
            Delete(jobId, bossCfgDir)
            
        except StandardError :
            pass



