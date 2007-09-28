#!/usr/bin/env python

import time
from popen2 import Popen4
import select
import fcntl
import string
import os
import signal
import re
from ProdAgent.WorkflowEntities import JobState
import logging
import shutil
from ProdAgentCore.ProdAgentException import ProdAgentException

from BossSession import BossSession, BossAdministratorSession, BossError

from time import sleep

class BOSS:

    """
    A static instance of this class deals with job status operations
    """
    bossCfgDir = ''
    def __init__(self):
        """
        Attention: in principle, only class variables should be initialized
        here.
        """
        
        pass

    @classmethod
    def setBossCfgDir(cls, path):
        """
        Set the configuration directory for static class BOSS
        """
        cls.bossCfgDir = path

    @classmethod
    def getBossCfgDir(cls):
        """
        Get the BOSS configuration dir
        """
        return cls.bossCfgDir

    @classmethod
    def getBossSession(cls):
        """
        gets a BOSS session.
        """

        bossSessionOk = False

        while not bossSessionOk:
            try:

                # create session
                bossSession = BossSession(cls.bossCfgDir)
                bossSessionOk = True

            # BOSS error
            except BossError, e:
                logging.info("BOSS Error : " + e.__str__())
                logging.info("Waiting 30 seconds to try to get a session...")
                sleep(30)

        # return session
        return bossSession

    @classmethod
    def getBossAdminSession(cls):
        """
        gets a BOSS session.
        """

        bossSessionOk = False

        while not bossSessionOk:
            try:

                # create session
                adminSession = BossAdministratorSession(cls.bossCfgDir)
                bossSessionOk = True

            # BOSS error
            except BossError, e:
                logging.info("BOSS Error : " + e.__str__())
                logging.info("Waiting 30 seconds to try to get a session...")
                sleep(30)

        # return session
        return adminSession


    @classmethod
    def performBossQuery(cls, adminSession, query):
        """
        execute a BOSS query.
        """

        queryOk = False

        while not queryOk:
            try:

                # perform query
                out = adminSession.SQL(query)
                queryOk = True

            # BOSS error, assume connection problems
            except BossError, e:

                logging.info("BOSS query error: " + e.__str__() + \
                             ", trying to recreate session")
                adminSession = cls.getBossSession()

        # return session and query results
        return (adminSession, out)











def checkSuccess(jobId, bossCfgDir):
    success = False
    try:
        taskid  = jobId.split('.')[0]
        chainid = jobId.split('.')[1]
        resub   = jobId.split('.')[2]
        #print resub
    except IndexError:
        logging.error("Boss4 JobId splitting error: " + jobId)
        return success

    adminSession = BOSS.getBossAdminSession()
    
    try:

        query = \
              "select CHAIN_PROGRAM_TYPE FROM CHAIN where TASK_ID=" + \
              taskid + " and id = " + chainid

        # execute query
        (adminSession, out) = BOSS.performBossQuery(adminSession, query)

        outp = out.split("CHAIN_PROGRAM_TYPE")[1].strip()
    except:
        return success
    if outp.find("crabjob") >= 0 :
        return checkCrabSuccess(jobId, bossCfgDir)
    else:
        
        try:
            query = \
                  "select TASK_EXIT FROM ENDED_cmssw where TASK_ID=" + \
                  taskid + " and CHAIN_ID=" + chainid + " and ID=" + resub
            
            (adminSession, out) = BOSS.performBossQuery(adminSession, query)
            
            outp = out.split("TASK_EXIT")[1].strip()
            # print outp
        except:
        
            return success


        
    success = (outp == "0")
    return success



def checkCrabSuccess(jobId, bossCfgDir ):
    #print "CRAB"
    success = False
    
    taskid  = jobId.split('.')[0]
    chainid = jobId.split('.')[1]
    resub   = jobId.split('.')[2]
    
    
    try:
        query  = \
              "select EXE_EXIT_CODE,JOB_EXIT_STATUS FROM ENDED_crabjob" + \
              " where TASK_ID=" + taskid + " and CHAIN_ID=" + chainid + \
              " and ID=" + resub
        
        # get a BOSS session
        adminSession = BOSS.getBossAdminSession()

        # execute query
        (adminSession, out) = BOSS.performBossQuery(adminSession, query)

        outp = out.split('JOB_EXIT_STATUS')[1].strip()
    except:
        
        return success
    
    try:
        exeCode = outp.split()[0].strip()
        jobCode = outp.split()[1].strip()
        
    except:
        return success
    success = (exeCode == "0" and jobCode == "0")
    return success

def resubmit(jobId, bossCfgDir):
    """
    BOSSsubmit
    
    BOSS command to submit a task
    """

    task  = jobId.split('.')[0]
    chain = jobId.split('.')[1]
    bossSubmit = "boss submit "
    bossSubmit += "-taskid %s " % task
    bossSubmit += "-jobid %s " % chain
    bossSubmit += " -c " + bossCfgDir + " "
    return bossSubmit


def submit(jobId, scheduler, bossCfgDir):
    """
    BOSSsubmit
    
    BOSS command to submit a task
    """
    
    bossSubmit = "boss submit "

    ids = jobId.split(".")
    taskid = ""
    try:
        taskid = ids[0]
    except:
        raise ProdAgentException("Missing BOSS taskid")
    try:
        bossSubmit += "-jobid %s " % ids[1]
    except:
        pass
    
    bossSubmit += "-taskid %s " % taskid
    bossSubmit += "-scheduler %s " %  scheduler
    bossSubmit += " -c " + bossCfgDir + " "
    return bossSubmit


def getTaskIdFromName(taskName, bossCfgDir):
    """
    getTaskIdFromName

    get TaskId from TaskName
    """
    
    try:
        # set BOSS path
        BOSS.setBossCfgDir(bossCfgDir)

        # build query    
        query = \
              "select MAX(ID) ID from TASK where TASK_NAME='" + taskName + "'"

        # get a BOSS session
        adminSession = BOSS.getBossAdminSession()

        # execute query
        (adminSession, out) = BOSS.performBossQuery(adminSession, query)

        outp = out.split('ID')[1].strip()
    except:
        return 0
    return outp


def chainTemplate(parameters, bossJobType):
    """
    BOSS4createXML

    BOSS 4 command to declare a task
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


def getIdFromJobName(bossCfgDir, JobName):
    """
    _getIdFromJobName___
    
    If this job has been declared to BOSS, return the BOSS ID
    from the cache area. If it has not, return None
    
    """

    # set BOSS path
    BOSS.setBossCfgDir(bossCfgDir)

    # build query      
    query = \
           "select TASK_ID,ID from CHAIN where NAME='" + JobName + "'"

    # get a BOSS session
    adminSession = BOSS.getBossAdminSession()

    # execute query
    (adminSession, out) = BOSS.performBossQuery(adminSession, query)
    
    outf = out.strip().split("\n")
    try:
        if outf[0] == "" :
            jobId = ""
        elif len( outf ) == 2 :
            jobId = outf[1].strip()
            jobId = jobId[0:jobId.find(' ')] + '.' + jobId[jobId.rfind(' ')+1:]
        elif len( outf ) > 1 :
            logging.debug(outf)
            raise  (ProdAgentException( "job declared %d times" % len( outf ) ))
        else:
            outf = outf[1].split()
            jobId = "%s.%s"% (outf[0], outf[1] )
    except ValueError:
        jobId = ""

    return jobId


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


def declareBulk(bossCfgDir, jobList, inpSandbox, workingDir , workflow ):
    """
    BOSS4declareBulk

    BOSS 4 command to declare a task from a list of jobSpec paths
    """

    # xml file name
    xmlfile = "%s/%s-declare.xml"% ( workingDir , workflow )
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
    wrapperName = "%s/%s-submit" % (workingDir, workflow)
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
    except StandardError, ex:
#        logging.debug(
#            "SubmitterInterface:BOSS Job ID: %s. BossJobId set to 0\n" \
#            % bossJobId
#            )
        raise ProdAgentException("Job Declaration Failed")
    #os.remove(xmlfile)
    return bossJobId


def subdir(jobId, bossCfgDir):
    """
    _BOSS4subdir_

    This function retrieve job sub dir
    """
    try:
        taskid  = jobId.split('.')[0]
        chainid = jobId.split('.')[1]
        resub   = jobId.split('.')[2]
    except IndexError:
        logging.error("Boss4 JobId splitting error: " + jobId)
        return ""

    try:
        # set BOSS path
        BOSS.setBossCfgDir(bossCfgDir)

        # build query
        query = "select SUB_PATH from TASK where ID=" + taskid

        # get a BOSS session
        adminSession = BOSS.getBossAdminSession()

        # execute query
        (adminSession, out) = BOSS.performBossQuery(adminSession, query)

    except:
        out = ""

    try:
        out = out.split("SUB_PATH")[1].strip()
    except:
        out = ""
    #logging.debug("BOSS4subdir outp '%s'"%outp)    
    return out



def schedulerInfo( bossCfgDir, jobId, ended = "" ):
    """
    _BOSS4schedulerInfo_
    
    Retrieves Scheduler info
    """

    schedinfo = {}
    
    try:
        taskid  = jobId.split('.')[0]
        chainid = jobId.split('.')[1]
        resub   = jobId.split('.')[2]
    except IndexError:
        logging.error("Boss4 JobId splitting error: " + jobId)
        return schedinfo
    
    # set BOSS path
    BOSS.setBossCfgDir(bossCfgDir) 
    
    # get a BOSS session
    adminSession = BOSS.getBossAdminSession()
    
    # build query
    query = \
          "select SCHEDULER,SCHED_ID from " + ended + " JOB where TASK_ID=" \
          + taskid + " and CHAIN_ID="  + chainid + " and ID=" + resub 
    
    # execute query
    (adminSession, out) = BOSS.performBossQuery(adminSession, query)

    try :
        schedI = out.split("\n")[1].strip().split()
        jobScheduler = schedInfo[0]
        schedinfo ['SCHED_ID'] = schedInfo[1]
    except IndexError:
        logging.error("Boss4 retrieving scheduler information error: " + out)
        return schedinfo

    # build query 
    query = \
          "select * from " + ended + "SCHED_" + jobScheduler + \
          " where TASK_ID=" + taskid + \
          " and CHAIN_ID=" + chainid + " and ID=" + resub
    
    # execute query
    (adminSession, out) = BOSS.performBossQuery(adminSession, query)
        
    try:
        ks = out.split("\n")[0].strip().split()
        vs = out.split("\n")[1].strip().split()
        for i in range(len(ks)):
            schedinfo[ks[i]] = vs[i]
    except:
        logging.debug("BOSS4schedulerInfo out " + schedinfo.__str__() )
            
    return schedinfo



def executeCommand( command, timeout = 600, userProxy = "" ):
    """
    _executeCommand_

    Util it execute the command provided in a popen object with a timeout

    """

    if userProxy != "" or userProxy != 'NULL':
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
    while 1:
        (r, w, e) = select.select([outfno], [], [], timeout)
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
    except:
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
        except:
            pass
        ## be nice, children signal their parents mostly
        time.sleep(float(1))




def getUserProxy( adminSession, taskid ):
    
    # get task information 
    query = 'select TASK_INFO from TASK t where ID=' + taskid
    
    # execute query
    (adminSession, out) = BOSS.performBossQuery(adminSession, query)
    cert = out.split()[1]

    if cert == 'NULL' or cert == '':
        cert = ''
        pass
    
    # check certificate
    elif os.path.exists(cert):
        logging.debug("Using %s" % str(cert))
        pass

    # wrong certificate, using default
    else:
        logging.error("cert path " + cert + "does not exists: " \
                      + "trying to use the default one if there")
        cert = ""

    return cert


def loggingInfo( jobId, directory, bossCfgDir ):
    """
    perform scheduler listmatch

    """

    try:
        taskid  = jobId.split('.')[0]
        chainid = jobId.split('.')[1]
        resub   = jobId.split('.')[2]
    except IndexError:
        logging.error("Boss4 JobId splitting error: " + jobId)
        return

    # set BOSS path
    BOSS.setBossCfgDir(bossCfgDir)
    
    # build query
    query = \
          "select SCHEDULER,SCHED_ID from JOB where TASK_ID=" + taskid + \
          " and CHAIN_ID="  + chainid + " and ID=" + resub  
    
    # get a BOSS session
    adminSession = BOSS.getBossAdminSession()
    
    # execute query
    (adminSession, out) = BOSS.performBossQuery(adminSession, query)

    try :
        schedInfo = out.split("\n")[1].strip().split()
        jobScheduler = schedInfo[0]
        schedId = schedInfo[1]
    except IndexError:
        logging.error("Boss4 retrieving scheduler information error: " + out)
        return

    if jobScheduler == "" or schedId == "" :
        return

    # EDG
    if jobScheduler == "edg" :
        command = "edg-job-get-logging-info -v 2 " + schedId + \
                  " > " + directory + "/edgLoggingInfo.log"
        
    elif jobScheduler.find("glite") != -1:
        command = "glite-wms-job-logging-info -v 2 " + schedId + \
                  " > " + directory + "/gliteLoggingInfo.log"
    else :
        return

    # get user proxy
    cert = getUserProxy( adminSession, taskid )

    # get log file
    executeCommand( command, userProxy = cert )

    return


def getoutput( jobId, directory, bossCfgDir ):
    """
    BOSS4getoutput

    Boss 4 command to retrieve output
    """

    # get job information
    try:
        taskid  = jobId.split('.')[0]
        chainid = jobId.split('.')[1]
        resub   = jobId.split('.')[2]
    except IndexError:
        logging.error("Boss4 JobId splitting error: " + jobId)
        return ""

    # set BOSS path
    BOSS.setBossCfgDir(bossCfgDir)

    # get a BOSS session
    adminSession = BOSS.getBossAdminSession()
    
    # get user proxy
    cert = getUserProxy( adminSession, taskid )
    
    getoutpath = "%s/BossJob_%s_%s/Submission_%s/" \
                 % (directory, taskid, chainid, resub)

    os.makedirs( getoutpath )

    outfile = executeCommand(
        "boss getOutput -outdir " + getoutpath + " -logfile " + getoutpath \
        + "/bossGetOutput.log -taskid " + taskid + " -jobid " + chainid \
        + " -c " + bossCfgDir, \
        userProxy = cert
        )

    return outfile


def reportfilename(jobId, directory):
    """
    BOSS4reportfilename

    Boss 4 command to define the correct FrameworkJobReport Location
    """
    try:
        taskid  = jobId[0].split('.')[0]
        chainid = jobId[0].split('.')[1]
        resub   = jobId[0].split('.')[2]
    except IndexError:
        logging.error("Boss4 JobId splitting error: " + jobId)
        return ""

    return "%s/BossJob_%s_%s/Submission_%s/FrameworkJobReport.xml" \
           % (directory, taskid, chainid, resub)


def jobSpecId(jobId, bossCfgDir):
    """
    BOSS4JobSpecId

    BOSS 4 command to retrieve JobSpecID from BOSS db
    """
    try:
        taskid = jobId.split('.')[0]
    except:
        return ""
    try:
        chainid = jobId.split('.')[1]
    except:
        #logging.error("Boss4 JobSpecId splitting error")
        chainid = "1"


    # build query 
    query = \
          "select NAME from CHAIN where TASK_ID=" + taskid + \
          " AND ID=" +  chainid
    
    
    # get a BOSS session
    adminSession = BOSS.getBossAdminSession()
    
    # execute query
    (adminSession, out) = BOSS.performBossQuery(adminSession, query)

    try:
        out = out.split("NAME")[1].strip()
    except:
        out = ""
        
    return out




def schedulerId(jobId, bossCfgDir):
    """
    BOSS4schedulerId

    Boss 4 command which retrieves the scheduler used to submit job
    """
    try:
        taskid  = jobId.split('.')[0]
        chainid = jobId.split('.')[1]
        resub   = jobId.split('.')[2]
    except IndexError:
        logging.error("Boss4 JobId splitting error: " + jobId)
        return ""

    try:

        # set BOSS path
        BOSS.setBossCfgDir(bossCfgDir)

        # build query
        query = \
              "select SCHED_ID from JOB where TASK_ID=" + taskid + \
              " and CHAIN_ID="  + chainid + " and ID=" + resub  

        # get a BOSS session
        adminSession = BOSS.getBossAdminSession()

        # execute query
        (adminSession, out) = BOSS.performBossQuery(adminSession, query)

    except:
        out = ""

    try:
        out = out.split("SCHED_ID")[1].strip()
    except:
        out = ""
    #logging.debug("BOSS4schedulerId outp '%s'"%outp)    
    return out


def scheduler( jobId, bossCfgDir, ended = "" ):
    """
    BOSS4scheduler

    Boss 4 command which retrieves the scheduler used to submit job
    """

    try:
        taskid  = jobId.split('.')[0]
        chainid = jobId.split('.')[1]
        resub   = jobId.split('.')[2]
    except IndexError:
        logging.error("Boss4 JobId splitting error: " + jobId)
        return ""

    try:

        # set BOSS path
        BOSS.setBossCfgDir(bossCfgDir)

        # build query
        query = \
              "select SCHEDULER from JOB where TASK_ID=" + taskid + \
              " and CHAIN_ID="  + chainid + " and ID=" + resub              

        # get a BOSS session
        adminSession = BOSS.getBossAdminSession()

        # execute query
        (adminSession, out) = BOSS.performBossQuery(adminSession, query)

    except:
        out = ""

    try:
        out = out.split("SCHEDULER")[1].strip()
    except:
        out = ""
    #logging.debug("BOSS4scheduler outp '%s'"%outp)    

    return out


def taskEnded(jobId, bossCfgDir):
    """
    taskEnded

    This Function tests if all jobs of a Task are ended
    """
    try:
        taskid  = jobId.split('.')[0]
        chainid = jobId.split('.')[1]
        resub   = jobId.split('.')[2]
    except IndexError:
        logging.error("Boss4 JobId splitting error: " + jobId)
        return False

    # build query 
    query = \
          "select count(*) Jobs from CHAIN where TASK_ID=" + taskid
    
    # get a BOSS session
    adminSession = BOSS.getBossAdminSession()
    
    # execute query
    (adminSession, out) = BOSS.performBossQuery(adminSession, query)

    initialJobs = 0
    try:
        initialJobs = int(out.split("Jobs")[1])
    except:
        return False
    if initialJobs == 1:
        return True
    return False


def declareToBOSS(bossCfgDir, parameters):
    """
    _declareToBOSS_
    
    Declare this job to BOSS.
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


def FailedSubmission(jobId, bossCfgDir):
    
    taskid = jobId.split('.')[0]
    try:
        jobMaxRetries = \
                      JobState.general(jobSpecId(jobId, bossCfgDir))['MaxRetries']
        Retries = JobState.general(jobSpecId(jobId, bossCfgDir))['Retries']
    except:
        jobMaxRetries = 0
        Retries = 0
    
    if Retries >= (jobMaxRetries - 1):
        try:
            submissionDir = subdir(taskid + ".1.1", bossCfgDir)
            shutil.rmtree(submissionDir)
            Delete(jobId, bossCfgDir)
            
        except:
            pass


def archive(jobId, bossCfgDir):
    """
    BOSS4archive

    Boss 4 command to manually archive jobs in the BOSS db
    (i.e. move jobe entries to ENDED_ tables )
    """

    ids = jobId.split('.')
    
    outfile = executeCommand(
        "boss archive -taskid " + ids[0] + " -jobid " + ids[1] \
        + " -c " + bossCfgDir
        )

    return outfile


def Delete(jobId, bossCfgDir):
    """
    BOSS4Delete

    Boss 4 command to manually archive jobs in the BOSS db
    (i.e. move jobe entries to ENDED_ tables ) after setting to killed the job
    """

    ids = jobId.split('.')
    
    query = \
          "update JOB set STATUS='E',STOP_T='-1' where TASK_ID=" + ids[0] + \
          " and CHAIN_ID="  + ids[1] + " and ID=" + ids[2]

    # get a BOSS session
    adminSession = BOSS.getBossAdminSession()
    
    # execute query
    (adminSession, out) = BOSS.performBossQuery(adminSession, query)
    
    out = executeCommand(
        "boss archive -taskid " + ids[0] + " -jobid " + ids[1] \
        + " -c " + bossCfgDir
        )

    return out


