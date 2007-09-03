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
import shutil
from ProdAgentCore.ProdAgentException import ProdAgentException


def checkSuccess(id,bossCfgDir):
    success=False
    try:
        taskid=id.split('.')[0]
        chainid=id.split('.')[1]
        jobid=id.split('.')[2]
        #print jobid
    except:
        return success
    try:

        outfile=executeCommand("bossAdmin SQL -query \"select CHAIN_PROGRAM_TYPE FROM CHAIN where TASK_ID=%s and id = %s\""%(taskid,chainid)+" -c " + bossCfgDir)
        outp=outfile.split("CHAIN_PROGRAM_TYPE")[1].strip()
    except:
        return success
    if outp.find("crabjob")>=0:
        return checkCrabSuccess(id,bossCfgDir)
    else:
        
        try:
            outfile=executeCommand("bossAdmin SQL -query \"select TASK_EXIT FROM ENDED_cmssw where TASK_ID=%s and CHAIN_ID=%s and ID=%s\""%(taskid,chainid,jobid)+" -c " + bossCfgDir)
            outp=outfile.split("TASK_EXIT")[1].strip()
            # print outp
        except:
        
            return success


        
    success=(outp=="0")
    return success



def checkCrabSuccess(id,bossCfgDir):
    #print "CRAB"
    success=False
    
    taskid=id.split('.')[0]
    chainid=id.split('.')[1]
    jobid=id.split('.')[2]
    
    
    try:
        outfile=executeCommand("bossAdmin SQL -query \"select EXE_EXIT_CODE,JOB_EXIT_STATUS FROM ENDED_crabjob where TASK_ID=%s and CHAIN_ID=%s and ID=%s\""%(taskid,chainid,jobid)+" -c " + bossCfgDir)
        outp=outfile.split('JOB_EXIT_STATUS')[1].strip()
    except:
        
        return success
    
    try:
        exeCode=outp.split()[0].strip()
        jobCode=outp.split()[1].strip()
        
    except:
        return success
    success=(exeCode=="0" and jobCode=="0")
    return success

def resubmit(bossJobId,bossCfgDir):
    """
    BOSSsubmit
    
    BOSS command to submit a task
    """
    tId=bossJobId.split('.')[0]
    bId=bossJobId.split('.')[1]
    bossSubmit = "boss submit "
    bossSubmit += "-taskid %s " % tId
    bossSubmit += "-jobid %s " % bId
    bossSubmit += " -c " + bossCfgDir + " "
    return bossSubmit


def submit(bossJobId,scheduler,bossCfgDir):
    """
    BOSSsubmit
    
    BOSS command to submit a task
    """
    
    bossSubmit = "boss submit "

    ids = bossJobId.split(".")
    taskid=""
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


def getTaskIdFromName(taskName,bossCfgDir):
    """
    getTaskIdFromName

    get TaskId from TaskName
    """
    try:
        outfile=executeCommand("bossAdmin SQL -query \"select MAX(ID) ID from TASK where TASK_NAME='%s'\""%(taskName)+" -c " + bossCfgDir)
        outp=outfile.split('ID')[1].strip()
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
    
#    logging.debug("SubmitterInterface:BOSS xml declare file written:%s" % xmlfile)
    return chain


def getIdFromJobName(bossCfgDir,JobName):
    """
    _getIdFromJobName___
    
    If this job has been declared to BOSS, return the BOSS ID
    from the cache area. If it has not, return None
    
    """

    query = \
           "bossAdmin SQL -query \"select TASK_ID,ID from CHAIN where NAME='" + JobName + "'\" -c " + bossCfgDir
#    logging.debug(query)
    out = executeCommand(query)
#    logging.debug(out)
    outf = out.strip().split("\n")
    try:
        if outf[0].find( "No results!" ) >=0 :
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


def declare(bossCfgDir,parameters):
    """
    BOSS4declare

    BOSS 4 command to declare a task
    """

    bossQuery = "bossAdmin SQL -query \"select id from PROGRAM_TYPES "
    bossQuery += "where id = 'cmssw'\" -c " + bossCfgDir
    queryOut = executeCommand(bossQuery)
    bossJobType = "cmssw"
    if queryOut.find("cmssw") < 0:
        bossJobType=""


    #logging.debug( "bossJobType = %s"%bossJobType)
    ## move one dir up 
    #xmlfile = "%s/%sdeclare.xml" % (
    #    self.parameters['JobCacheArea'], self.parameters['JobName'],
    #    )
    xmlfile = "%s/%sdeclare.xml"% (
        os.path.dirname(parameters['Wrapper']) , parameters['JobName']
        )
    #logging.debug( "xmlfile=%s"%xmlfile)
    bossDeclare = "boss declare -xmlfile %s"%xmlfile + "  -c " + bossCfgDir
    declareClad=open(xmlfile,"w")
    declareClad.write("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n")


###############################################################################
# OLD
#    declareClad.write("<task name=\"%s\">"%parameters['JobName'])
#    declareClad.write("<chain scheduler=\"%s\" ch_tool_name=\"\">"%parameters['Scheduler'])
#    # declareClad.write(" <program exec=\"%s\" args=\"\" stderr=\"%s.stderr\" program_types=\"%s\" stdin=\"\" stdout=\"%s.stdout\"  infiles=\"%s,%s\" outfiles=\"*.root,%s.stdout,%s.stderr,FrameworkJobReport.xml\"  outtopdir=\"\"/></chain></task>"% (os.path.basename(self.parameters['Wrapper']), self.parameters['JobName'],bossJobType, self.parameters['JobName'],self.parameters['Wrapper'],self.parameters['Tarball'], self.parameters['JobName'], self.parameters['JobName']))
#    declareClad.write(" <program> <exec><![CDATA[%s]]></exec><args><![CDATA[""]]></args><stderr><![CDATA[%s.stderr]]></stderr><program_types><![CDATA[%s]]></program_types><stdin><![CDATA[""]]></stdin><stdout><![CDATA[%s.stdout]]></stdout><infiles><![CDATA[%s,%s]]></infiles><outfiles><![CDATA[*.root,%s.stdout,%s.stderr,FrameworkJobReport.xml]]></outfiles><outtopdir><![CDATA[""]]></outtopdir></program></chain></task>"% (os.path.basename(parameters['Wrapper']), parameters['JobName'],bossJobType, parameters['JobName'],parameters['Wrapper'],parameters['Tarball'], parameters['JobName'], parameters['JobName']))
###############################################################################
# NEW
    declareClad.write("<task name=\"%s\">\n" \
                      % parameters['JobSpecInstance'].payload.workflow )
    declareClad.write( chainTemplate(parameters, bossJobType) )
    declareClad.write("</task>\n")
###############################################################################
    
    declareClad.close()
#    logging.debug("SubmitterInterface:BOSS xml declare file written:%s" % xmlfile)

    #  //
    # // Do BOSS Declare
    #//
    bossJobId = executeCommand(bossDeclare)
    #logging.debug( bossJobId)
    try:
        #bossJobId = bossJobId.split("TASK_ID:")[1].split("\n")[0].strip()
        bossJobId = bossJobId.split(":")[1].split("\n")[0].strip()
    except StandardError, ex:
        #logging.debug("SubmitterInterface:BOSS Job ID: %s. BossJobId set to 0\n" % bossJobId)
        raise ProdAgentException("Job Declaration Failed")
    #os.remove(xmlfile)
    if (bossJobId == "") or ( bossJobId == "None" ) :
        raise ProdAgentException("Job Declaration Failed : issuing %s"%bossDeclare)
    return bossJobId


def declareBulk(bossCfgDir, jobList, inpSandbox, workingDir , workflow ):
    """
    BOSS4declareBulk

    BOSS 4 command to declare a task from a list of jobSpec paths
    """

    # xml file name
    xmlfile ="%s/%s-declare.xml"% ( workingDir , workflow )
    declareClad=open( xmlfile,"w" )
    declareClad.write(
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n"
        )
    declareClad.write( "<task name=\"%s\">\n" % workflow )

    # jobType retrieval
    bossQuery = "boss showProgramTypes -c " + bossCfgDir
    queryOut = executeCommand(bossQuery)
    bossJobType = "cmssw"
    if queryOut.find("cmssw") < 0:
        bossJobType=""

    # wrapper filename
    wrapperName = "%s/%s-submit" % (workingDir, workflow)
    parameters = { 'Wrapper' : wrapperName }

    jobSpecList = jobList#.split('\n')
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
    bossDeclare = "boss declare -xmlfile %s"%xmlfile + "  -c " + bossCfgDir
    bossJobId = executeCommand(bossDeclare)
#    print bossJobId
    #logging.debug( bossJobId)
    try:
        #bossJobId = bossJobId.split("TASK_ID:")[1].split("\n")[0].strip()
        bossJobId = bossJobId.split(":")[1].split("\n")[0].strip()
#        print "bossJobId", bossJobId
    except StandardError, ex:
        #logging.debug("SubmitterInterface:BOSS Job ID: %s. BossJobId set to 0\n" % bossJobId)
        raise ProdAgentException("Job Declaration Failed")
    #os.remove(xmlfile)
    return bossJobId


def subdir(id,bossCfgDir):
    """
    _BOSS4subdir_

    This function retrieve job sub dir
    """
    try:
        taskid=id.split('.')[0]
        chainid=id.split('.')[1]
        resub=id.split('.')[2]
    except:
#        logging.error("Boss4subdir Jobid splitting error")
        return ""

##         outfile=self.executeCommand("bossAdmin SQL -query \"select max(ID) ID from JOB where TASK_ID='%s' and CHAIN_ID ='%s'\" "%(taskid,chainid) + " -c " + self.bossCfgDir)
##         outp=outfile
##         try:
##             resub=outp.split("ID")[1].strip()
##         except:
##             resub="0"
##             print outp


    try:
        outfile=executeCommand("bossAdmin SQL -query \"select SUB_PATH from TASK where ID='%s'\""%(taskid) + " -c " + bossCfgDir)
        outp=outfile
    except:
        outp=""

    try:
        outp=outp.split("SUB_PATH")[1].strip()
    except:
        outp=""
    #logging.debug("BOSS4subdir outp '%s'"%outp)    
    return outp



def schedulerInfo( bossCfgDir,id,scheduler,ended=""):
        """
        _BOSS4schedulerInfo_

        Retrieves Scheduler info
        """
        try:
            taskid=id.split('.')[0]
            chainid=id.split('.')[1]
            resub=id.split('.')[2]
            
        except:
#            logging.error("Boss4 Jobid splitting error")
            return ""
        

        schedinfo={}
        try:
            outfile=executeCommand("bossAdmin SQL -query \"select DEST_CE,DEST_QUEUE,SCHED_STATUS,STATUS_REASON from %sSCHED_%s where TASK_ID='%s' and CHAIN_ID='%s' and ID='%s'\""%(ended,scheduler,taskid,chainid,resub) + " -c " + bossCfgDir)
            
            outp=outfile
        except:
            schedinfo=""
           
        try:
            ks=outp.split("\n")[0].strip().split()
            #print ks
            vs=outp.split("\n")[1].strip().split()
            for i in range(len(ks)):
                schedinfo[ks[i]]=vs[i]
            #print vs
#             a=ks.__iter__()
#             b=vs.__iter__()
#             while 1: 
#                 try:
#                     schedinfo[a.next()]=b.next()
#                 except:
#                     break
                #logging.info("%s = %s"%(k,v))
                
        except:
            schedinfo=""
#        logging.debug("BOSS4schedulerInfo outp '%s'"%outp)    
            
        return schedinfo



def executeCommand(command,timeout=600):
    """
    _executeCommand_

    Util it execute the command provided in a popen object with a timeout

    """

    try:
        command="export X509_USER_PROXY=\"%s\";%s"%(os.environ["X509_USER_PROXY"],command)
    except:
        pass

#    f=open("/bohome/bacchi/PRODAGENT_HEAD/PRODAGENT/BOSSCommands.log",'w')
    
#    f.write( command)
    p=Popen4(command)
    p.tochild.close()
    outfd=p.fromchild
    outfno=outfd.fileno()
#    f.write("\npoint 1")
    
#     signal.signal(signal.SIGCHLD,signal.SIG_IGN)
    fl=fcntl.fcntl(outfd,fcntl.F_GETFL,0)
#    f.write("\npoint 2")
    fcntl.fcntl(outfd,fcntl.F_SETFL, fl | os.O_NONBLOCK)
#    f.write("\npoint 2")
    err = -1
    outc = []
    outfeof = 0
    maxt=time.time()+timeout
    #logging.debug("from time %d to time %d"%(time.time(),maxt))
    pid=p.pid
    #logging.debug("process id of %s = %d"%(command,pid))
    timeout=max(1,timeout/10)
#    f.write("timeout=%s"%timeout)
    timedOut=True
    while 1:
        (r,w,e)=select.select([outfno],[],[],timeout)
        if len(r)>0:
            outch=outfd.read()
            if outch=='':
                timedOut=False
                break
            outc.append(outch)
            # f.write("outch=%s"%outch)
        if time.time()>maxt:
            break
        
    # time.sleep(.1)

    if timedOut:
        #logging.error("command %s timed out. timeout %d\n"%(command,timeout))
        # f.write("timedOut")
        os.kill(pid,signal.SIGTERM)
        stoppid(pid,signal.SIGTERM)
        return ""
#    if err > 0:
 #       logging.error("command %s gave %d exit code"%(command,err))
    #    p.wait()
        #ogging.error(p.fromchild.read())

        #eturn ""
        
    try:
        output=string.join(outc,"")
    except:
        output=""
    #logging.debug("command output \n %s"%output)
    #print "command output \n %s"%output
    # f.write("output=%s"%output)
    # f.close()
    return output


# Stijn suggestion for child processes. Thanks.
def stoppid(pid,sig):
    """
    stoppid

    Function to find and kill child processes.
    """
    parent_id=[]
    done=[]
    parent_id.append(str(pid))

    ## collect possible children

    regg=re.compile(r"\s*(\d+)\s+(\d+)\s*$")
    while len(parent_id)>0:
        pi=parent_id.pop()
        done.append(pi)
        ## not on 2.4 kernels
        ## cmd= "ps -o pid --ppid "+pi
        cmd="ps -axo pid,ppid"
        out=Popen4(cmd)
        for line in out.fromchild.readlines():
            line=line.strip('\n')
            if regg.search(line) and (regg.search(line).group(2) == pi):
                pidfound=regg.search(line).group(1)
                parent_id.append(pidfound)
        out.fromchild.close()
    ## kill the pids
    while len(done) >0 :
        nextpid=done.pop()
        try:
            os.kill(int(nextpid),sig)
        except:
            pass
        ## be nice, children signal their parents mostly
        time.sleep(float(1))

        
    
def getoutput(jobId,directory,bossCfgDir):
    """
    BOSS4getoutput

    Boss 4 command to retrieve output
    """
    #logging.debug("Boss4 getoutput start %s "%jobId)
    #print "Boss4 getoutput start %s "%jobId
    try:
        taskid = jobId[0].split('.')[0]
        chainid=jobId[0].split('.')[1]
        resub=jobId[0].split('.')[2]
    except:
        pass
        #print"Boss 4 getoutput - split error %s"%jobId 
##         outfile=self.executeCommand("bossAdmin SQL -query \"select max(ID) ID  from JOB where TASK_ID='%s' and CHAIN_ID ='%s'\" "%(taskid,chainid) + " -c " + self.bossCfgDir)
##         outp=outfile

##         try:
##             resub=outp.split("ID")[1].strip()
##         except:
##             logging.debug(outp)

    getoutpath="%s/BossJob_%s_%s/Submission_%s/" %(directory, taskid,chainid,resub)
    outfile=executeCommand("boss getOutput -outdir "+getoutpath+ " -taskid %s -jobid %s"%(taskid,chainid) + " -c " + bossCfgDir)
    outp=outfile
    return outp


def reportfilename(jobId,directory):
    """
    BOSS4reportfilename

    Boss 4 command to define the correct FrameworkJobReport Location
    """
    try:
        taskid = jobId[0].split('.')[0]
        chainid=jobId[0].split('.')[1]
        resub=jobId[0].split('.')[2]
    except:
        pass
        #logging.debug("Boss 4 reportfilename - split error %s"%jobId)

##         outfile=self.executeCommand("bossAdmin SQL -query \"select max(ID) ID from JOB where TASK_ID='%s' and CHAIN_ID ='%s'\" "%(taskid,chainid) + " -c " + self.bossCfgDir)
##         outp=outfile
##         try:
##             resub=outp.split("ID")[1].strip()
##         except:
##             print outp
    return "%s/BossJob_%s_%s/Submission_%s/FrameworkJobReport.xml" %(directory, taskid,chainid,resub)


def jobSpecId(id,bossCfgDir):
    """
    BOSS4JobSpecId

    BOSS 4 command to retrieve JobSpecID from BOSS db
    """
    try:
        taskid=id.split('.')[0]
    except:
        return ""
    try:
        chainid=id.split('.')[1]
    except:
        #logging.error("Boss4 JobSpecId splitting error")
        chainid="1"
    outfile=executeCommand("bossAdmin SQL -query \"select NAME from CHAIN where TASK_ID=" + taskid + " AND ID=" +  chainid  + "\" -c " + bossCfgDir)

    outp=outfile
    try:
        outp=outp.split("NAME")[1].strip()
    except:
        outp=""
        
    return outp




def schedulerId(id,bossCfgDir):
    """
    BOSS4schedulerId

    Boss 4 command which retrieves the scheduler used to submit job
    """
    try:
        taskid=id.split('.')[0]
        chainid=id.split('.')[1]
        resub=id.split('.')[2]
    except:
        logging.error("Boss4 Jobid splitting error")
        return ""

##         outfile=self.executeCommand("bossAdmin SQL -query \"select max(ID) ID from JOB where TASK_ID='%s' and CHAIN_ID ='%s'\" "%(taskid,chainid) + " -c " + self.bossCfgDir)
##         outp=outfile
##         try:
##             resub=outp.split("ID")[1].strip()
##         except:
##             resub="0"
##             print outp


    try:
        outfile=executeCommand("bossAdmin SQL -query \"select SCHED_ID from JOB where TASK_ID='%s' and CHAIN_ID='%s' and ID='%s'\""%(taskid,chainid,resub) + " -c " + bossCfgDir)
        outp=outfile
    except:
        outp=""

    try:
        outp=outp.split("SCHED_ID")[1].strip()
    except:
        outp=""
    #logging.debug("BOSS4schedulerId outp '%s'"%outp)    
    return outp


def scheduler(id,bossCfgDir,ended=""):
    """
    BOSS4scheduler

    Boss 4 command which retrieves the scheduler used to submit job
    """
    #logging.debug("BOSS4scheduler")
    try:
        taskid=id.split('.')[0]
        chainid=id.split('.')[1]
        resub=id.split('.')[2]

    except:
        #logging.error("Boss4 Jobid splitting error")
        return ""

##         outfile=self.executeCommand("bossAdmin SQL -query \"select max(ID) ID from JOB where TASK_ID='%s' and CHAIN_ID ='%s'\" "%(taskid,chainid) + " -c " + self.bossCfgDir)
##         outp=outfile
##         try:
##             resub=outp.split("ID")[1].strip()
##         except:
##             print outp
##             resub="0"


    try:
        outfile=executeCommand("bossAdmin SQL -query \"select SCHEDULER from %sJOB where TASK_ID='%s' and CHAIN_ID='%s' and ID='%s'\""%(ended,taskid,chainid,resub) + " -c " + bossCfgDir)
        outp=outfile
    except:
        outp=""

    try:
        outp=outp.split("SCHEDULER")[1].strip()
    except:
        outp=""
    #logging.debug("BOSS4scheduler outp '%s'"%outp)    

    return outp


def taskEnded(id,bossCfgDir):
    """
    taskEnded

    This Function tests if all jobs of a Task are ended
    """
    try:
        taskid=id.split('.')[0]
        chainid=id.split('.')[1]
        resub=id.split('.')[2]

    except:
        #logging.error("Boss4 Jobid splitting error")
        return ""

    outfile=executeCommand("bossAdmin SQL -query \"select count(*) Jobs from CHAIN where TASK_ID=%s\" -c %s"%(taskid,bossCfgDir))
    initialJobs=0
    try:
        initialJobs=int(outfile.split("Jobs")[1])
    except:
        return False
    if initialJobs==1:
        return True
    return False


def declareToBOSS(bossCfgDir,parameters):
    """
    _declareToBOSS_
    
    Declare this job to BOSS.
    Parameters are extracted from this instance
    
    """


    # logging.debug("SubmitterInterface:Declaring Job To BOSS")
    bossJobId=declare(bossCfgDir,parameters)

    ## move id file out from job-cache area
    #idFile = "%s/%sid" % (
    #    self.parameters['JobCacheArea'], self.parameters['JobName'],
    #    )
    idFile = "%s/%sid" % (os.path.dirname(parameters['Wrapper']),parameters['JobName'])

    handle = open(idFile, 'w')
    handle.write("JobId=%s" % bossJobId)
    handle.close()
    # logging.debug("SubmitterInterface:BOSS JobID File:%s" % idFile)
    #os.remove(cladfile)
    return


def isBOSSDeclared(Wrapper,JobName):
    """
    _isBOSSDeclared_
    
    If this job has been declared to BOSS, return the BOSS ID
    from the cache area. If it has not, return None
    
    """
    idFile ="%s/%sid" % (os.path.dirname(Wrapper), JobName)

    if not os.path.exists(idFile):
        #  //
        # // No BOSS Id File ==> not declared
        #//
        return None
    content = file(idFile).read().strip()
    content=content.replace("JobId=", "")
    try:
        jobId = content
    except ValueError:
        jobId = None
    return jobId


def FailedSubmission(bossJobId,bossCfgDir):
    
    taskid=bossJobId.split('.')[0]
    try:
        jobMaxRetries=JobState.general(jobSpecId(bossJobId,bossCfgDir))['MaxRetries']
        Retries=JobState.general(jobSpecId(bossJobId,bossCfgDir))['Retries']
    except:
        jobMaxRetries=0
        Retries=0
    
#     outfile=executeCommand("bossAdmin SQL -query \"select max(ID) resub from JOB where TASK_ID=%s\" -c %s"%(taskid,bossCfgDir))
#     resub=0
#     try:
#         resub=int(outfile.split("resub")[1])
#     except:
#         return 
    if Retries>=(jobMaxRetries-1):
        try:
            submissionDir=subdir(taskid+".1.1",bossCfgDir)
            shutil.rmtree(submissionDir)
            Delete(bossJobId,bossCfgDir)
            
        except:
            pass


def archive(jobId,bossCfgDir):
    """
    BOSS4archive

    Boss 4 command to manually archive jobs in the BOSS db
    (i.e. move jobe entries to ENDED_ tables )
    """
    
    outfile=executeCommand("boss archive -taskid %s -jobid %s"%(jobId.split('.')[0],jobId.split('.')[1]) + " -c " + bossCfgDir)
    outp=outfile
    return outp


def Delete(jobId,bossCfgDir):
    """
    BOSS4Delete

    Boss 4 command to manually archive jobs in the BOSS db
    (i.e. move jobe entries to ENDED_ tables ) after setting to killed the job
    """
    
    # print "boss delete -taskid %s -noprompt -c %s"%(jobId.split('.')[0],bossCfgDir)
#    outfile=executeCommand("boss delete -taskid %s -noprompt -c %s"%(jobId.split('.')[0],bossCfgDir))
    outfile=executeCommand("bossAdmin SQL -query \"update JOB set STATUS='K',STOP_T='-1' where TASK_ID='%s'  and CHAIN_ID='%s'\" -c %s"%(jobId.split('.')[0],jobId.split('.')[1],bossCfgDir))
    outfile=executeCommand("boss archive -taskid %s -jobid %s -c %s"%(jobId.split('.')[0],jobId.split('.')[1],bossCfgDir))
    # print outfile
    return


