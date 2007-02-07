#!/usr/bin/env python

import time
from popen2 import Popen4
import select
import fcntl
import string
import os
import signal
def submit(bossJobId,scheduler,bossCfgDir):
    """
    BOSSsubmit
    
    BOSS command to submit a task
    """
    bossSubmit = "boss submit "
    bossSubmit += "-taskid %s " % bossJobId
    bossSubmit += "-scheduler %s " %  scheduler
    bossSubmit += " -c " + bossCfgDir + " "
    return bossSubmit




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

    declareClad.write("<task name=\"%s\">"%parameters['JobName'])
    declareClad.write("<chain scheduler=\"%s\" rtupdater=\"mysql\" ch_tool_name=\"\">"%parameters['Scheduler'])
    # declareClad.write(" <program exec=\"%s\" args=\"\" stderr=\"%s.stderr\" program_types=\"%s\" stdin=\"\" stdout=\"%s.stdout\"  infiles=\"%s,%s\" outfiles=\"*.root,%s.stdout,%s.stderr,FrameworkJobReport.xml\"  outtopdir=\"\"/></chain></task>"% (os.path.basename(self.parameters['Wrapper']), self.parameters['JobName'],bossJobType, self.parameters['JobName'],self.parameters['Wrapper'],self.parameters['Tarball'], self.parameters['JobName'], self.parameters['JobName']))
    declareClad.write(" <program> <exec><![CDATA[%s]]></exec><args><![CDATA[""]]></args><stderr><![CDATA[%s.stderr]]></stderr><program_types><![CDATA[%s]]></program_types><stdin><![CDATA[""]]></stdin><stdout><![CDATA[%s.stdout]]></stdout><infiles><![CDATA[%s,%s]]></infiles><outfiles><![CDATA[*.root,%s.stdout,%s.stderr,FrameworkJobReport.xml]]></outfiles><outtopdir><![CDATA[""]]></outtopdir></program></chain></task>"% (os.path.basename(parameters['Wrapper']), parameters['JobName'],bossJobType, parameters['JobName'],parameters['Wrapper'],parameters['Tarball'], parameters['JobName'], parameters['JobName']))
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

def getoutput(jobId,directory,bossCfgDir):
    """
    BOSS4getoutput

    Boss 4 command to retrieve output
    """
    #logging.debug("Boss4 getoutput start %s "%jobId)
    print "Boss4 getoutput start %s "%jobId
    try:
        taskid = jobId[0].split('.')[0]
        chainid=jobId[0].split('.')[1]
        resub=jobId[0].split('.')[2]
    except:
        #pass
        print"Boss 4 getoutput - split error %s"%jobId 
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
        #logging.error("Boss4 JobSpecId splitting error")
        return ""
    outfile=executeCommand("bossAdmin SQL -query \"select TASK_NAME from TASK where id='%s'\""%taskid + " -c " + bossCfgDir)

    outp=outfile
    try:
        outp=outp.split("TASK_NAME")[1].strip()
    except:
        outp=""

    return outp


def JobSpecId(id,bossCfgDir):
    """
    BOSS4JobSpecId

    BOSS 4 command to retrieve JobSpecID from BOSS db
    """
    try:
        taskid=id.split('.')[0]
    except:
        #logging.error("Boss4 JobSpecId splitting error")
        return ""
    outfile=executeCommand("bossAdmin SQL -query \"select TASK_NAME from TASK where id='%s'\""%taskid + " -c " + bossCfgDir)

    outp=outfile
    try:
        outp=outp.split("TASK_NAME")[1].strip()
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
