#!/usr/bin/env python
"""
_TrackingComponent_

Skeleton for a standalone server object that implements the ProdAgentClient
as a daemonised thread, while it polls the BOSSDB to watch for
jobs that complete.

When a job is completed this component should create one of two events.


JobSuccess - The job completed successfully, extract the job report for that
job and make it available, the location of the job report should be the payload
for the JobSuccess event.

JobFailure - The job failed in someway and was abandoned. Retrieve debug
information and make it available. The location of the error report should
be the payload of the JobFailure event

"""

__revision__ = "$Id: TrackingComponent.py,v 1.25 2006/09/01 10:58:22 bacchi Exp $"

import socket
import time
import os
from shutil import copy
import string
import logging
from logging.handlers import RotatingFileHandler
from popen2 import Popen4
# threads
from threading import Thread, Condition
from ProdAgentCore.Configuration import ProdAgentConfiguration
from MessageService.MessageService import MessageService
from FwkJobRep.ReportState import checkSuccess
from FwkJobRep.FwkJobReport import FwkJobReport
from FwkJobRep.ReportParser import readJobReport
from JobState.JobStateAPI import JobStateChangeAPI
from JobState.JobStateAPI import JobStateInfoAPI 
import select
import fcntl

class TrackingComponent:
    """
    _TrackingComponent_

    Really rudimentary server that runs a ComponentThread and periodically
    polls the BOSSDB to search for completed jobs that it hasnt found yet.

    This is a really quick and nasty placeholder for proof of principle/
    example, and should probably be something much more involved...
    
    """
    def __init__(self, **args):
        
        self.args = {}
        self.args.setdefault("PollInterval", 10 )
        self.args.setdefault("jobsToPoll", 100)
        self.args.setdefault("ComponentDir","/tmp")
        self.args['Logfile'] = None
        self.args.setdefault("verbose",0)
        self.args.update(args)

# Set up logging for this component
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")
            
        logHandler = RotatingFileHandler(self.args['Logfile'],
                                         "a", 1000000, 3)
        logFormatter = logging.Formatter("%(asctime)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.INFO)
        logging.info("JobTracking Component Initializing...")

# Determine the location of the BOSS configuration files. These is expected
# to be in the configDir of BOSS 
        config = None 
        config = os.environ.get("PRODAGENT_CONFIG", None)
        if config == None:
           msg = "No ProdAgent Config file provided\n"
           msg += "either set $PRODAGENT_CONFIG variable\n"
           msg += "or provide the --config option"

        cfgObject = ProdAgentConfiguration()
        cfgObject.loadFromFile(config)
        prodAgentConfig = cfgObject.get("ProdAgent")
        workingDir = prodAgentConfig['ProdAgentWorkDir'] 
        workingDir = os.path.expandvars(workingDir)
        #self.bossCfgDir = workingDir + "/bosscfg/"
        bossConfig = cfgObject.get("BOSS")
        self.bossCfgDir = bossConfig['configDir'] 
        logging.info("Using BOSS configuration from " + self.bossCfgDir)

# The rest of the initialization
        ##AF: get boss version from "boss v"
##         outf=os.popen4("boss v -c " + self.bossCfgDir)[1].read()
##         version=outf.split("BOSS Version")[1].strip()
##         self.BossVersion="v"+version.split('_')[1] 

        #number of iterations after which failed jobs are purged from DB
#        self.failedJobsPublishedTTL = 180
        #dictionary containing failed jobs: the key is jobid and value is a counter
#        self.bossJobScheduler={"v3":self.BOSS3scheduler,"v4":self.BOSS4scheduler}
        #self.bossJobSpecId={"v3":self.BOSS3JobSpecId,"v4":self.BOSS4JobSpecId}
        #self.bossGetoutput={"v3":self.BOSS3getoutput,"v4":self.BOSS4getoutput}
        #self.bossReportFileName={"v3":self.BOSS3reportfilename,"v4":self.BOSS4reportfilename}
        self.failedJobsPublished = {}
        self.cmsErrorJobs = {}
        self.directory=self.args["ComponentDir"]
        self.loadDict(self.failedJobsPublished,"failedJobsPublished")
        self.loadDict(self.cmsErrorJobs,"cmsErrorJobs")
        self.verbose=(self.args["verbose"]==1)

        logging.info("JobTracking Component Started...")
        logging.info("BOSS_ROOT = %s"%os.environ["BOSS_ROOT"])         
        logging.info("BOSS_VERSION = v4\n")

    def __call__(self, event, payload):
        """
        _operator()_

        Respond to events to control debug level for this component

        """
        if event == "TrackingComponent:StartDebug":
            logging.getLogger().setLevel(logging.DEBUG)
            return
        elif event == "TrackingComponent:EndDebug":
            logging.getLogger().setLevel(logging.INFO)
            return
        return
        
        
    def pollBOSSDB(self):
        """
        _pollBOSSDB_

        Poll the BOSSDB for completed job ids, making sure that
        only newly completed jobs are retrieved

        return two lists, one of successful job Ids, one of failed
        job Ids
        """
#lists containing jobs in a particular status        
        success= []
        failure = []
        running = []
        waiting = []
        pending = []
        scheduled = []
        unknown = []
        aborted = []
        submitted = []
        cancelled = []
        cleared = []
        checkpointed = []

# query boss Database to get jobs status

        jobNumber=300
        timeout=0
        outfile=self.executeCommand("bossAdmin SQL -query \"select DISTINCT j.TASK_ID from JOB j, (select TASK_ID ,CHAIN_ID,max(id) id from JOB group by task_id,chain_id) a  where j.TASK_ID=a.TASK_ID and j.CHAIN_ID=a.CHAIN_ID and j.ID=a.ID and j.getout_t=0\" -c " + self.bossCfgDir)
        try:
            jobNumber=len(outfile.split('\n'))-2
            logging.debug("JobNumber = %s\n"%jobNumber)
        except:
            logging.debug("outfile\n")
            logging.debug(outfile)
            logging.debug("\n")

#        outfile=self.executeCommand("bossAdmin SQL -query \"select MAX(TASK_ID),'-',MIN(TASK_ID)  from  JOB where GETOUT_T=0 and SCHED_ID!='' \" -c " + self.bossCfgDir)
        outfile=self.executeCommand("bossAdmin SQL -query \"select max(j.TASK_ID),'-',min(j.TASK_ID) from JOB j, (select TASK_ID ,CHAIN_ID,max(id) id from JOB group by task_id,chain_id) a  where j.TASK_ID=a.TASK_ID and j.CHAIN_ID=a.CHAIN_ID and j.ID=a.ID and j.getout_t=0\" -c "+ self.bossCfgDir)
#        outfile=self.executeCommand("bossAdmin SQL -query \"select MAX(TASK_ID),'-',MIN(TASK_ID)  from  (select TASK_ID,CHAIN_ID,MAX(ID),sum(GETOUT_T) GETOUT_T from  JOB group by TASK_ID,CHAIN_ID having GETOUT_T=0) a \" -c " + self.bossCfgDir)

        try:
            lines=outfile.split("\n")
            #logging.info(lines[1].split("-")[1])
            startId=int(lines[1].split("-")[1])
            #logging.info("startId = %d\n"%startId)
            
            maxId=int(lines[1].split("-")[0])
            endId=startId+float(self.args["jobsToPoll"])
        except:
            maxId=0
            startId=1
        #logging.info("%d %d %d \n"%(startId,maxId,endId))


        try:
            timeout=jobNumber*5
        except:
            logging.debug("JobNumber = %d;timeout = %d\n"%(jobNumber,jobNumber*.5))
        if timeout==0:
            timeout=600
        logging.debug("number of jobs %d; timeout set to %d\n"%(jobNumber,timeout))

        outfile=self.executeCommand("boss RTupdate -jobid all -c " + self.bossCfgDir)
        
        while startId <= maxId :
            logging.debug("from taskid %d to taskid %d, maxId %d \n"%(startId,endId,maxId))
#            outfile=self.executeCommand("boss q -submitted -statusOnly -taskid %d:%d"%(startId,endId)+"  -c " + self.bossCfgDir,timeout)
            outfile=self.executeCommand("boss q -submitted -taskid %d:%d"%(startId,endId)+"  -c " + self.bossCfgDir,timeout)
            startId=endId+1
            endId=startId+float(self.args["jobsToPoll"])
            logging.debug("dentro while startId = %d\n"%startId)
        
            #lines=outfile.readlines()
            lines=[]
            try:
                lines=outfile.split('\n')
            except:
                pass
            #logging.debug("boss q -statusOnly -all -c " + self.bossCfgDir)
            logging.debug(lines)
            # fill job lists
            for j in lines[1:]:
                j=j.strip()
                try:
                    int(j.split()[0])>0
                    jid=j.split()[0]+"."+j.split()[1]+"."+j.split()[2]
                    st=j.split()[5]
                except StandardError, ex:
                    logging.debug("Incorrect JobId \n %s \n skipping line"%j)
                    jid=''
                    st=''
                if st == 'E':
                    pass
                elif st=='' and jid=='':
                    pass
                elif st == 'OR' or st == 'SD' or st == 'O?':
                    success.append([jid,st])
                elif st == 'R' :
                    running.append([jid,st])
                elif st == 'I':
                    pending.append([jid,st])
                elif st == 'SW' or st == 'W':
                    waiting.append([jid,st])
                elif st == 'SS':
                    scheduled.append([jid,st])
                elif st == 'SA' :
                    failure.append([jid,st])
                elif st == 'SK' or st=='K':
                    failure.append([jid,st])
                elif st == 'SU':
                    submitted.append([jid,st])
                elif st == 'SE':
                    cleared.append([jid,st])
                elif st == 'SC':
                    checkpointed.append([jid,st])
                else:
                    unknown.append([jid,st])
       

        return success, failure, running, pending, waiting, scheduled, submitted, cleared, checkpointed, unknown 


    def checkJobs(self):
        """
        _checkJobs_

        Poll the DB and call the appropriate handler method for each
        jobId that is returned.

        """
# if BOSS db is empty there is an exception but you just don't have anything to do
        try:
            goodJobs, badJobs,rJobs,pJobs,wJobs,sJobs,subJobs,cJobs,chJobs,uJobs = self.pollBOSSDB()
        except StandardError, ex:
            return 0
# here we manage jobs
        
        logging.debug("Success Jobs "+  str(len(goodJobs)))
        
        for jobId in goodJobs:
        
            #if the job is ok for the scheduler retrieve output

            outp=self.BOSS4getoutput(jobId)
            logging.debug("BOSS Getoutput ")
            logging.debug(outp)
            if (outp.find("-force")<0 and outp.find("error")< 0 and outp.find("already been retrieved") < 0 ):
                self.reportfilename=self.BOSS4reportfilename(jobId)
                logging.debug("%s exists=%s"%(self.reportfilename,os.path.exists(self.reportfilename)))
                if os.path.exists(self.reportfilename):
                    #AF: remove the notifyJobState since it's blocking the rest of the code
                    #AFlogging.debug("Notify JobState.finished: %s" % self.reportfilename)
                    #AFself.notifyJobState(self.reportfilename)

                    logging.debug("check Job Success %s"%checkSuccess(self.reportfilename))
                    
                    if checkSuccess(self.reportfilename):
                        
                        self.jobSuccess(jobId)
                        
                    else:
                        try:
                            self.cmsErrorJobs[jobId[0]]+=0
                        except StandardError:
                            self.cmsErrorJobs[jobId[0]]=0
                            self.jobFailed(jobId)

                            logging.error("%s - %d" % (jobId.__str__() , self.cmsErrorJobs[jobId[0]]))
                            self.jobFailed(jobId )
                            
                            
                else:
                    self.resubmit(jobId)

            else:
                if outp.find("Unable to find output sandbox file:")>=0:
                    self.resubmit(jobId)
                                        
                else:
                    logging.error(outp)

        logging.debug("failed jobs"+str(len(badJobs)))
        for jobId in badJobs:
            logging.debug(jobId)
            
            #if there aren't failed jobs don't print anything

            try:
                self.failedJobsPublished[jobId[0]]+=0
            except StandardError,ex:
                self.reportfilename=self.BOSS4reportfilename(jobId)
                logging.info("Creating directory %s"%os.path.dirname(self.reportfilename))
                try:
                    os.makedirs(os.path.dirname(self.reportfilename))
                except:
                    pass
                
                logging.info( "Creating FrameworkReport %s" %self.reportfilename )
                fwjr=FwkJobReport()
                fwjr.jobSpecId=self.BOSS4JobSpecId(jobId[0])
                fwjr.exitCode=-1
                fwjr.status="Failed"
                fwjr.write(self.reportfilename)
                if jobId[1]=="SA":
                    jobScheduler=self.BOSS4scheduler(jobId[0])
                    logging.debug("JobScheduler=%s"%jobScheduler)
                    if jobScheduler=="edg":
                        sched_id=self.BOSS4schedulerId(jobId[0])
                        logging.info("Aborted Job Sched_id=%s"%sched_id)
                        if sched_id!="":
                            self.executeCommand("edg-job-get-logging-info -v 2 %s > %s/edgLoggingInfo.log"%(sched_id,os.path.dirname(self.reportfilename)))
      
                self.jobFailed(jobId)
                
                
        self.saveDict(self.failedJobsPublished,"failedJobsPublished")
            
        logging.debug("Running Jobs "+ str( len(rJobs)))
            
        for jobId in rJobs:
            logging.debug(jobId)
                    
            
        logging.debug("Pending Jobs "+ str( len(pJobs)))
            
        for jobId in pJobs:
            logging.debug(jobId)

            
        logging.debug("Waiting Jobs "+str( len(wJobs)))
        for jobId in wJobs:
            logging.debug(jobId)
            
        logging.debug("Scheduled Jobs "+  str(len(sJobs)))
        for jobId in sJobs:
            logging.debug(jobId)
                
        logging.debug("Submitted Jobs "+  str(len(subJobs)))
        for jobId in subJobs:
            logging.debug(jobId)


        logging.debug("Cleared Jobs "+  str(len(cJobs)))
        for jobId in cJobs:
            logging.debug(jobId)

        logging.debug("Checkpointed Jobs "+ str( len(chJobs)))
        for jobId in chJobs:
            logging.debug(jobId)

        logging.debug("Other Jobs "+ str(len(uJobs)))
        for jobId in uJobs:
            logging.debug(jobId)

        self.saveDict(self.cmsErrorJobs,"cmsErrorJobs")
        time.sleep(float(self.args["PollInterval"]))
        return
    

    def resubmit(self,jobId):
        """
        _resubmit_


        Creates a dummy FrameworkJobReport and send a JobFailed message
        """

        try:
            self.cmsErrorJobs[jobId[0]]+=0
        except StandardError:
            self.cmsErrorJobs[jobId[0]]=0
            
            logging.error("%s - %d" % (jobId.__str__() , self.cmsErrorJobs[jobId[0]]))
            logging.info( "%s - %d no FrameworkReport" % (jobId.__str__() , self.cmsErrorJobs[jobId[0]]))
            logging.info( "%s - %d Creating FrameworkReport" % (jobId.__str__() , self.cmsErrorJobs[jobId[0]]))
            fwjr=FwkJobReport()
            fwjr.jobSpecId=self.BOSS4JobSpecId(jobId[0])
            self.reportfilename=self.BOSS4reportfilename(jobId)
            fwjr.exitCode=-1
            fwjr.status="Failed"
            fwjr.write(self.reportfilename)
            
            self.jobFailed(jobId)
        return
        
    def eventCallback(self, event, handler):
        """
        _eventCallback_

        This method is called whenever an event is sent to this component,
        but since this component publishes events rather than recieves
        them, there is really nothing to do here.
        
        """
        pass
    

    def notifyJobState(self, jobReportFile):
        """
        _notifyJobState_

        Notify the JobState DB of finished jobs

        """
        reports = readJobReports(jobReportFile)
        jobspecs = []
        for report in reports:
            if report.jobSpecId not in jobspecs:
                jobspecs.append(report.jobSpecId)

        for jobspec in jobspecs:
            try:
                JobStateChangeAPI.finished(jobspec)
            except Exception, ex:
                msg = "Error setting job state to finished for job: %s\n" % jobspec
                msg += str(ex)
                logging.error(msg)
        return
        
    
    def jobSuccess(self,jobId):
        """
        _jobSuccess_
        
        Pull the JobReport for the jobId from the DB,
        present it in some way that the rest
        of the prodAgent components can find it and transmit the 
        JobSuccess event to the prodAgent
        
        """

        self.archiveJob("Success",jobId)
        self.msThread.publish("JobSuccess", self.reportfilename)
        self.msThread.commit()
        
        logging.info("published JobSuccess with payload :%s" % self.reportfilename)
       
        
        return
    
    def jobFailed(self, jobId):
        """
        _jobFailed_
        
        Pull the error report for the jobId from the DB, 
        present it in some way that the rest
        of the prodAgent components can find it and transmit the 
        JobFailure event to the prodAgent
        
        """
        
        #if it's the first time we see this failed job we  publish JobFailed event and add the job in failedJobsPublished dict
        
        try:
            self.failedJobsPublished[jobId[0]] += 0
        except StandardError:
            self.archiveJob("Failed",jobId)
            msg=self.reportfilename
            logging.debug("JobFailed: %s" % msg)
            self.msThread.publish("JobFailed", msg)
            self.msThread.commit()
            logging.info("published JobFailed with payload %s"%msg)
            self.failedJobsPublished[jobId[0]] = 1
           
        return
    
    def archiveJob(self,success,jobId):
        """
        _archiveJob_

        Moves output file to archdir
        """
        try:
            taskid=jobId[0].split('.')[0]
            chainid=jobId[0].split('.')[1]
            resub=jobId[0].split('.')[2]
        except:
            logging.error("archiveJob jobId split error\nNo Files deleted")
            return
        
        lastdir=os.path.dirname(self.reportfilename).split('/').pop()
        
        baseDir=os.path.dirname(self.reportfilename)+"/"
        #logging.info("baseDir = %s"%baseDir)
        fjr=readJobReport(self.reportfilename)
        jobCacheDir=JobStateInfoAPI.general(fjr[0].jobSpecId)['CacheDirLocation']
        logging.debug("jobCacheDir = %s"%jobCacheDir)

        newPath=jobCacheDir+"/JobTracking/"+success+"/"+lastdir+"/"
        #logging.info("newPath = %s"%newPath)
        
        try:
            os.makedirs(newPath)
        except:
            pass
        try:
            copy(self.reportfilename,newPath)
            os.unlink(self.reportfilename)
        except:
            logging.error("failed to move %s to %s\n"%(self.reportfilename,newPath))
            pass
        self.reportfilename=newPath+os.path.basename(self.reportfilename)
        files=os.listdir(baseDir)
        
        for f in files:
            (name,ext)=os.path.splitext(f)
            ext=ext.split('.')[1]
            try:
                os.makedirs(newPath+ext)
            except:
                pass
            try:
                copy(baseDir+f,newPath+ext)
                os.unlink(baseDir+f)
            except:
                logging.error("failed to move %s to %s\n"%(baseDir+f,newPath+ext))
                pass
        try:
            os.rmdir(baseDir)
            logging.debug("removing baseDir %s"%baseDir)

        except:
            logging.error("error removing baseDir %s"%baseDir)

            
        try:
            chainDir=baseDir.split('/')
            chainDir.pop()
            chainDir.pop()
            chainDir="/".join(chainDir)
            logging.debug("removing chainDir %s"%chainDir)
            os.rmdir(chainDir)
        except:
            logging.error("error removing chainDir %s"%chainDir)
        if success=="Success":
            try:
                logging.debug("bossAdmin SQL -query \"select SUB_PATH from JOB where TASK_ID='%s' and CHAIN_ID='%s' and ID='%s'\""%(taskid,chainid,resub) + " -c " + self.bossCfgDir)

                outfile=self.executeCommand("bossAdmin SQL -query \"select SUB_PATH from JOB where TASK_ID='%s' and CHAIN_ID='%s' and ID='%s'\""%(taskid,chainid,resub) + " -c " + self.bossCfgDir)
                subPath=outfile
            except:
                subPath=""
                
            try:
                subPath=subPath.split("SUB_PATH")[1].strip()
            except:
                subPath=""
            logging.debug("SubmissionPath '%s'"%subPath)
            try:
                os.remove("%s/BossArchive_%s_g0.tar"%(subPath,taskid))
                logging.debug("removed %s/BossArchive_%s_g0.tar"%(subPath,taskid))
                os.remove("%s/BossClassAdFile_%s"%(subPath,taskid))
                logging.debug("removed %s/BossClassAdFile_%s"%(subPath,taskid))
            except:
                logging.error("Failed to remove submission files")
        return

        
    def subFailed(self, jobId, msg):
        """
        _subFailed_
        
        Publishes a SubmissionFailed event
        
        """
        
        #if it's the first time we see this failed job we  publish JobFailed event and add the job in failedJobsPublished dict
       # logging.debug("SubmissionFailed: %s" % msg)

        try:
            self.failedJobsPublished[jobId[0]] += 0
        except StandardError:
            JobStateChangeAPI.submitFailure(msg)
            
            logging.debug("SubmissionFailed: %s" % msg)
            self.msThread.publish("SubmissionFailed", msg)
            self.msThread.commit()
            logging.info("published SubmissionFailed with payload %s"%msg)
                                                                                
            self.failedJobsPublished[jobId[0]] = 1
           
        return

    def executeCommand(self,command,timeout=600):
        """
        _executeCommand_
        
        Util it execute the command provided in a popen object with a timeout
        
        """

       
        p=Popen4(command)
        p.tochild.close()
	outfd=p.fromchild
	outfno=outfd.fileno()
	fl=fcntl.fcntl(outfno,fcntl.F_GETFL)
 	try:
	    fcntl.fcntl(outfno,fcntl.F_SETFL, fl | os.O_NDELAY)
        except AttributeError:
            fcntl.fcntl(outfno,fcntl.F_SETFL, fl | os.FNDELAY)
	err = -1
        outc = []
        outfeof = 0
        maxt=time.time()+timeout
        logging.debug("from time %d to time %d"%(time.time(),maxt))
        pid=p.pid
        logging.debug("process id of %s = %d"%(command,pid))
        while time.time() < maxt :
            ready=select.select([outfno],[],[])
            if outfno in ready[0]:
                outch=outfd.read()
                if outch=='':
                    outfeof=1
                outc.append(outch)
            if outfeof:
                err=p.wait()
                break
            time.sleep(.1)
    
        if err == -1:
            logging.error("command %s timed out. timeout %d\n"%(command,timeout))
            return ""
        if err > 0:
            logging.error("command %s gave %d exit code"%(command,err))
        #    p.wait()
            #ogging.error(p.fromchild.read())
            
            #eturn ""
        
        output=string.join(outc,"")
        logging.debug("command output \n %s"%output)
        return output


    def saveDict(self,d,filename):
        try:
            f=open("%s/%s"%(self.directory,filename),'w')
        except StandardError,ex:
     #       logging.debug("Errore ad aprire il file")
            return
        for i in d:
            t="%s %s\n" % (i,d[i])
    #        logging.debug(t)
            f.write(t)
        f.close()
        return
    
    def loadDict(self,d,filename):
        try:
            f=open("%s/%s"%(self.directory,filename),'r')
        except StandardError, ex:
            return
        lines=f.readlines()
        for l in lines:
            k,v=(l.strip()).split(' ')
            d[k]=int(v)
        f.close()
        return
        
    
    

    def startComponent(self):
        """
        _startComponent_

        Start up this component, start the ComponentThread and then
        commence polling the DB

        """

        # create message server instances
        self.ms = MessageService()
        self.msThread = MessageService()
                                                                                
        # register
        self.ms.registerAs("TrackingComponent")
        self.msThread.registerAs("TrackingComponentThread")

        # subscribe to messages
        self.ms.subscribeTo("TrackingComponent:StartDebug")
        self.ms.subscribeTo("TrackingComponent:EndDebug")

        # start polling thread
        #logging.info("before Poll constructor")

        pollingThread = Poll(self.checkJobs)
        #logging.info("after Poll constructor")
        pollingThread.start()
        #logging.info("after Poll start")

        # wait for messages
        while True:
            
            #logging.info("Thread Alive %s"%pollingThread.isAlive())
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("TrackingComponent: %s, %s" % (type, payload))
            self.__call__(type, payload)


    
        
    def BOSS4getoutput(self,jobId):
        """
        BOSS4getoutput

        Boss 4 command to retrieve output
        """
        logging.debug("Boss4 getoutput start %s "%jobId)
        try:
            taskid = jobId[0].split('.')[0]
            chainid=jobId[0].split('.')[1]
            resub=jobId[0].split('.')[2]
        except:
            logging.error("Boss 4 getoutput - split error %s"%jobId)
##         outfile=self.executeCommand("bossAdmin SQL -query \"select max(ID) ID  from JOB where TASK_ID='%s' and CHAIN_ID ='%s'\" "%(taskid,chainid) + " -c " + self.bossCfgDir)
##         outp=outfile
        
##         try:
##             resub=outp.split("ID")[1].strip()
##         except:
##             logging.debug(outp)
        
        getoutpath="%s/BossJob_%s_%s/Submission_%s/" %(self.directory, taskid,chainid,resub)
        outfile=self.executeCommand("boss getOutput -outdir "+getoutpath+ " -taskid %s -jobid %s"%(taskid,chainid) + " -c " + self.bossCfgDir)
        outp=outfile
        return outp
    

    def BOSS4reportfilename(self,jobId):
        """
        BOSS4reportfilename

        Boss 4 command to define the correct FrameworkJobReport Location
        """
        try:
            taskid = jobId[0].split('.')[0]
            chainid=jobId[0].split('.')[1]
            resub=jobId[0].split('.')[2]
        except:
            logging.debug("Boss 4 reportfilename - split error %s"%jobId)

##         outfile=self.executeCommand("bossAdmin SQL -query \"select max(ID) ID from JOB where TASK_ID='%s' and CHAIN_ID ='%s'\" "%(taskid,chainid) + " -c " + self.bossCfgDir)
##         outp=outfile
##         try:
##             resub=outp.split("ID")[1].strip()
##         except:
##             print outp
        return "%s/BossJob_%s_%s/Submission_%s/FrameworkJobReport.xml" %(self.directory, taskid,chainid,resub)

        
    def BOSS4JobSpecId(self,id):
        """
        BOSS4JobSpecId

        BOSS 4 command to retrieve JobSpecID from BOSS db
        """
        try:
            taskid=id.split('.')[0]
        except:
            logging.error("Boss4 JobSpecId splitting error")
            return ""
        outfile=self.executeCommand("bossAdmin SQL -query \"select TASK_NAME from TASK_HEAD where id='%s'\""%taskid + " -c " + self.bossCfgDir)
            
        outp=outfile
        try:
            outp=outp.split("TASK_NAME")[1].strip()
        except:
            outp=""
            
        return outp

    

    def BOSS4scheduler(self,id):
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
            logging.error("Boss4 Jobid splitting error")
            return ""
        
##         outfile=self.executeCommand("bossAdmin SQL -query \"select max(ID) ID from JOB where TASK_ID='%s' and CHAIN_ID ='%s'\" "%(taskid,chainid) + " -c " + self.bossCfgDir)
##         outp=outfile
##         try:
##             resub=outp.split("ID")[1].strip()
##         except:
##             print outp
##             resub="0"


        try:
            outfile=self.executeCommand("bossAdmin SQL -query \"select SCHEDULER from JOB where TASK_ID='%s' and CHAIN_ID='%s' and ID='%s'\""%(taskid,chainid,resub) + " -c " + self.bossCfgDir)
            outp=outfile
        except:
            outp=""
            
        try:
            outp=outp.split("SCHEDULER")[1].strip()
        except:
            outp=""
        logging.debug("BOSS4scheduler outp '%s'"%outp)    
            
        return outp

    def BOSS4schedulerId(self,id):
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
            outfile=self.executeCommand("bossAdmin SQL -query \"select SCHED_ID from JOB where TASK_ID='%s' and CHAIN_ID='%s' and ID='%s'\""%(taskid,chainid,resub) + " -c " + self.bossCfgDir)
            outp=outfile
        except:
            outp=""
            
        try:
            outp=outp.split("SCHED_ID")[1].strip()
        except:
            outp=""
        logging.debug("BOSS4schedulerId outp '%s'"%outp)    
        return outp

        



class Poll(Thread):
    """
    Thread that performs polling
    """
                                                                                                                            
    def __init__(self, poll):
        """
        __init__
                                                                                                                            
        Initialize thread and set polling callback
        """
        Thread.__init__(self)
        self.poll = poll;
                                                                                                                            
    def run(self):
        """
        __run__
                                                                                                                            
        Performs polling 
        """
                                                                                                                            
        while True:
            self.poll()
