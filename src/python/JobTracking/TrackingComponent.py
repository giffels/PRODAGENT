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

__revision__ = "$Id: TrackingComponent.py,v 1.12 2006/05/27 01:25:59 afanfani Exp $"

import socket
import time
import os
import logging
from logging.handlers import RotatingFileHandler
# threads
from threading import Thread, Condition

from ProdAgentCore.Configuration import ProdAgentConfiguration
from MessageService.MessageService import MessageService
from FwkJobRep.ReportState import checkSuccess
from FwkJobRep.FwkJobReport import FwkJobReport


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
        self.BossVersion="v4"
        self.failedJobsPublishedTTL = 180
        #dictionary containing failed jobs: the key is jobid and value is a counter
        self.bossJobScheduler={"v3":self.BOSS3scheduler,"v4":self.BOSS4scheduler}
        self.bossJobSpecId={"v3":self.BOSS3JobSpecId,"v4":self.BOSS4JobSpecId}
        self.bossGetoutput={"v3":self.BOSS3getoutput,"v4":self.BOSS4getoutput}
        self.bossReportFileName={"v3":self.BOSS3reportfilename,"v4":self.BOSS4reportfilename}

        self.failedJobsPublished = {}
        self.cmsErrorJobs = {}
        self.directory=self.args["ComponentDir"]
        self.loadDict(self.failedJobsPublished,"failedJobsPublished")
        self.loadDict(self.cmsErrorJobs,"cmsErrorJobs")
        self.verbose=(self.args["verbose"]==1)

        logging.info("JobTracking Component Started...")
        logging.info("BOSS_ROOT = %s"%os.environ["BOSS_ROOT"])         
        logging.info("BOSS_VERSION = %s\n"%self.BossVersion)
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

        infile,outfile=os.popen4("boss RTupdate -jobid all -c " + self.bossCfgDir)
        infile,outfile=os.popen4("boss q -statusOnly -all -c " + self.bossCfgDir)
        lines=outfile.readlines()
        logging.debug("boss q -statusOnly -all -c " + self.bossCfgDir)
        logging.debug(lines)
# fill job lists
        for j in lines:
            j=j.strip()
            try:
                jid=j.split(' ')[0]
                st=j.split(' ')[1]
            except StandardError, ex:
                logging.debug("splitting error %s"%j)
                jid=''
                st=''
            if st == 'E':
                try:
                    self.failedJobsPublished.pop(jid)
                except StandardError, ex:
                    pass
            elif st == 'OR' or st == 'SD' or st == 'O?':
                try:
                    self.failedJobsPublished.pop(jid)                
                except StandardError, ex:
                    pass
                success.append([jid,st])
            elif st == 'R' or st == 'SR':
                try:
                    self.failedJobsPublished.pop(jid)                
                    self.cmsErrorJobs.pop(jid)
                except StandardError, ex:
                    pass
                running.append([jid,st])
            elif st == 'I':
                try:
                    self.failedJobsPublished.pop(jid)                
                    self.cmsErrorJobs.pop(jid)
                except StandardError, ex:
                    pass
                pending.append([jid,st])
            elif st == 'SW' or st == 'W':
                try:
                    self.failedJobsPublished.pop(jid)                
                    self.cmsErrorJobs.pop(jid)
                except StandardError, ex:
                    pass
                waiting.append([jid,st])
            elif st == 'SS':
                try:
                    self.failedJobsPublished.pop(jid)                
                    self.cmsErrorJobs.pop(jid)
                except StandardError, ex:
                    pass
                scheduled.append([jid,st])
            elif st == 'SA' or  st == 'A?':
                failure.append([jid,st])
            elif st == 'SK' or st=='K':
                failure.append([jid,st])
            elif st == 'SU':
                try:
                    self.failedJobsPublished.pop(jid)                
                    self.cmsErrorJobs.pop(jid)
                except StandardError, ex:
                    pass
                submitted.append([jid,st])
            elif st == 'SE':
                try:
                    self.failedJobsPublished.pop(jid)                
                    self.cmsErrorJobs.pop(jid)
                except StandardError, ex:
                    pass
                cleared.append([jid,st])
            elif st == 'SC':
                try:
                    self.failedJobsPublished.pop(jid)                
                    self.cmsErrorJobs.pop(jid)
                except StandardError, ex:
                    pass
                checkpointed.append([jid,st])
            else:
                try:
                    self.failedJobsPublished.pop(jid)                
                    self.cmsErrorJobs.pop(jid)
                except StandardError, ex:
                    pass
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

            outp=self.bossGetoutput[self.BossVersion](jobId)
            logging.debug("BOSS Getoutput ")
            logging.debug(outp)
            if (outp.find("-force")<0 and outp.find("error")< 0):
                self.reportfilename=self.bossReportFileName[self.BossVersion](jobId)
                logging.debug("%s exists=%s"%(self.reportfilename,os.path.exists(self.reportfilename)))
                if os.path.exists(self.reportfilename):
                    logging.debug("check Job Success %s"%checkSuccess(self.reportfilename))
                    
                    if checkSuccess(self.reportfilename):
                        self.jobSuccess()
                    else:
                        try:
                            self.cmsErrorJobs[jobId[0]]+=0
                        except StandardError:
                            self.cmsErrorJobs[jobId[0]]=0
                            self.jobFailed(jobId,self.reportfilename)

                            logging.error("%s - %d" % (jobId.__str__() , self.cmsErrorJobs[jobId[0]]))
                            self.jobFailed(jobId,self.reportfilename )
                            
                            
                else:
                    try:
                        self.cmsErrorJobs[jobId[0]]+=0
                    except StandardError:
                        self.cmsErrorJobs[jobId[0]]=0
                    
                        logging.error("%s - %d" % (jobId.__str__() , self.cmsErrorJobs[jobId[0]]))
                        self.jobFailed(jobId,"Output retrieved but no FrameworkJobReport!")
                        logging.info( "%s - %d no FrameworkReport" % (jobId.__str__() , self.cmsErrorJobs[jobId[0]]))
                        logging.info( "%s - %d Creating FrameworkReport" % (jobId.__str__() , self.cmsErrorJobs[jobId[0]]))
                        fwjr=FwkJobReport()
                        fwjr.jobSpecId=self.bossJobSpecId[self.BossVersion](jobId[0])
                        self.reportfilename=self.bossReportFileName[self.BossVersion](jobId)
                        fwjr.exitCode=-1
                        fwjr.status="Failed"
                        fwjr.write(self.reportfilename)

            else:
                logging.info(outp)

             
        for jobId in badJobs:
            self.jobFailed(jobId,"Output not retrieved")
            
            #if there aren't failed jobs don't print anything

            try:
                logging.debug("%s - %d" % (jobId.__str__() , self.failedJobsPublished[jobId[0]]))
            except StandardError,ex:
                pass
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

        logging.debug("Unknown Jobs "+ str(len(uJobs)))
        for jobId in uJobs:
            logging.debug(jobId)

        self.saveDict(self.cmsErrorJobs,"cmsErrorJobs")
        time.sleep(float(self.args["PollInterval"]))
        return
    
     
        
    def eventCallback(self, event, handler):
        """
        _eventCallback_

        This method is called whenever an event is sent to this component,
        but since this component publishes events rather than recieves
        them, there is really nothing to do here.
        
        """
        pass
    
    
    def jobSuccess(self):
        """
        _jobSuccess_
        
        Pull the JobReport for the jobId from the DB,
        present it in some way that the rest
        of the prodAgent components can find it and transmit the 
        JobSuccess event to the prodAgent
        
        """
        
        self.ms.publish("JobSuccess", self.reportfilename)
        self.ms.commit()
                                                                                
        logging.debug("JobSuccess:%s" % self.reportfilename)
       
        
        return
    
    def jobFailed(self, jobId, msg):
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
            logging.debug("JobFailed: %s" % msg)
            self.ms.publish("JobFailed", msg)
            self.ms.commit()
                                                                                
            self.failedJobsPublished[jobId[0]] = 1
           
        return
    
    
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

        # create message server
        self.ms = MessageService()
                                                                                
        # register
        self.ms.registerAs("TrackingComponent")
        self.ms.subscribeTo("TrackingComponent:StartDebug")
        self.ms.subscribeTo("TrackingComponent:EndDebug")

        # start polling thread
        pollingThread = Poll(self.checkJobs)
        pollingThread.start()

        # wait for messages
        while True:
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("TrackingComponent: %s, %s" % (type, payload))
            self.__call__(type, payload)


    
        
    def BOSS4getoutput(self,jobId):
        """
        BOSS4getoutput

        Boss 4 command to retrieve output
        """
        logging.info("Boss4 getoutput start %s "%jobId)
        try:
            taskid = jobId[0].split('.')[0]
            chainid=jobId[0].split('.')[1]
        except:
            logging.debug("Boss 4 getoutput - split error %s"%jobId)
        infile,outfile=os.popen4("bossAdmin SQL -query \"select max(ID) ID  from JOB_HEAD where TASK_ID='%s' and CHAIN_ID ='%s'\" "%(taskid,chainid) + " -c " + self.bossCfgDir)
        outp=outfile.read()
        
        try:
            resub=outp.split("ID")[1].strip()
        except:
            logging.debug(outp)

        getoutpath="%s/BossJob_%s_%s/Submission_%s/" %(self.directory, taskid,chainid,resub)
        infile,outfile=os.popen4("boss getOutput -outdir "+getoutpath+ " -taskid %s -jobid %s"%(taskid,chainid) + " -c " + self.bossCfgDir)
        outp=outfile.read()
        return outp
    
    def BOSS3getoutput(self,jobId):
        """
        BOSS3getoutput

        Boss 3 command to retrieve output
        """
    
        infile,outfile=os.popen4("boss getOutput -outdir "+self.directory+ "/BossJob_" + jobId[0] + " -jobid "+jobId[0] + " -c " + self.bossCfgDir)
        outp=outfile.read()
        return outp

    def BOSS3reportfilename(self,jobId):
        """
        BOSS3reportfilename

        Boss 3 command to define the correct FrameworkJobReport Location
        """
        return "%s/BossJob_%s/FrameworkJobReport.xml" %(self.directory,jobId[0])

    def BOSS4reportfilename(self,jobId):
        """
        BOSS4reportfilename

        Boss 4 command to define the correct FrameworkJobReport Location
        """
        
        taskid = jobId[0].split('.')[0]
        chainid=jobId[0].split('.')[1]
        infile,outfile=os.popen4("bossAdmin SQL -query \"select max(ID) ID from JOB_HEAD where TASK_ID='%s' and CHAIN_ID ='%s'\" "%(taskid,chainid) + " -c " + self.bossCfgDir)
        outp=outfile.read()
        try:
            resub=outp.split("ID")[1].strip()
        except:
            print outp
        return "%s/BossJob_%s_%s/Submission_%s/FrameworkJobReport.xml" %(self.directory, taskid,chainid,resub)

    def BOSS3JobSpecId(self,id):
        """
        BOSS3JobSpecId

        BOSS 3 command to retrieve JobSpecID from BOSS db
        """
        infile,outfile=os.popen4("boss SQL -query \"select GROUP_N from JOB where id='%s'\""%id)
        outp=outfile.read()
        try:
            outp=outp.split("GROUP_N")[1].strip()
            
        except:
            outp=""
        
        return outp
        
    def BOSS4JobSpecId(self,id):
        """
        BOSS4JobSpecId

        BOSS 4 command to retrieve JobSpecID from BOSS db
        """
        
        infile,outfile=os.popen4("bossAdmin SQL -query \"select TASK_NAME from TASK_HEAD where id='%s'\""%id.split('.')[0] + " -c " + self.bossCfgDir)
        outp=outfile.read()
        try:
            outp=outp.split("TASK_NAME")[1].strip()
        except:
            outp=""
            
        return outp

    

    def BOSS3scheduler(self,id):
        """
        BOSS3scheduler

        Boss 3 command which retrieves the scheduler used to submit job
        """

        infile,outfile=os.popen4("boss SQL -query \"select SCH from JOB where ID='%s'\""%id)
        outp=outfile.read()
        try:
            outp=outp.split("SCH")[1].strip()
        except:
            outp=""
            
        return outp

        
    def BOSS4scheduler(self,id):
        """
        BOSS4scheduler

        Boss 4 command which retrieves the scheduler used to submit job
        """
        
        try:
            infile,outfile=os.popen4("bossAdmin SQL -query \"select SCHEDULER from JOB where TASK_ID='%s' and ID='%s'\""%(id.split('.')[0],id.split('.')[1]) + " -c " + self.bossCfgDir)
            outp=outfile.read()
        except:
            outp=""
            
        try:
            outp=outp.split("SCHEDULER")[1].strip()
        except:
            outp=""
            
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
