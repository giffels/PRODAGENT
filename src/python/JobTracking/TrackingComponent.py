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

import socket
import time
import os
import logging
from logging.handlers import RotatingFileHandler


from MessageService.MessageService import MessageService
from FwkJobRep.ReportState import checkSuccess



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

        

        
        os.environ["BOSSDIR"]=self.args["BOSSDIR"]
        os.environ["BOSSVERSION"]=self.args["BOSSVERSION"]
        if self.args["BOSSPATH"]!="":
            try:
                os.environ["PATH"]+=":"+self.args["BOSSPATH"]
            except StandardError, ex:
                os.environ["PATH"]=self.args["BOSSPATH"]
            
        #number of iterations after which failed jobs are purged from DB
        self.failedJobsPublishedTTL = 180
        #dictionary containing failed jobs: the key is jobid and value is a counter
        self.failedJobsPublished = {}
        self.cmsErrorJobs = {}
        self.directory=self.args["ComponentDir"]
        self.loadDict(self.failedJobsPublished,"failedJobsPublished")
        self.loadDict(self.cmsErrorJobs,"cmsErrorJobs")
        self.verbose=(self.args["verbose"]==1)

        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")
            
        logHandler = RotatingFileHandler(self.args['Logfile'],
                                         "a", 1000000, 3)
        logFormatter = logging.Formatter("%(asctime)s:%(message)s")
        logHandler.setFormatter(logFormatter)
        logging.getLogger().addHandler(logHandler)
        logging.getLogger().setLevel(logging.INFO)
        logging.info("JobTracking Component Started...")
        

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

        infile,outfile=os.popen4("boss RTupdate -jobid all")
        infile,outfile=os.popen4("boss q -statusOnly -all")
        lines=outfile.readlines()

# fill job lists
        for j in lines:
            j=j.strip()
            try:
                jid=j.split(' ')[0]
                st=j.split(' ')[1]
            except StandardError, ex:
                #               print "splitting error"
                return ""
            if st == 'E':
                try:
                    self.failedJobsPulished.pop(jid)
                    self.cmsErrorJobs.pop(jid)
                except StandardError, ex:
                    pass
            elif st == 'OR' or st == 'SD' or st == 'O?':
                try:
                    self.failedJobsPulished.pop(jid)                
                    self.cmsErrorJobs.pop(jid)
                except StandardError, ex:
                    pass
                success.append([jid,st])
            elif st == 'R' or st == 'SR':
                try:
                    self.failedJobsPulished.pop(jid)                
                    self.cmsErrorJobs.pop(jid)
                except StandardError, ex:
                    pass
                running.append([jid,st])
            elif st == 'I':
                try:
                    self.failedJobsPulished.pop(jid)                
                    self.cmsErrorJobs.pop(jid)
                except StandardError, ex:
                    pass
                pending.append([jid,st])
            elif st == 'SW' or st == 'W':
                try:
                    self.failedJobsPulished.pop(jid)                
                    self.cmsErrorJobs.pop(jid)
                except StandardError, ex:
                    pass
                waiting.append([jid,st])
            elif st == 'SS':
                try:
                    self.failedJobsPulished.pop(jid)                
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
                    self.failedJobsPulished.pop(jid)                
                    self.cmsErrorJobs.pop(jid)
                except StandardError, ex:
                    pass
                submitted.append([jid,st])
            elif st == 'SE':
                try:
                    self.failedJobsPulished.pop(jid)                
                    self.cmsErrorJobs.pop(jid)
                except StandardError, ex:
                    pass
                cleared.append([jid,st])
            elif st == 'SC':
                try:
                    self.failedJobsPulished.pop(jid)                
                    self.cmsErrorJobs.pop(jid)
                except StandardError, ex:
                    pass
                checkpointed.append([jid,st])
            else:
                try:
                    self.failedJobsPulished.pop(jid)                
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
#            print "pollBossDB Error"
            return 0
# here we manage jobs
        
        logging.debug("Success Jobs "+  str(len(goodJobs)))
        
        for jobId in goodJobs:
        
            #if the job is ok for the scheduler retrieve output

            infile,outfile=os.popen4("boss getOutput -outdir "+self.directory+ "/BossJob_" + jobId[0] + " -jobid "+jobId[0])
            outp=outfile.read()
            if (outp.find("-force")<0 and outp.find("error")< 0):
                reportfilename="%s/BossJob_%s/FrameworkJobReport.xml" %(self.directory, jobId[0])
                if os.path.exists(reportfilename):
                    if checkSuccess(reportfilename):
                        self.jobSuccess(jobId[0])
                    else:
                        try:
                            self.cmsErrorJobs[jobId[0]]+=0
                        except StandardError:
                            self.cmsErrorJobs[jobId[0]]=0
                            logging.error("%s - %d" % (jobId.__str__() , self.cmsErrorJobs[jobId[0]]))
                            self.jobFailed(jobId,"file://"+self.directory+"/BossJob_%s/FrameworkJobReport.xml" % jobId[0] )
                            self.saveDict(self.cmsErrorJobs,"cmsErrorJobs")
                            
                            
                else:
                    try:
                        self.cmsErrorJobs[jobId[0]]+=0
                    except StandardError:
                        self.cmsErrorJobs[jobId[0]]=0
                      #  print "JobSuccess but no FrameworkJobReport!\n"
                    
                        logging.error("%s - %d" % (jobId.__str__() , self.cmsErrorJobs[jobId[0]]))
                        self.jobFailed(jobId,"Output retrieved but no FrameworkJobReport!")
                        self.saveDict(self.cmsErrorJobs,"cmsErrorJobs")
#after publishing JobSuccess event the job is purged from BOSS db. We can decide to wait some time as for failed jobs.

#            infile,outfile=os.popen4("boss p -before "+ time.strftime("%Y-%m-%d",time.localtime(time.time()+87000)) +" -jobid "+jobId[0]+" -noprompt")

        

        logging.info("failed Jobs "+ str( len(badJobs)))
             
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
        #a=self.cmsErrorJobs.copy()
        #for  i in self.cmsErrorJobs.keys():
            #self.cmsErrorJobs[i]+=1
            #if self.cmsErrorJobs[i] > self.failedJobsPublishedTTL:
              #  print "job %s deleted from db"%i
                #infile,outfile=os.popen4("boss d -jobid %s -noprompt"%i)
                #del self.cmsErrorJobs[i]

        self.saveDict(self.cmsErrorJobs,"cmsErrorJobs")

        time.sleep(self.args["PollInterval"])
        return
    
     
        
    def eventCallback(self, event, handler):
        """
        _eventCallback_

        This method is called whenever an event is sent to this component,
        but since this component publishes events rather than recieves
        them, there is really nothing to do here.
        
        """
        pass
    
    
    def jobSuccess(self, jobId):
        """
        _jobSuccess_
        
        Pull the JobReport for the jobId from the DB,
        present it in some way that the rest
        of the prodAgent components can find it and transmit the 
        JobSuccess event to the prodAgent
        
        """
        
        jobReportLocation = "file://"+self.directory+"/BossJob_%s/FrameworkJobReport.xml" % jobId
        self.ms.publish("JobSuccess", jobReportLocation)
        self.ms.commit()
                                                                                
        logging.debug("JobSuccess:%s" % jobReportLocation)
       
        
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
            #errReportLocation = "file://"+self.directory+"/BossJob_%s/FrameworkJobReport.xml" % jobId[0] 
            logging.debug("JobFailed: %s" % msg)
            self.ms.publish("JobFailed", msg)
            self.ms.commit()
                                                                                
            self.failedJobsPublished[jobId[0]] = 1
            #print "messaggio pubblicato %s\n"%msg;
            #Count down for job purge
           
        #if self.failedJobsPublished[jobId[0]] > self.failedJobsPublishedTTL:
            #purge
#            infile,outfile=os.popen4("boss p -before "+ time.strftime("%Y-%m-%d",time.localtime(time.time()+87000)) +" -jobid "+jobId[0]+" -noprompt")
         #   infile,outfile=os.popen4("boss d -jobid "+jobId[0]+" -noprompt")

          #  self.failedJobsPublished.pop(jobId[0])
        return
    
    
    def saveDict(self,d,filename):
        
        f=open("%s/%s"%(self.directory,filename),'w')
        for i in d:
            t="%s %s\n" % (i,d[i])
            f.write(t)
        f.close()
            
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

        
        while True:
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("TrackingComponent: %s, %s" % (type, payload))
            self.__call__(type, payload)
            self.checkJobs()
            
    
        
