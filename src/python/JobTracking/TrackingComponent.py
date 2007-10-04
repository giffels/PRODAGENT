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

__revision__ = "$Id: TrackingComponent.py,v 1.49 2007/10/04 14:21:41 afanfani Exp $"

import traceback
import time
import os
from shutil import copy
from shutil import rmtree
import logging
# PA configuration
from ProdAgentCore.Configuration import ProdAgentConfiguration
from ProdAgentCore.Configuration import loadProdAgentConfiguration
from MessageService.MessageService import MessageService
# fjr handling
from FwkJobRep.ReportState import checkSuccess
from FwkJobRep.FwkJobReport import FwkJobReport
from FwkJobRep.ReportParser import readJobReport

from ProdCommon.Database import Session
from ProdAgent.WorkflowEntities import JobState
from ProdAgent.WorkflowEntities import Job as WEJob
from ProdAgentDB.Config import defaultConfig as dbConfig

from ShREEK.CMSPlugins.DashboardInfo import DashboardInfo
from ProdAgentBOSS import BOSSCommands
import  ProdAgentCore.LoggingUtils as LoggingUtils
from ProdAgentCore.ProdAgentException import ProdAgentException



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
#        self.args.setdefault("proxyCacheDir","NULL")
        self.args['Logfile'] = None
        self.args.setdefault("verbose",0)
        self.args.update(args)

# Set up logging for this component
        if self.args['Logfile'] == None:
            self.args['Logfile'] = os.path.join(self.args['ComponentDir'],
                                                "ComponentLog")

# use the LoggingUtils
        LoggingUtils.installLogHandler(self)
        logging.info("JobTracking Component Initializing...")


        # compute poll delay
        delay = int(self.args['PollInterval'])
        if delay < 10:
            delay = 10 # a minimum value

        seconds = str(delay % 60)
        minutes = str((delay / 60) % 60)
        hours = str(delay / 3600)

        self.pollDelay = hours.zfill(2) + ':' + \
                         minutes.zfill(2) + ':' + \
                         seconds.zfill(2)
        
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

        self.directory=self.args["ComponentDir"]
        self.verbose=(self.args["verbose"]==1)

        self.submittedJobs = {}
        self.loadDict(self.submittedJobs,"submittedJobs")

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
        elif event == "TrackingComponent:pollDB":
            self.checkJobs()
            return
        return


    # it will be substituted by a threaded system
    def pollLB(self):
        """
        _pollLB_

        Poll the LB through BOSS to update the job status
        get completed job ids, making sure that
        only newly completed jobs are retrieved

        return two lists, one for successful jobs, one of failed jobs
        """

        logging.info("****************pollLB")
        jobNumber=300
        timeout=0
        goodJobs = []
        badJobs = []

        command  = \
                "bossAdmin SQL -query \"select DISTINCT TASK_INFO from TASK \" -c " + self.bossCfgDir
        outfile=BOSSCommands.executeCommand( command )
        if  outfile.find("Unknown column") != -1 or  outfile.find("No results") != -1:
            certs = ['']
        else :
            certs=outfile.split()[1:]
            logging.info("certs=%s"%certs)
        
        for cert in certs:
            if cert=='NULL':
                cert='';

            command  = \
                    'bossAdmin SQL -query "select j.TASK_ID from JOB j,TASK t ' + \
                    "where TASK_INFO='" + cert + "' and t.ID=j.TASK_ID " +\
                    'order by TASK_ID" -c ' + self.bossCfgDir
            outfile = BOSSCommands.executeCommand( command )
            #logging.debug(outfile)
            
            try:
                jobIds=outfile.split()[1:]
                jobNumber=len(jobIds)
                taskIds=list(set(jobIds))
                # logging.info("TaskIds = %s\n"%taskIds)
            except:
                pass

            try:
                timeout=jobNumber*5
            except:
                timeout=0
                # logging.info("JobNumber = %d;timeout = %d\n"%(jobNumber,jobNumber*.5))
            if timeout==0:
                timeout=600
            logging.info("number of jobs %d; timeout set to %d\n"%(jobNumber,timeout))

            if cert == '' :
                logging.debug("using environment X509_USER_PROXY")
            elif os.path.exists(cert):
                os.environ["X509_USER_PROXY"]=cert
                logging.info("using %s"%os.environ["X509_USER_PROXY"])
            else:
                logging.info(
                    "cert path " + cert + \
                    " does not exists: trying to use the default one if there"
                    )

            outfile=BOSSCommands.executeCommand("boss RTupdate -jobid all -c " + self.bossCfgDir)
            # logging.info("len taskids %s >0 = %s"%(len(taskIds),len(taskIds)>0))
            while len(taskIds)>0 :
                
                # logging.debug("from taskid %d to taskid %d, maxId %d \n"%(startId,endId,maxId))
                ids="%s"%taskIds.pop()
                c=1
                while c<=int(self.args["jobsToPoll"]) and len(taskIds)>0:
                    ids+=",%s"%taskIds.pop()
                    c+=1
                logging.debug("ids = %s"%ids)
                command = \
                        "boss q -statusOnly -submitted -taskid " + ids \
                        + " -c " + self.bossCfgDir
                outfile = BOSSCommands.executeCommand( command, timeout )
                logging.debug( ' BOSS OUTPUT: \n' + outfile)
                
                lines=[]
                try:
                    lines=outfile.split('\n')
                    goods,bads = self.getStates( lines )
                    goodJobs.extend( goods )
                    badJobs.extend( bads )
                except:
                    pass
        return goodJobs,badJobs



    def pollBOSSDB(self):
        """
        _pollBOSSDB_

        Poll the BOSSDB for completed job ids, making sure that
        only newly completed jobs are retrieved

        return two lists, one of successful job Ids, one of failed
        job Ids
        """
        
        logging.info("****************pollBOSSDB")
        goodJobs = []
        badJobs = []
        
#        command = \
#                "boss q -statusOnly -submitted -avoidCheck -c " \
#                + self.bossCfgDir

        command = \
                'bossAdmin SQL -query "select TASK_ID,CHAIN_ID,ID,STATUS' + \
                ' from JOB order by TASK_ID,CHAIN_ID" -c ' + self.bossCfgDir
        
        outfile=BOSSCommands.executeCommand( command )
        logging.debug( ' BOSS OUTPUT: \n' + outfile)
        
        lines=[]
        
        lines=outfile.split('\n')
        goodJobs, badJobs = self.getStates( lines )
        return goodJobs,badJobs



    def getStates( self, lines ):

        pending = []   
        submitted = []   
        waiting = []
        ready = []
        scheduled = []
        running = []
        success= []
        failure = []
        cleared = []
        unknown = []

        newSubmittedJobs={}

        for j in lines[1:] :
            j=j.strip()
            try:
                int(j.split()[0])>0
                jid=j.split()[0]+"."+j.split()[1]+"."+j.split()[2]
                st=j.split()[3]
            except StandardError, ex:
#                logging.debug("Incorrect JobId \n %s \n skipping line"%j)
                jid=''
                st=''
            if jid !='':
                try:
                    newSubmittedJobs[jid]=self.submittedJobs[jid]
                except StandardError,ex:
                    newSubmittedJobs[jid]=0
                    self.dashboardPublish(jid)

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
            elif st == 'SW' :
                waiting.append([jid,st])
            elif st == 'SS':
                scheduled.append([jid,st])
            elif st == 'SA' or st == 'A' :
                failure.append([jid,st])
            elif st == 'SK' or st=='K':
                failure.append([jid,st])
            elif st == 'SU':
                submitted.append([jid,st])
            elif st == 'SE':
                cleared.append([jid,st])
            else:
                unknown.append([jid,st])

        logging.info( "Pending   Jobs  : " + str( len(pending)   ) )
        logging.info( "Submitted Jobs  : " + str( len(submitted) ) )
        logging.info( "Waiting   Jobs  : " + str( len(waiting)   ) )
        logging.info( "Ready     Jobs  : " + str( len(ready)     ) )
        logging.info( "Scheduled Jobs  : " + str( len(scheduled) ) )
        logging.info( "Running   Jobs  : " + str( len(running)   ) )
        logging.info( "Success   Jobs  : " + str( len(success)   ) )
        logging.info( "Failed    Jobs  : " + str( len(failure)   ) )
        logging.info( "Cleared   Jobs  : " + str( len(cleared)   ) )
        logging.info( "Others    Jobs  : " + str( len(unknown)   ) )

        self.submittedJobs = newSubmittedJobs
        self.saveDict(self.submittedJobs,"submittedJobs")

        return success, failure


    def checkJobs(self):
        """
        _checkJobs_

        Poll the DB and call the appropriate handler method for each
        jobId that is returned.

        """
        # if BOSS db is empty there is an exception but you just don't have anything to do

        logging.info("****************checkJobs")
        try:
            goodJobs, badJobs = self.pollLB()
#            goodJobs, badJobs = self.pollBOSSDB()
            self.handleFailed( badJobs )
            self.handleFinished( goodJobs )

        except StandardError, ex:
            logging.info( ex.__str__() )
            logging.info( traceback.format_exc() )
            return 0

        # sleep until next polling cycle
        self.ms.publish("TrackingComponent:pollDB", "")
        time.sleep(float(self.args["PollInterval"]))


    def handleFailed( self, badJobs ):
        """
        _handleFailed_

        handle failed jobs

        """

        for jobId in badJobs:
            try:
                jobSpecId=BOSSCommands.jobSpecId(jobId[0],self.bossCfgDir)
            except:
                logging.debug( "************ Bad jobid : " + jobId.__str__())
                continue

            self.reportfilename=BOSSCommands.reportfilename(jobId,self.directory)
            self.dashboardPublish(jobId[0])
            self.dashboardPublish(jobId[0],"ENDED_")

            logging.info("Creating directory %s"%os.path.dirname(self.reportfilename))
            try:
                os.makedirs(os.path.dirname(self.reportfilename))
            except:
                pass
                
            logging.info( "Creating FrameworkReport %s" %self.reportfilename )
            fwjr=FwkJobReport()
            #fwjr.jobSpecId=self.BOSS4JobSpecId(jobId[0])
            logging.info("JobSpecId=%s"%jobSpecId)
            fwjr.jobSpecId=jobSpecId
            fwjr.exitCode=-1
            fwjr.status="Failed"
            fwjr.write(self.reportfilename)
#            if jobId[1]=="SA":
            jobScheduler=BOSSCommands.scheduler(jobId[0],self.bossCfgDir)
            logging.debug("JobScheduler=%s"%jobScheduler)
            if jobScheduler == "edg" :
                sched_id=BOSSCommands.schedulerId(jobId[0],self.bossCfgDir)
                logging.info("Aborted Job Sched_id=%s"%sched_id)
                if sched_id!="":
                    BOSSCommands.executeCommand("edg-job-get-logging-info -v 2 %s > %s/edgLoggingInfo.log"%(sched_id,os.path.dirname(self.reportfilename)))
            elif jobScheduler.find("glite") != -1 :
                sched_id=BOSSCommands.schedulerId(jobId[0],self.bossCfgDir)
                logging.info("Aborted Job Sched_id=%s"%sched_id)
                if sched_id!="":
                    BOSSCommands.executeCommand("glite-wms-job-logging-info -v 2 %s > %s/gliteLoggingInfo.log"%(sched_id,os.path.dirname(self.reportfilename)))
            BOSSCommands.archive(jobId[0], self.bossCfgDir)
            self.jobFailed(jobId)


    def handleFinished( self, goodJobs ):
        """
        _handleFailed_

        handle finished jobs: retrieve output
        and notify execution failure/success

        """

        for jobId in goodJobs:

            try:
                jobSpecId=BOSSCommands.jobSpecId(jobId[0],self.bossCfgDir)
            except:
                logging.debug( "************ Bad jobid : " + jobId.__str__())
                continue

            #if the job is ok for the scheduler retrieve output
            command = \
                    'bossAdmin SQL -query "select TASK_INFO from TASK t' \
                    + ' where ID=' + jobId[0].split('.')[0] \
                    + '" -c ' + self.bossCfgDir
            
            outfile = BOSSCommands.executeCommand( command )
            if  outfile.find("Unknown column") == -1 and outfile.find("No results") == -1:
            
                cert=outfile.split()[1]
                if cert=='NULL':
                    pass
                elif os.path.exists(cert):
                    os.environ["X509_USER_PROXY"]=cert
                    logging.info("using %s"%os.environ["X509_USER_PROXY"])
                else:
                    logging.info(
                        "cert path " + cert + \
                        " does not exists: trying to use the default one if there"
                        )
                
            jobSpecId=BOSSCommands.jobSpecId(jobId[0],self.bossCfgDir)
            outp=BOSSCommands.getoutput(jobId,self.directory,self.bossCfgDir)
            logging.info("BOSS Getoutput ")
            self.dashboardPublish(jobId[0],"ENDED_")
            logging.info(outp)
            # if successful output retrieval
            if  outp.find("-force") < 0 and \
                outp.find("error")< 0 and \
                outp.find("already been retrieved") < 0 :
                self.reportfilename = BOSSCommands.reportfilename(
                    jobId,self.directory
                    )
                logging.debug("%s exists=%s"%(self.reportfilename,os.path.exists(self.reportfilename)))
                success=False
                # is the FwkJobReport there?
                if os.path.exists(self.reportfilename):
                    logging.debug("check Job Success %s"%checkSuccess(self.reportfilename))
                    success=checkSuccess(self.reportfilename)
                # FwkJobReport not there: create one based on BOSS DB
                else:
                    logging.debug("BOSS check Job Success %s"%BOSSCommands.checkSuccess(jobId[0],self.bossCfgDir))
                    success=BOSSCommands.checkSuccess(jobId[0],self.bossCfgDir)
                    fwjr=FwkJobReport()
                    fwjr.jobSpecId=jobSpecId
                    self.reportfilename=BOSSCommands.reportfilename(jobId,self.directory)
                    # job successful even if FwkJobReport not there
                    if success:
                        logging.info( "%s  no FrameworkReport" % (jobId.__str__() ))
                        logging.info( "%s  Creating FrameworkReport" % (jobId.__str__() ))
                        fwjr.status="Success"
                        fwjr.exitCode=0
                    # job failed
                    else:
                        fwjr.status="Failed"
                        fwjr.exitCode=-1
                    fwjr.write(self.reportfilename)
                
                # in both cases: is the job successful?
                if success:
                    self.jobSuccess(jobId)
                    self.notifyJobState(jobSpecId) 
                else:
                    self.jobFailed(jobId)
                    logging.error( jobId.__str__() + " " + jobSpecId.__str__() )

            # else if output retrieval failed
            elif outp.find("Unable to find output sandbox file:") >= 0 \
                     or outp.find("Error extracting files ") >= 0 \
                     or outp.find("Error retrieving Output") >= 0:
                jobSpecId=BOSSCommands.jobSpecId(jobId[0],self.bossCfgDir)
                logging.info( "%s no FrameworkReport " + jobId.__str__() + " : creating a dummy one" )
                fwjr=FwkJobReport()
                fwjr.jobSpecId=jobSpecId
                self.reportfilename=BOSSCommands.reportfilename(jobId,self.directory)
                fwjr.exitCode=-1
                fwjr.status="Failed"
                fwjr.write(self.reportfilename)
                BOSSCommands.Delete(jobId[0], self.bossCfgDir)
                self.jobFailed(jobId)
                                        
            else:
                logging.error(outp)

        return


    def dashboardPublish(self,jobId,ended=""):
        """
        _dashboardPublish_
        
        publishes dashboard info
        """

        taskid=jobId.split('.')[0]
        chainid=jobId.split('.')[1]
        resub=jobId.split('.')[2]
        dashboardInfoFile = BOSSCommands.subdir(jobId,self.bossCfgDir)+"/DashboardInfo%s_%s_%s.xml"%(taskid,chainid,resub)
        # logging.debug("dashboardinfofile=%s"%dashboardInfoFile)
        # logging.debug("dashboardinfofile exist =%s"%os.path.exists(dashboardInfoFile))
        # logging.debug("ended  ='%s'"%ended)

        
        if os.path.exists(dashboardInfoFile):
            dashboardInfo = DashboardInfo()
            try:
               dashboardInfo.read(dashboardInfoFile)
            except:
  	       logging.error("Reading dashboardInfoFile "+dashboardInfoFile+" failed (jobId="+str(jobId)+")")
  	       return
            gridJobId=dashboardInfo['GridJobID']
            dashboardInfo.clear()
            # logging.debug("dashboardInfoJob=%s"%dashboardInfo.job)
            # logging.debug("retrieving scheduler")
            # logging.debug("jobid=%s"%jobId)
            scheduler=BOSSCommands.scheduler(jobId,self.bossCfgDir,ended)
            logging.debug("scheduler=%s"%scheduler)
            schedulerI=BOSSCommands.schedulerInfo(self.bossCfgDir,jobId,scheduler,ended)
            logging.debug("schedulerinfo%s"%schedulerI.__str__())
            try:
                dashboardInfo['StatusEnterTime']=time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime(float(schedulerI['LAST_T'])))
                dashboardInfo['StatusValue']=schedulerI['SCHED_STATUS']
                dashboardInfo['StatusValueReason']=schedulerI['STATUS_REASON'].replace('-',' ')
                dashboardInfo['StatusDestination']=schedulerI['DEST_CE']+"/"+schedulerI['DEST_QUEUE']
                dashboardInfo['SubTimeStamp']=time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime(float(schedulerI['SUBMITTED'])))
                dashboardInfo['GridJobID']=gridJobId
            
                logging.debug("dashboardinfo%s"%dashboardInfo.__str__())

                dashboardInfo.publish(5)
            except:
                logging.debug("dashboardinfo%s"%dashboardInfo.__str__())

        return


    def eventCallback(self, event, handler):
        """
        _eventCallback_

        This method is called whenever an event is sent to this component,
        but since this component publishes events rather than recieves
        them, there is really nothing to do here.
        
        """
        pass


    def notifyJobState(self, jobId):
        """
        _notifyJobState_

        Notify the JobState DB of finished jobs

        """
        try:

          JobState.finished(jobId)
          Session.commit_all()
        except Exception, ex:
                msg = "Error setting job state to finished for job: %s\n" % jobId
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
        self.ms.publish("JobSuccess", self.reportfilename)
        self.ms.commit()
        
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

        jobSpecId=BOSSCommands.jobSpecId(jobId[0],self.bossCfgDir)
        self.archiveJob("Failed",jobId)
        msg=self.reportfilename
        logging.info("JobFailed: %s" % msg)
        self.ms.publish("JobFailed", msg)
        self.ms.commit()
        logging.info("published JobFailed with payload %s"%msg)
           
        return


    def archiveJob(self,success,jobId):
        """
        _archiveJob_ copy(self.reportfilename,newPath)

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

        # fallback to a dir in JobTracking....it won't be picked up by JobCleanup
        fallbackCacheDir=self.args['ComponentDir'] + "/%s"%fjr[0].jobSpecId

        try:
            jobCacheDir=JobState.general(fjr[0].jobSpecId)['CacheDirLocation']
        except Exception, ex:
            msg = "Cant get JobCache from JobState.general for xxx %s xxx\n" %fjr[0].jobSpecId
            msg += str(ex)
            logging.warning(msg)
            try: 
              WEjobState = WEJob.get(fjr[0].jobSpecId)
              jobCacheDir = WEjobState['cache_dir']
            except Exception, ex:
              msg = "Cant get JobCache from Job.get['cache_dir'] for xxx %s xxx\n" %fjr[0].jobSpecId
              msg += str(ex)
              logging.warning(msg)
              # try guessing the JobCache area based on jobspecId name
              try:
                # split the jobspecid=workflow-run into workflow/run
                spec=fjr[0].jobSpecId
                end=spec.rfind('-')
                workflow=spec[:end]
                run=spec[end+1:]
                # additional split for PM jobspecid that are in the form
                # jobcut-workflow-run
                pmspec=workflow.find('jobcut-')
                if pmspec > 0: workflow=workflow[pmspec+7:]

                PAconfig = loadProdAgentConfiguration()
                jobCreatorCfg = PAconfig.getConfig("JobCreator")
                jobCreatorDir = os.path.expandvars(jobCreatorCfg['ComponentDir'])
                jobCacheDir="%s/%s/%s"%(jobCreatorDir,workflow,run)                                                                                                   
                if not os.path.exists(jobCacheDir):
                  jobCacheDir=fallbackCacheDir

              except Exception, ex:
                msg = "Cant guess JobCache in JobCreator dir" 
                msg += str(ex)
                logging.warning(msg)
                jobCacheDir=fallbackCacheDir

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
            try:
                ext=ext.split('.')[1]
            except:
                ext=""
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
        try:
            jobMaxRetries=JobState.general(fjr[0].jobSpecId)['MaxRetries']
        except:
           try: 
             jobMaxRetries=WEjobState['max_retries']
           except:
             jobMaxRetries=10

        logging.info("maxretries=%s and resub = %s\n"%(jobMaxRetries,resub))
        if success=="Success" or int(jobMaxRetries)<=int(resub):
            try:
                subPath=BOSSCommands.subdir(jobId[0],self.bossCfgDir)
            except:
                subPath=""
            logging.info("SubmissionPath '%s'"%subPath)
            if BOSSCommands.taskEnded(jobId[0],self.bossCfgDir):
                try:
                    rmtree(subPath)
                    BOSSCommands.archive(jobId[0], self.bossCfgDir)
                    logging.info("removed %s for task %s"%(subPath,taskid))
                except:
                    logging.error("Failed to remove submission files")
                # remove ..id file, so that re-declaration is possible if needed
                try:
                    os.remove("%s/%sid"%(jobCacheDir,fjr[0].jobSpecId))
                except: 
                    logging.info("not removed file %s/%sid"%(jobCacheDir,fjr[0].jobSpecId))
                    pass
        return


### not used?!?!
    def subFailed(self, jobId, msg):
        """
        _subFailed_
        
        Publishes a SubmissionFailed event
        
        """
        
        #if it's the first time we see this failed job we  publish JobFailed event and add the job in failedJobsPublished dict
        jobSpecId=BOSSCommands.jobSpecId(jobId[0],self.bossCfgDir)
        JobState.submitFailure(msg)
        logging.info("SubmissionFailed: %s" % msg)
        self.ms.publish("SubmissionFailed", msg)
        self.ms.commit()
        logging.info("published SubmissionFailed with payload %s"%msg)
        return


### used to have persistent information about active jobs with 
### and jobs with application failure so that, in case of component restart,
### the related status is not sent twice to the dashboard
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


### load information about jobs at the component startup
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

        # register
        self.ms.registerAs("TrackingComponent")

        # subscribe to messages
        self.ms.subscribeTo("TrackingComponent:StartDebug")
        self.ms.subscribeTo("TrackingComponent:EndDebug")
        self.ms.subscribeTo("TrackingComponent:pollDB")

        # generate first polling cycle
        self.ms.remove("TrackingComponent:pollDB")
        self.ms.publish("TrackingComponent:pollDB", "")
        self.ms.commit()

        # wait for messages
        while True:
            Session.set_database(dbConfig)
            Session.connect()
            Session.start_transaction()
            type, payload = self.ms.get()
            self.ms.commit()
            logging.debug("TrackingComponent: %s, %s" % (type, payload))
            self.__call__(type, payload)
            Session.commit_all()
            Session.close_all()


