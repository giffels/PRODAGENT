#!/usr/bin/env python
"""
Resubmit jobs in FailureArchive

"""
from MessageService.MessageService import MessageService
from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdCommon.Database import Session
from ProdCommon.MCPayloads.JobSpec import JobSpec

import sys,os,getopt
import time,tarfile


usage="\n Description: this script resubmit jobs from FailureArchive dir \n Usage: python ResubmitArchivedFailures.py <options> \n Options: \n --workflowname=<workflowName> \t\t workflow name \n --jobname=<jobspecName> \t\t jobspec name \n --all \t\t\t all jobs in FailureArchive dir\n --jobQueue=<true|false> - if not given take from pa config.\n"
valid = ['workflowname=', 'jobname=','all','jobQueue=']

try:
    opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
    print usage
    print str(ex)
    sys.exit(1)

workflow = None
jobname = None
alljobs = False
useJobQueue = None


for opt, arg in opts:
    if opt == "--workflowname":
        workflow = arg
    if opt == "--jobname":
        jobname = arg
    if opt == "--all":
        alljobs = True
    if opt == "--jobQueue":
        if arg.lower() in ('true', 'yes'):
            useJobQueue = True
        else:
            useJobQueue = False

# ##########################
def getFailureArchiveDir():
   """
   get the FailureArchive dir from JobCleanup Component config
   """
   try:
     config = loadProdAgentConfiguration()
   except StandardError, ex:
     msg = "Error: error reading configuration:\n"
     msg += str(ex)
     print msg
     sys.exit(1)

   if not config.has_key("JobCleanup"):
      msg = "Error: Configuration block JobCleanup is missing from $PRODAGENT_CONFIG"
      print msg
      sys.exit(1)

   JCConfig = config.getConfig("JobCleanup")
   failureArchive=JCConfig.get("FailureArchive", None)
   if not os.path.isdir(failureArchive):
      cleanupDir=JCConfig.get("ComponentDir",None)
      failureArchive=os.path.join(cleanupDir,failureArchive)

   return failureArchive

# ##########################
def getJobQueueConfig():
    """
    find out if we should use the jobQueue
    """
    try:
        config = loadProdAgentConfiguration()
    except StandardError, ex:
        msg = "Error: error reading configuration:\n"
        msg += str(ex)
        print msg
        sys.exit(1)

    mergeConfig = config.getConfig("MergeSensor")
    if mergeConfig.get('QueueJobMode', 'false').lower() in ('true', 'yes'):
        global useJobQueue
        useJobQueue = True

# ##########################
def ResubmitJobs(TarFileList,FailureDir,ResubDir,jobQueue):
  """
  For each tarfile in FailureDir, move it to ResubDir
  extract the jobSpec File and re-create the job
  """
  for TarFile in TarFileList:
   #
   # mv tar file to "ResubmitFailures" subdir
   #
   FailureTarFile=os.path.join(FailureDir,TarFile)
   ResubTarFile=os.path.join(ResubDir,TarFile)
   os.rename(FailureTarFile,ResubTarFile)
   #
   # extract the JobSpec File
   #
   if tarfile.is_tarfile(ResubTarFile):
      jobtarfile = tarfile.open(ResubTarFile, 'r:gz')
      specFileList=[tf for tf in jobtarfile.getnames() if (tf.count("JobSpec.xml") and not tf.count("BULK"))]

      tarspecFile=specFileList[0]
      #jobspecFile=os.path.join(ResubDir,os.path.basename(tarspecFile))
      jobspecFile="%s/%s_%s"%(ResubDir,os.path.basename(tarspecFile),time.time())
      try:
         jobtarfile.extract(tarspecFile,ResubDir)
         os.rename("%s/%s"%(ResubDir,tarspecFile),jobspecFile)
         os.rmdir(os.path.join(ResubDir,os.path.dirname(tarspecFile)))
      except Exception, ex:
         print "Error extracting JobSpec file from % :%s"%(ResubTarFile,str(ex))
         jobspecFile=None
         return
   else:
      print "Error: file %s is not a tarfile "%(ResubTarFile,)
      return
   #
   # re-create job
   #
   recreateJob(jobspecFile, jobQueue)

# ##########################
def clean_tr_tables(jobspecFile):
  """

  remove job entries from tr_Trigger and tr_Action tables

  """
  spec = JobSpec()
  spec.load(jobspecFile)
  try:
     jobspecid=spec.parameters['JobName']
  except Exception,ex:
     msg = "Problem extracting jobspec name from JobSpec File: %s Details: %s"%(jobspecFile,str(ex))
     print msg
     return

  Session.set_database(dbConfig)
  Session.connect()

  sqlStr1 = """DELETE FROM tr_Trigger WHERE JobSpecID="%s" """ % (jobspecid)
  Session.execute(sqlStr1)
  sqlStr2 = """DELETE FROM tr_Action WHERE JobSpecID="%s" """ % (jobspecid)
  Session.execute(sqlStr2)

  Session.commit_all()

# ##########################
def recreateJob(jobspecFile, jobQueue):
  """

  re-create the processing job

  """
  # remove entries from tr_Trigger/Action tables to be on safer side
  clean_tr_tables(jobspecFile)

  # create job if not merge
  spec = JobSpec()
  spec.load(jobspecFile)

  #  //
  # // clean spec id from the job queue
  #//  No easy way to do this in JobQueueAPI so use nekkid SQL for now
  Session.set_database(dbConfig)
  Session.connect()
  sqlStr1 = "DELETE FROM jq_queue WHERE job_spec_id=\"%s\"; " % spec.parameters['JobName']
  Session.execute(sqlStr1)
  Session.commit_all()



  if spec.parameters['JobType'] in ('Processing', 'CleanUp', 'LogCollect', 'Harvesting'):
     # publish CreateJob
     print "- Resubmit Processing job"
     print "--> Publishing CreateJob for %s"%jobspecFile
     ms = MessageService()
     ms.registerAs("Test")
     if jobQueue:
         ms.publish("QueueJob", jobspecFile)
     else:
         ms.publish("CreateJob", jobspecFile)
     ms.commit()
  elif spec.parameters['JobType']=="Merge" :
     try:
       jobname=spec.parameters['JobName']
     except Exception,ex:
       msg = "Problem extracting jobspec name from JobSpec File: %s Details: %s"%(jobspecFile,str(ex))
       print msg
       return

     print "- Resubmit Merge job"
     print "--> Publishing GeneralJobFailures for %s"%jobname
     ms = MessageService()
     ms.registerAs("TestMA")
     ms.publish("GeneralJobFailure", jobname)
     ms.commit()
     time.sleep(1)
     print "--> Publishing MergeSensor:ReSubmit for %s"%jobname
     ms = MessageService()
     ms.registerAs("Test")
     ms.publish("MergeSensor:ReSubmit", jobname)
     ms.commit()
  else:
     print "ERROR: Do not know how to handle jobType %s"%spec.parameters['JobType']



###################################################
if __name__ == '__main__':

 FailureDir=getFailureArchiveDir()
 ResubDir=os.path.join(FailureDir,"ResubmitFailures")
 if not os.path.isdir(ResubDir):
         os.mkdir(ResubDir)
 if useJobQueue is None:
     getJobQueueConfig()

 if alljobs:
 #
 # all jobs in FailureArchive dir
 #
    TarFileList=[ f for f in os.listdir(FailureDir) if os.path.isfile(os.path.join(FailureDir, f)) ]
 matchingcriteria=None
 if workflow:
   matchingcriteria=workflow
 elif jobname:
   matchingcriteria=jobname
 elif not alljobs:
    print "either --workflow or --jobname should be provided."
    print usage
    sys.exit()
 if matchingcriteria:
 #
 # look for tar file matching jobname/workflow
 #
    TarFileList=[ f for f in os.listdir(FailureDir) if f.count(matchingcriteria) > 0 and os.path.isfile(os.path.join(FailureDir, f)) ]


 ResubmitJobs(TarFileList,FailureDir,ResubDir, useJobQueue)



