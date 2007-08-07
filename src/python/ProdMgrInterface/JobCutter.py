#!/usr/bin/env python

"""
_JobCutter

Methods that cut jobs based on an event range (or set of files)
returned from the ProdMgr.

"""

__revision__ = "$Id: JobCutter.py,v 1.12 2007/07/25 20:24:28 fvlingen Exp $"
__version__ = "$Revision: 1.12 $"
__author__ = "fvlingen@caltech.edu"


from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdCommon.Database import Session
from ProdCommon.MCPayloads.WorkflowSpec import JobSpec
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.FactoriseJobSpec import factoriseJobSpec
from ProdAgent.WorkflowEntities import Job as Job
from ProdAgent.WorkflowEntities import Allocation
from ProdAgent.WorkflowEntities import Workflow


from ProdCommon.MCPayloads import EventJobSpec

import logging
import math
import os

jobSpecDir='/tmp'
maxRetries=None
try:
    config = loadProdAgentConfiguration()
    compCfg = config.getConfig("ProdMgrInterface")
    jobSpecDir=compCfg['JobSpecDir']
    jobStatesCfg = config.getConfig("JobStates")
    if jobStatesCfg.has_key('maxRetries'):
       maxRetries=jobStatesCfg['maxRetries']
except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    logging.info("WARNING: "+msg)

def cut(job_id,jobCutSize):
    """
    __cut__

    Cuts a job(spec) associated to an allocation received 
    by a prodmgr into a number of smaller jobs as specified 
    by the jobCutSize parameter in the prodagent config file.
    """
    global jobSpecDir,maxRetries
    # generate the jobspec
    jobDetails=Allocation.get(job_id)['details']
    workflowspec=Workflow.get(job_id.split('_')[1])['workflow_spec_file']
    first_event=int(jobDetails['start_event'])
    event_count=int(jobDetails['end_event'])-int(jobDetails['start_event'])+1
    run_number=int(jobDetails['start_event'])
    start_event=run_number
    job_file=job_id+'.xml'
    jobSpecFile=jobSpecDir+'/'+job_file
    Allocation.setAllocationSpecFile(job_id,jobSpecFile)
    logging.debug("start with local jobspec generation")
    EventJobSpec.createJobSpec(job_id,workflowspec,jobSpecFile,run_number,event_count,start_event,False,False)
    logging.debug("finished with local jobspec generation")

    jobSpec= JobSpec()
    jobSpec.load(jobSpecFile)
    jobSpec.parameters['ProdMgr']='generated'

    eventCount=int(jobSpec.parameters['EventCount'])
    # find out how many jobs we want to cut.
    jobs=int(math.ceil(float(eventCount)/float(jobCutSize)))
    logging.debug("Retrieve "+str(jobs)+" run numbers")
    job_run_numbers=Workflow.getNewRunNumber(job_id.split('_')[1],jobs)
    logging.debug("Got: "+str(job_run_numbers))
    logging.debug("Starting factorization")
    logging.debug("Writing job cut specs to: "+str(jobSpecDir))
    listOfSpecs=factoriseJobSpec(jobSpec,jobSpecDir,job_run_numbers,jobSpec.parameters['EventCount'],\
        RunNumber=jobSpec.parameters['RunNumber'],FirstEvent=jobSpec.parameters['FirstEvent'])
    logging.debug("test10")
    for i in xrange(0,len(listOfSpecs)):
        listOfSpecs[i]['owner'] = 'prodmgr'
        if maxRetries:
           listOfSpecs[i]['max_retries']=maxRetries
    logging.debug("Registering job cuts")
    Job.register(None,job_id,listOfSpecs)
    Session.commit()
    logging.debug("Jobs registered")
    return listOfSpecs 

def cutFile(job_ids,jobCutSize,maxJobs):
    global jobSpecDir,maxRetries

    logging.debug("Job_ids: "+str(job_ids))
    jobIDs=job_ids.split(',')
    listOfSpecs=[]
    for job_id in jobIDs[:-1]:
        jobDetails=Allocation.get(job_id)['details']
        logging.debug("Job details: "+str(jobDetails))
        workflowspec=Workflow.get(job_id.split('_')[1])['workflow_spec_file']
        job_file=job_id+'.xml'
        jobSpecFile=jobSpecDir+'/'+job_file
        Allocation.setAllocationSpecFile(job_id,jobSpecFile)
        logging.debug("start with local jobspec generation")
        run_number=int(jobDetails['start_event'])
        event_count=int(jobDetails['event_count'])
        # find out how many jobs we want to cut.
        jobs=int(math.ceil(float(event_count)/float(jobCutSize)))
        if jobs>maxJobs and maxJobs>0:
            jobs=maxJobs
            maxJobs=maxJobs-jobs
        start_event=run_number
        EventJobSpec.createJobSpec(job_id,workflowspec,jobSpecFile,run_number,event_count,start_event,False,False)
        jobSpec= JobSpec()
        jobSpec.load(jobSpecFile)
        jobSpec.parameters['ProdMgr']='generated'
        fileData={}
        fileData['LFN']=jobDetails['lfn']
        jobSpec.addAssociatedFiles('fileList', fileData)
        jobSpec.save(jobSpecFile)
        job_run_numbers=Workflow.getNewRunNumber(job_id.split('_')[1],jobs)
        logging.debug("Starting factorization")
        logging.debug("Writing job cut specs to: "+str(jobSpecDir))
        listOfSpecs=factoriseJobSpec(jobSpec,jobSpecDir,job_run_numbers,jobSpec.parameters['EventCount'],\
            RunNumber=jobSpec.parameters['RunNumber'],FirstEvent=jobSpec.parameters['FirstEvent'])
        logging.debug("Registering job cuts")
        for i in xrange(0,len(listOfSpecs)):
            listOfSpecs[i]['owner'] = 'prodmgr'
            if maxRetries:
               listOfSpecs[i]['max_retries']=maxRetries
        Job.register(None,job_id,listOfSpecs)
    Session.commit()
    return listOfSpecs
