#!/usr/bin/env python

"""
_JobCutter

Methods that cut jobs based on an event range (or set of files)
returned from the ProdMgr.

"""

__revision__ = "$Id: JobCutter.py,v 1.17 2008/02/14 10:07:56 fvlingen Exp $"
__version__ = "$Revision: 1.17 $"
__author__ = "fvlingen@caltech.edu"


from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgent.WorkflowEntities import Aux
from ProdAgent.WorkflowEntities import Job
from ProdAgent.WorkflowEntities import Allocation
from ProdAgent.WorkflowEntities import Workflow
from ProdCommon.Database import Session
from ProdCommon.MCPayloads.WorkflowSpec import JobSpec
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
from ProdCommon.MCPayloads.FactoriseJobSpec import factoriseJobSpec
from ProdCommon.MCPayloads import EventJobSpec

from ProdMgrInterface.RequestJobFactory import RequestJobFactory

import logging
import math
import os

jobSpecDir='/tmp'
maxRetries = None
requestJobFactory =  None

def initialize():
    global maxRetries, requestJobFactory, jobSpecDir
    if not requestJobFactory:
        try:
            config = loadProdAgentConfiguration()
            compCfg = config.getConfig("ProdMgrInterface")
            jobSpecDir=compCfg['JobSpecDir']
            jobStatesCfg = config.getConfig("JobStates")
            if jobStatesCfg.has_key('maxRetries'):
               maxRetries=jobStatesCfg['maxRetries']
            logging.debug("Initializing RequestJobFactory")
            requestJobFactory = RequestJobFactory(None,'',0)
            requestJobFactory.workflowSpec = WorkflowSpec() 
            requestJobFactory.workingDir = compCfg['JobSpecDir']
            logging.debug("Writing job cut specs to: "+str(jobSpecDir))
        except StandardError, ex:
            msg = "Error reading configuration:\n"
            msg += str(ex)
            logging.info("WARNING: "+msg)


def cut(job_id,jobCutSize, allocation = None):
    """
    __cut__

    Cuts a job(spec) associated to an allocation received 
    by a prodmgr into a number of smaller jobs as specified 
    by the jobCutSize parameter in the prodagent config file.
    """
    global maxRetries, requestJobFactory 

    try:
        initialize()
    except:
        raise

    if allocation != None:
        jobDetails = allocation['details']
    else:
        jobDetails=Allocation.get(job_id)['details']

    workflowspecFile = Workflow.get(Aux.split(job_id)[1])['workflow_spec_file']
    WorkflowPriority = 1
    try:
        id = Aux.split(job_id)[1]
        logging.debug("Looking for priority of workflow: %s" \
            % (id))
        WorkflowPriority = Workflow.get(workflowID = id)['priority']
    except Exception, ex:
        logging.debug("Priority Not found for workflow: %s . Details: %s" \
            % (id, str(ex)))

    # load the workflow spec to object from file
    requestJobFactory.workflowSpec.load(workflowspecFile)
    event_count = int(jobDetails['end_event'])-int(jobDetails['start_event'])+1
    numberOfJobs = int(math.ceil(float(event_count)/float(jobCutSize)))
    requestJobFactory.start_event =int(jobDetails['start_event'])
    requestJobFactory.totalEvents= event_count
    requestJobFactory.job_run_numbers = Workflow.getNewRunNumber(Aux.split(job_id)[1], \
        numberOfJobs)
    requestJobFactory.job_prefix = job_id
    logging.debug("Starting factorization: Calling RequestJobFactory")
    requestJobFactory.init()


    listOfSpecs = requestJobFactory()
    for i in xrange(0,len(listOfSpecs)):
        listOfSpecs[i]['owner'] = 'prodmgr'
        listOfSpecs[i]['job_type'] = 'Processing'
        if maxRetries:
           listOfSpecs[i]['max_retries']=maxRetries
    logging.debug("Registering job cuts")
    Job.register(Aux.split(job_id)[1],job_id,listOfSpecs)
    Session.commit()
    logging.debug("Jobs registered")
    return {'specs' : listOfSpecs, 'workflow' : Aux.split(job_id)[1], \
        'priority' : WorkflowPriority} 

def cutFile(job_ids,jobCutSize,maxJobs):
    global jobSpecDir,maxRetries

    logging.debug("Job_ids: "+str(job_ids))
    jobIDs=job_ids.split(',')
    listOfSpecs=[]
    for job_id in jobIDs[:-1]:
        jobDetails=Allocation.get(job_id)['details']
        logging.debug("Job details: "+str(jobDetails))
        workflowspec=Workflow.get(Aux.split(job_id)[1])['workflow_spec_file']
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
        job_run_numbers=Workflow.getNewRunNumber(Aux.split(job_id)[1],jobs)
        logging.debug("Starting factorization")
        logging.debug("Writing job cut specs to: "+str(jobSpecDir))
        listOfSpecs=factoriseJobSpec(jobSpec,jobSpecDir,job_run_numbers,jobSpec.parameters['EventCount'],\
            RunNumber=jobSpec.parameters['RunNumber'],FirstEvent=jobSpec.parameters['FirstEvent'])
        logging.debug("Registering job cuts")
        for i in xrange(0,len(listOfSpecs)):
            listOfSpecs[i]['owner'] = 'prodmgr'
            listOfSpecs[i]['job_type'] = 'Processing'
            if maxRetries:
               listOfSpecs[i]['max_retries']=maxRetries
        Job.register(None,job_id,listOfSpecs)
    Session.commit()
    return {'specs' : listOfSpecs, 'workflow' : 'test', \
        'priority' : 1} 
