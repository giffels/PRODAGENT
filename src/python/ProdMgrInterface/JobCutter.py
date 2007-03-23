
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

try:
    config = loadProdAgentConfiguration()
    compCfg = config.getConfig("ProdMgrInterface")
    jobSpecDir=compCfg['JobSpecDir']
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
    global jobSpecDir

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
    logging.debug("Registering job cuts")
    Job.register(None,job_id,listOfSpecs)
    Session.commit()
    logging.debug("Jobs registered")
    return listOfSpecs 

def cutFile(jobSpecFile,request_id):
    global jobSpecDir
    #NOTE: this needs to be replaced by the official payload package when
    #NOTE: available.
    file=open(jobSpecFile,'r')
    line=file.readline()
    job_cuts=[]
    jobSpecID=''
    while line:
       if line.find('ENDFILES')>-1:
          logging.debug('found ENDFILES')
          jobSpecID=file.readline().split(':')[0]
          logging.debug('JobSpecID '+str(jobSpecID))
          break
       segments=line.split(',')
       element={}
       element['id']=segments[0].split(':')[1]
       filefile=open(jobSpecDir+'/'+element['id']+'_jobCut.xml','w')
       filefile.write(str(element['id']+'_jobCut'))
       filefile.close()
       job_cut={'id':element['id']+'_jobCut',\
             'spec':jobSpecDir+'/'+element['id']+'_jobCut.xml',\
             'parent_id':element['id']}
       job_cuts.append(job_cut)
       #JobCut.insert([job_cut],element['id'])
       line=file.readline()
    # now get the contact url from the jobspec id and register
    # the cuts as jobs (this is different than event based jobs.
    #url=Job.getUrl(jobSpecID) 
    #Job.rm(jobSpecID)
    #try:
    #    os.remove(jobSpecFile)
    #except:
    #    pass
    #jobs=[]
    #for job_cut in job_cuts:
    #    job={}
    #    job['jobSpecId']=job_cut['parent_id']
    #    job['URL']='none'
    #    jobs.append(job)
    #Job.insert('active',jobs,request_id,url)
    Session.commit()
    return job_cuts
