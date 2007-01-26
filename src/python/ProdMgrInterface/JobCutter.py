
from ProdAgentDB import Session
from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdCommon.MCPayloads.WorkflowSpec import JobSpec
from ProdCommon.MCPayloads.FactoriseJobSpec import factoriseJobSpec
from ProdMgrInterface import JobCut
from ProdMgrInterface import Job
from ProdMgrInterface import Request


#import ProdCommon.MCPayloads.EventJobSpec

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

def local_cut(job_id,request_id,jobCutSize):
    """
    __local_cut__

    Cuts a job(spec) associated to an allocation received 
    by a prodmgr into a number of smaller jobs as specified 
    by the jobCutSize parameter in the prodagent config file.
    Local cut means it generates the jobspecs locally based
    on the downloaded workflow.
    """
    global jobSpecDir

    jobDetails=Job.getDetails(job_id)
    eventCount=int(jobDetails['end_event'])-int(jobDetails['start_event'])+1
    # find out how many jobs we want to cut.
    jobs=int(math.ceil(float(eventCount)/float(jobCutSize)))
    # keep track of some orginal parameters as we will augment them.
    first_event=int(jobDetails['start_event'])
    
    job_cuts=[]
 
    start_event=first_event
    for job in xrange(0,jobs):
        end_event=start_event+jobCutSize-1
        if ((end_event-first_event)>=eventCount):
           end_event=first_event+eventCount-1

        job_cut_id=job_id+'_jobCut'+str(job)
        workflowspec=Request.getWorlflowLocation(request_id)
        job_cut_file=job_cut_id+'.xml'
        job_cut_location=jobSpecDir+'/'+job_cut_file
        event_count=end_event-start_event+1
        run_number=start_event


        #EventJobSpec.createJobSpec(job_cut_id,workflowspec,job_cut_location,run_number,event_count,start_event)

        job_cut={'id':job_cut_id,\
             'spec':job_cut_location}
        job_cuts.append(job_cut)
        start_event=end_event+1
    JobCut.insert(job_cuts,job_name_NOTE)
    Session.commit()
    return job_cuts


def cut(jobSpecFile,jobCutSize):
    """
    __cut__

    Cuts a job(spec) associated to an allocation received 
    by a prodmgr into a number of smaller jobs as specified 
    by the jobCutSize parameter in the prodagent config file.
    """
    global jobSpecDir

    jobSpec= JobSpec()
    jobSpec.load(jobSpecFile)

    eventCount=int(jobSpec.parameters['EventCount'])
    # find out how many jobs we want to cut.
    jobs=int(math.ceil(float(eventCount)/float(jobCutSize)))
    logging.debug("Writing job cut specs to: "+str(jobSpecDir))
    listOfSpecs=factoriseJobSpec(jobSpec,jobSpecDir,jobs,jobSpec.parameters['EventCount'],\
        RunNumber=jobSpec.parameters['RunNumber'],FirstEvent=jobSpec.parameters['FirstEvent'])
    logging.debug("Registering job cuts")
    JobCut.insert(listOfSpecs,jobSpec.parameters['JobName'])
    Session.commit()
    logging.debug("JobCuts registered")
    for job_cut in listOfSpecs:
        logging.debug("test_cut "+str(JobCut.hasID(job_cut['id'])))
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
       JobCut.insert([job_cut],element['id'])
       line=file.readline()
    # now get the contact url from the jobspec id and register
    # the cuts as jobs (this is different than event based jobs.
    url=Job.getUrl(jobSpecID) 
    Job.rm(jobSpecID)
    try:
        os.remove(jobSpecFile)
    except:
        pass
    jobs=[]
    for job_cut in job_cuts:
        job={}
        job['jobSpecId']=job_cut['parent_id']
        job['URL']='none'
        jobs.append(job)
    Job.insert('active',jobs,request_id,url)
    Session.commit()
    return job_cuts
