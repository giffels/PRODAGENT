import math

from ProdAgentDB import Session
from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdCommon.MCPayloads.WorkflowSpec import JobSpec
from ProdMgrInterface import JobCut
from ProdMgrInterface import Job

import logging
import os

jobSpecDir='/tmp'

try:
    config = loadProdAgentConfiguration()
    compCfg = config.getConfig("ProdMgrInterface")
    jobSpecDir=compCfg['JobSpecDir']
except StandardError, ex:
    msg = "Error reading configuration:\n"
    msg += str(ex)
    raise RuntimeError, msg

def cut(jobSpecFile,jobCutSize):
    """
    __jobCut__

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

    # keep track of some orginal parameters as we will augment them.
    first_event=int(jobSpec.parameters['FirstEvent'])
    event_count=int(jobSpec.parameters['EventCount'])
    job_name=jobSpec.parameters['JobName']

    job_cuts=[]

    for job in xrange(0,jobs):
        start_event=int(jobSpec.parameters['FirstEvent'])
        end_event=start_event+jobCutSize-1
        if ((end_event-first_event)>=event_count):
           end_event=first_event+event_count-1
        jobSpec.parameters['EventCount']=end_event-start_event+1
        jobSpec.setJobName(job_name+'_jobCut'+str(job))

        jobSpec.save(jobSpecDir+'/'+jobSpec.parameters['JobName']+'.xml')
        jobSpec.parameters['RunNumber']=int(jobSpec.parameters['RunNumber'])+1
        jobSpec.parameters['FirstEvent']=end_event+1
        job_cut={'id':jobSpec.parameters['JobName'],\
             'spec':jobSpecDir+'/'+jobSpec.parameters['JobName']+'.xml'}
        job_cuts.append(job_cut)
    JobCut.insert(job_cuts,job_name)
    Session.commit()
    return job_cuts


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
