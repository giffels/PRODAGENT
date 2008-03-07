#/usr/bin/env python

from ProdCommon.Database import Session

separator = '@'
old_separator = '_'

def split(inputString):
    """
    auxilerary method to smootly transition
    to new separators.
    """
    global separator
    global old_separator

    # check if the string contains the new separator
    if inputString.find(separator) < 0:
        return inputString.split(old_separator)
    return inputString.split(separator)

def getSeparator(inputString):
    """
    Auxilerary method to get the right separator.
    """
    global separator
    global old_separator

    # check if the string contains the new separator
    if inputString.find(separator) < 0:
        return old_separator
    return separator


def removeJob(jobIDs=[] , entityType = 'job'):
   """
   __remove__

   removes the jobs with the specified ids.
   """
   if(type(jobIDs)!=list):
       jobIDs=[str(jobIDs)]
   if len(jobIDs)==0:
       return

   id = 'id'
   if entityType == 'allocation':
       id = 'allocation_id'

   if len(jobIDs)==1:
       sqlStr1="""DELETE FROM we_Job WHERE %s="%s"
       """ % (id, str(jobIDs[0]))
       sqlStr2="""DELETE FROM tr_Trigger WHERE
       JobSpecID='%s' """ %(jobIDs[0])
       sqlStr3="""DELETE FROM tr_Action WHERE
       JobSpecID='%s' """ %(jobIDs[0])
   else:
       sqlStr1="""DELETE FROM we_Job WHERE %s IN %s
       """ % (id, str(tuple(jobIDs)))
       sqlStr2="""DELETE FROM tr_Trigger WHERE
       JobSpecID IN %s""" %(str(tuple(jobIDs)))
       sqlStr3="""DELETE FROM tr_Action WHERE
       JobSpecID IN %s""" %(str(tuple(jobIDs)))
   Session.execute(sqlStr1)
   #Session.execute(sqlStr2)
   Session.execute(sqlStr3)


def removeAllocation(allocationID=[]):
   """ 
   __remove__
   removes allocations with a particular ID
   """
   if(type(allocationID)!=list):
       allocationID=[str(allocationID)]
   if len(allocationID)==0:
       return
   if len(allocationID)==1:
       sqlStr="""DELETE FROM we_Allocation WHERE id="%s" """ %(str(allocationID[0]))
   else:
       sqlStr="""DELETE FROM we_Allocation WHERE id IN %s """ %(str(tuple(allocationID)))
   Session.execute(sqlStr)


def removeWorkflow(workflowID=[]):
    """
    __remove__
 
    removes a (or multiple)  workflow entry (entries)
    """
    if(type(workflowID)!=list):
        workflowID=[str(workflowID)]
    if len(workflowID)==0:
        return
    if len(workflowID)==1:
        sqlStr="""DELETE FROM we_Workflow WHERE id="%s"
        """ %(str(workflowID[0]))
    else:
        sqlStr="""DELETE FROM we_Workflow WHERE id IN 
        %s """ %(str(tuple(workflowID)))
    # first get the jobs associated to this workflow
    allocationIDs = getAllocationIDs(workflowID)
    removeJob(allocationIDs,'allocation')
    removeAllocation(allocationIDs)
    Session.execute(sqlStr)



def getJobIDs(workflowIDs = []):
    """
    __getJobsIDs__
 
    returns jobids associated to the list of workflowIDs
    """
    if(type(workflowIDs)!=list):
        workflowIDs=[str(workflowIDs)]
    if len(workflowIDs)==0:
        return []
    if len(workflowIDs)==1:
        sqlStr="""SELECT id FROM we_Job WHERE workflow_id='%s'
        """ %(str(workflowIDs[0]))
    else:
        sqlStr="""SELECT id FROM we_Job WHERE workflow_id IN %s
        """ %(str(tuple(workflowIDs)))
    Session.execute(sqlStr)
    rows=Session.fetchall()
    result=[]
    for row in rows:
       result.append(row[0])
    return result 

def getAllocationIDs(workflowIDs =[]):
    """
    __getAllocationIDs
 
    returns allocationIDs associated to the list of workflowIDs
    """
    if(type(workflowIDs)!=list):
        workflowIDs=[str(workflowIDs)]
    if len(workflowIDs)==0:
        return []
    if len(workflowIDs)==1:
        sqlStr="""SELECT id FROM we_Allocation WHERE workflow_id='%s'
        """ %(str(workflowIDs[0]))
    else:
        sqlStr="""SELECT id FROM we_Allocation WHERE workflow_id IN %s
        """ %(str(tuple(workflowIDs)))
    Session.execute(sqlStr)
    rows=Session.fetchall()
    result=[]
    for row in rows:
       result.append(row[0])
    return result 

