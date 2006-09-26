#!/usr/bin/env python
"""
_PriorityQueue_

Object that keeps a list of requests in order based on their priority


"""
import logging

from ProdAgentCore.ProdAgentException import ProdAgentException

#NOTE: all functionality exposed by the priority queue needs to be made
#NOTE: persistent to avoid losing information during a crash. 
#NOTE: not only the request queue, but also the allocations acquired
#NOTE: by the prodagent from a prodmgr and the jobs it aquired based
#NOTE: on the allocations

class RequestRecord(dict):
    """
    _RequestRecord_

    Object to contain details about a PM and request along with its
    priority.

    This object should be easily saved/loaded from a persistent DB table
    in the future

    """
    def __init__(self):
        dict.__init__(self)
        self.setdefault("RequestID" , None)
        self.setdefault("ProdMgrURL", None)
        self.setdefault("Priority", 0)

class AllocationRecord(dict):
    def __init__(self):
        dict.__init__(self)
        self.setdefault("AllocationID" , None)
        self.setdefault("State", "idle")

class JobRecord(dict):
    """
    _JobRecord_

    Object to contain details about a PM and job

    This object should be easily saved/loaded from a persistent DB table
    in the future

    """
    def __init__(self):
        dict.__init__(self)
        self.setdefault("JobSpecURL" , None)
        self.setdefault("JobSpecID", None)
        self.setdefault("StartEvent", None)
        self.setdefault("EndEvent", None)


def sortByPriority(record1, record2):
    """
    _sortByPriority_

    Sort metric: determines which record has highest priority

    """
    prior1 = record1.get("Priority", -1)
    prior2 = record2.get("Priority", -1)
    if prior1 > prior2:
        return 1
    if prior1 == prior2:
        return 0
    return -1


class JobQueue(list):
    def __init__(self):
        list.__init__(self)

   
    def add(self,jobs=[]):

      for job in jobs:
         newJob= JobRecord();
         newJob['JobSpecURL']=job['URL']
         newJob['JobSpecID']=job['jobSpecId']
         newJob['StartEvent']=job['start_event']
         newJob['EndEvent']=job['end_event']
         self.append(newJob)

    def delJob(self,jobSpecID):
      for job in self:      
         if job['JobSpecID']==jobSpecID:
               del job
               break

    def hasJobs(self,requestID):
      for job in self:
         request_id=job['JobSpecURL'].split('/')[0]
         if request_id==requestID:
             return True
      return False

       
class AllocationQueue(list):       
    def __init__(self):
        list.__init__(self)

    def add(self,allocations=[]):
        """
        _addAllocations_

        Adds allocations this prodagent aquires
        """
        # little bit innefficient but should be done better in a db
        for allocation in allocations:
           newAllocation=AllocationRecord()
           newAllocation['AllocationID']=allocation
           self.append(newAllocation)
       
    def getIdle(self,reqId):
        """
        _getIdleAllocations_
        
        returns allocations allocated by this prodagent which
        are currently idle. Note: this can be done more efficient
        in a database
        """
        result=[]
        for allocation in self:
            if allocation['AllocationID'].split('/')[0]==reqId:
                if allocation['State']=='idle':
                   result.append(allocation['AllocationID'])
        return result

    
    def delAllocation(self,allocations=[]): 
        """
        _delAllocations_

        Deletes allocations this prodagent is finished 
        with or if there are not many resources.

        """
        # little bit innefficient but should be done better in a db
        for allocation in self:
           if allocation['AllocationID']==allocation:
              del allocation

    def setState(self,reqId,allocations=[],status='idle'):
        """
        _setAllocations_

        Sets the status of an allocation. Once we acquire allocations
        the default will be idle. If we are running a job on it, the
        allocation is busy prodmgr keeps track of it too. We need to 
        know how many allocations are busy to determine if we need
        to acquire more allocations for this request. We also need
        to keep track of the allocations so we can release them later
        if necessary.
        """
        index=0
        for allocation in self:
           if allocation['AllocationID']==allocation:
               self[index]['State']=status

class PriorityQueue(list):
    """
    _PriorityQueue_


    List based object that keeps a prioritised list of requests along
    with their priority value (an integer)

    """
    def __init__(self):
        list.__init__(self)

    def addRequest(self, reqId, prodMgr, priority):
        """
        _addRequest_

        Add a new request to this list

        """
        newReq = RequestRecord()
        newReq['RequestID'] = reqId
        newReq['ProdMgrURL'] = prodMgr
        newReq['Priority'] = priority
        newReq['Allocations']={}
        self.append(newReq)
        return newReq

    def delRequest(self,reqId):
        for request in self:
           if request['RequestID']==reqId:
               del request
               break

    def orderRequests(self):
        """
        _orderRequests_

        Sort self using a metric that sorts based on Priority setting of each
        request

        """
        self.sort(sortByPriority)
        return

    def retrieveRequest(self,reqId):
        """
        _retrieveRequest_

        Returns a particular request.
        """
        for request in self:
           if request['RequestID']==reqId:
               return request
        raise ProdAgentException("Request is not available!")


