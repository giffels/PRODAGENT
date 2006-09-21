#!/usr/bin/env python
"""
_PriorityQueue_

Object that keeps a list of requests in order based on their priority


"""
import logging

from ProdAgentCore.ProdAgentException import ProdAgentException

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

    def retrieveRequest(self,reqId):
        """
        _retrieveRequest_

        Returns a particular request.
        """
        for request in self:
           if request['RequestID']==reqId:
               return request
        raise ProdAgentException("Request is not available!")


    def addAllocations(self,reqId,allocations=[]):
        """
        _addAllocations_

        Adds allocations this prodagent aquires
        """
        # little bit innefficient but should be done better in a db
        for request in self:
            if request['RequestID']==reqId:
                for allocation in allocations:
                    request['Allocations'][allocation]='idle'
                break
    
    def delAllocations(self,reqId,allocations=[]): 
        """
        _delAllocations_

        Deletes allocations this prodagent is finished 
        with or if there are not many resources.

        """
        # little bit innefficient but should be done better in a db
        for request in self:
           if request['RequestID']==reqId:
               for allocation in allocations: 
                   del request['Allocations'][allocation]
               break

    def setAllocations(self,reqId,allocations=[],status='idle'):
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
        for request in self:
           if request['RequestID']==reqId:
               for allocation in allocations: 
                   request['Allocations'][allocation]=status
               break

    def orderRequests(self):
        """
        _orderRequests_

        Sort self using a metric that sorts based on Priority setting of each
        request

        """
        self.sort(sortByPriority)
        return

