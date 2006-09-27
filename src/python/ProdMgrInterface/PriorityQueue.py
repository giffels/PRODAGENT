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


