#!/usr/bin/env python

"""
_Interface_

Methods that describe the interface used for interacting
with the ProdMgr. Current Clarens is used as facilitator
but other facilitator can be used. The Interface methods
insulate the ProdAgent code from this facilitator.
"""

__revision__  =  "$Id: Interface.py,v 1.15 2007/06/02 05:23:05 fvlingen Exp $"
__version__  =  "$Revision: 1.15 $"
__author__  =  "fvlingen@caltech.edu"


import logging

from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdMgrInterface import Management

service_map = {'userID':'prodMgrRequest.userID', \
             'acquireAllocation':'prodMgrProdAgent.acquireAllocation', \
             'acquireEventJob':'prodMgrProdAgent.acquireEventJob', \
             'releaseJob':'prodMgrProdAgent.releaseJob', \
             'releaseAllocation':'prodMgrProdAgent.releaseAllocation', \
             'getRequests':'prodMgrProdAgent.getRequests', \
             'setLocations':'prodMgrProdAgent.setLocations', }

def userID(serverUrl, componentID = "defaultComponent"):
    """
    _userID_

    returns the certifcate DN used by the prodagent
    to interact with this particular prodmgr
    """

    return Management.executeCall(serverUrl,  \
        "prodMgrRequest.userID", [], componentID)

def acquireAllocation(serverUrl, request_id, amount, \
    componentID = "defaultComponent"):
    """
    deprecated
    """
    return Management.executeCall(serverUrl, \
        "prodMgrProdAgent.acquireAllocation", \
        [request_id, amount], \
        componentID)
   
def acquireEventJob(serverUrl, request_id, parameters, \
    componentID = "defaultComponent"):
    """
    _acquireEventJob_

    Returns either an event range or a list of logical
    files (if it is a file based request) for the prodagent
    to work on. This method is invoked when the prodagent
    has resources available. The amount of work returned
    is usually cut on the prodagent side into smaller jobs. 
    """

    return Management.executeCall(serverUrl, \
        "prodMgrProdAgent.acquireEventJob", \
        [request_id, parameters], componentID)

def releaseJob(serverUrl, jobspec, events_completed, \
    componentID = "defaultComponent"):

    """
    _releaseJob_

    Releases work performed by this prodagent (it was
    acquired first). If the work was cut into smaller 
    jobs on the prodagent side these chunks are first 
    aggregated.
    """

    return Management.executeCall(serverUrl, \
        "prodMgrProdAgent.releaseJob", \
        [str(jobspec), events_completed], componentID)

def releaseAllocation(serverUrl, allocation_id, \
    componentID = "defaultComponent"):
    """
    deprecated
    """

    return Management.executeCall(serverUrl, \
        "prodMgrProdAgent.releaseAllocation", \
        [allocation_id], componentID)

def getRequests(serverUrl, agent_tag, componentID = "defaultComponent"):
    return Management.executeCall(serverUrl, \
        "prodMgrProdAgent.getRequests", \
        [agent_tag], componentID)

def setLocations(serverUrl, locations = [], componentID = "defaultComponent"):
    return Management.executeCall(serverUrl, \
        "prodMgrProdAgent.setLocations", \
        [locations], componentID)

def unsubscribeWorkflow(serverUrl, agent_id, workflow_id, componentID = "defaultComponent"):
    """
    _unsubscribeWorkflow_
 
    Unsubscribes from a collection of workflows.
    """
    return Management.executeCall(serverUrl, \
        "prodMgrProdAgent.unsubscribeWorkflow", \
        [agent_id, workflow_id], componentID)
    

def retrieveWorkflow(serverUrl, requestID, componentID = "defaultComponent"):
    return Management.executeRestCall(serverUrl, \
        'psp/prodMgrRequest/retrieveWorkflow.psp?RequestID=' + \
        str(requestID), componentID)

def commit(serverUrl = None, method_name = None, componentID = None):
    Management.commit(serverUrl, method_name, componentID)

def retrieve(serverUrl = None, method_name = None, \
    componentID = "defaultComponent", tag = "0"):

    if method_name != None:
        quad = Management.retrieve(serverUrl, \
            service_map[method_name], componentID)
    else:
       quad = Management.retrieve(serverUrl, method_name, componentID)
    return Management.executeCall(quad[0], \
        "prodCommonRecover.lastServiceCall", [quad[1], \
        quad[2], quad[3]], componentID)

def lastCall(serverUrl = None, method_name = None, \
    componentID = "defaultComponent", tag = "0"):

    if method_name != None:
        quad = Management.retrieve(serverUrl, \
            service_map[method_name], componentID)
    else:
        quad = Management.retrieve(serverUrl, method_name, componentID)
    return quad

def retrieveFile(url, local_destination):
    Management.retrieveFile(url, local_destination)
