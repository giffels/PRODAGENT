import logging

from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdMgrInterface import Management

service_map={'userID':'prodMgrRequest.userID',\
             'acquireAllocation':'prodMgrProdAgent.acquireAllocation',\
             'acquireJob':'prodMgrProdAgent.acquireJob',\
             'releaseJob':'prodMgrProdAgent.releaseJob',\
             'releaseAllocation':'prodMgrProdAgent.releaseAllocation'}

def userID(serverUrl,componentID="defaultComponent"):
   return Management.executeCall(serverUrl,"prodMgrRequest.userID",[],componentID)
   
def acquireAllocation(serverUrl,request_id,amount,componentID="defaultComponent"):
   return Management.executeCall(serverUrl,"prodMgrProdAgent.acquireAllocation",[request_id,amount],componentID)

def acquireJob(serverUrl,request_id,parameters,componentID="defaultComponent"):
   return Management.executeCall(serverUrl,"prodMgrProdAgent.acquireJob",[request_id,parameters],componentID)

def releaseJob(serverUrl,jobspec,events_completed,componentID="defaultComponent"):
   return Management.executeCall(serverUrl,"prodMgrProdAgent.releaseJob",[str(jobspec),events_completed],componentID)

def releaseAllocation(serverUrl,allocation_id,componentID="defaultComponent"):
   return Management.executeCall(serverUrl,"prodMgrProdAgent.releaseAllocation",[allocation_id],componentID)

def commit(serverUrl=None,method_name=None,componentID=None):
   try:
       Management.commit(serverUrl,method_name,componentID)
   except Exception,ex:
       raise ProdAgentException("commit Error: "+str(ex))

def retrieve(serverUrl=None,method_name=None,componentID="defaultComponent",tag="0"):
   try:
        if method_name!=None:
            quad=Management.retrieve(serverUrl,service_map[method_name],componentID)
        else:
            quad=Management.retrieve(serverUrl,method_name,componentID)
        return Management.executeCall(quad[0],"prodCommonRecover.lastServiceCall",[quad[1],quad[2],quad[3]],componentID)
   except Exception,ex:
       raise ProdAgentException("retrieve Error: "+str(ex))

def retrieveFile(url,local_destination):
   Management.retrieveFile(url,local_destination)
