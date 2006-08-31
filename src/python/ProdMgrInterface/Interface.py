import logging

from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdMgrInterface import Management

def userID(serverUrl,componentID="defaultComponent"):
   try:
      connection=Management.getConnection(serverUrl)
      Management.logServiceCall(serverUrl,"prodMgrRequest.userID",[],componentID)
      result=connection.execute("prodMgrRequest.userID",[])
      return result
   except Exception,ex:
       raise ProdAgentException("userID Service Error: "+str(ex))
   
def acquireAllocation(serverUrl,request_id,amount,componentID="defaultComponent"):
   try:
       connection=Management.getConnection(serverUrl)
       Management.logServiceCall(serverUrl,"prodMgrProdAgent.acquireAllocation",[request_id,amount],componentID)
       allocations=connection.execute("prodMgrProdAgent.acquireAllocation",[request_id,amount])
       return allocations
   except Exception,ex:
       raise ProdAgentException("acquireAllocation Service Error: "+str(ex))

def acquireJob(serverUrl,request_id,parameters,componentID="defaultComponent"):
   try:
       connection=Management.getConnection(serverUrl)
       Management.logServiceCall(serverUrl,"prodMgrProdAgent.acquireJob",[request_id,parameters],componentID)
       jobs=connection.execute("prodMgrProdAgent.acquireJob",[request_id,parameters])
       return jobs
   except Exception,ex:
       raise ProdAgentException("acquireJob Service Error: "+str(ex))

def releaseJob(serverUrl,jobspec,events_completed,componentID="defaultComponent"):
   try:
       connection=Management.getConnection(serverUrl)
       Management.logServiceCall(serverUrl,"prodMgrProdAgent.releaseJob",[str(jobspec),events_completed],componentID)
       finished=connection.execute("prodMgrProdAgent.releaseJob",[str(jobspec),events_completed])
       return finished
   except Exception,ex:
       raise ProdAgentException("releaseJob Service Error: "+str(ex))


def commit(serverUrl=None,method_name=None,componentID=None):
   try:
       Management.commit(serverUrl,method_name,componentID)
   except Exception,ex:
       raise ProdAgentException("commit Service Connection Error: "+str(ex))

def retrieve():
   try:
       i='donothing'
   except Exception,ex:
       raise ProdAgentException("commit Service Connection Error: "+str(ex))
