import logging

from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdMgrInterface import Management

service_map={'userID':'prodMgrRequest.userID',\
             'acquireAllocation':'prodMgrProdAgent.acquireAllocation',\
             'acquireJob':'prodMgrProdAgent.acquireJob',\
             'releaseJob':'prodMgrProdAgent.releaseJob',\
             'releaseAllocation':'prodMgrProdAgent.releaseAllocation'}

def userID(serverUrl,componentID="defaultComponent"):
   try:
      connection=Management.getConnection(serverUrl)
      Management.logServiceCall(serverUrl,"prodMgrRequest.userID",[],componentID)
      result=connection.execute("prodMgrRequest.userID",[componentID])
      return result
   except Exception,ex:
       raise ProdAgentException("userID Service Error: "+str(ex))
   
def acquireAllocation(serverUrl,request_id,amount,componentID="defaultComponent"):
   try:
       connection=Management.getConnection(serverUrl)
       Management.logServiceCall(serverUrl,"prodMgrProdAgent.acquireAllocation",[request_id,amount],componentID)
       allocations=connection.execute("prodMgrProdAgent.acquireAllocation",[request_id,amount,componentID])
       return allocations
   except Exception,ex:
       raise ProdAgentException("acquireAllocation Service Error: "+str(ex))

def acquireJob(serverUrl,request_id,parameters,componentID="defaultComponent"):
   try:
       connection=Management.getConnection(serverUrl)
       Management.logServiceCall(serverUrl,"prodMgrProdAgent.acquireJob",[request_id,parameters],componentID)
       jobs=connection.execute("prodMgrProdAgent.acquireJob",[request_id,parameters,componentID])
       return jobs
   except Exception,ex:
       raise ProdAgentException("acquireJob Service Error: "+str(ex))

def releaseJob(serverUrl,jobspec,events_completed,componentID="defaultComponent"):
   try:
       connection=Management.getConnection(serverUrl)
       Management.logServiceCall(serverUrl,"prodMgrProdAgent.releaseJob",[str(jobspec),events_completed],componentID)
       finished=connection.execute("prodMgrProdAgent.releaseJob",[str(jobspec),events_completed,componentID])
       return finished
   except Exception,ex:
       raise ProdAgentException("releaseJob Service Error: "+str(ex))

def releaseAllocation(serverUrl,allocation_id,componentID="defaultComponent"):
   try:
       connection=Management.getConnection(serverUrl)
       Management.logServiceCall(serverUrl,"prodMgrProdAgent.releaseAllocation",[allocation_id],componentID)
       finished=connection.execute("prodMgrProdAgent.releaseAllocation",[allocation_id,componentID])
       return finished
   except Exception,ex:
       raise ProdAgentException("releaseJob Service Error: "+str(ex))


def commit(serverUrl=None,method_name=None,componentID=None):
   try:
       Management.commit(serverUrl,method_name,componentID)
   except Exception,ex:
       raise ProdAgentException("commit Error: "+str(ex))

def retrieve(serverUrl=None,method_name=None,componentID="defaultComponent"):
   try:
       if ((serverUrl!=None) and (method_name!=None)):
           connection=Management.getConnection(serverUrl)
           result=connection.execute("prodMgrAdmin.lastServiceCall",[service_map[method_name],componentID])
           return result
       else:
           tripple=Management.retrieve(serverUrl,method_name,componentID)
           connection=Management.getConnection(tripple[0])
           result=connection.execute("prodMgrAdmin.lastServiceCall",[tripple[1],tripple[2]])
           return result
   except Exception,ex:
       raise ProdAgentException("retrieve Service Connection Error: "+str(ex))
