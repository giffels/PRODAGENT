import logging

import ProdMgr.Clarens
from ProdAgentCore.Configuration import loadProdAgentConfiguration

config = loadProdAgentConfiguration()
compCfg = config.getConfig("ProdAgent")

prodAgentCert=compCfg['ProdAgentCert']
prodAgentKey=compCfg['ProdAgentKey']

connections={}

def getConnection(serverUrl):
   global prodAgentCert
   global prodAgentKey

   try:
       if not connections.has_key(serverUrl):
           dbsvr=ProdMgr.Clarens.client(serverUrl, certfile=str(prodAgentCert),\
               keyfile=str(prodAgentKey),debug=0)
           connections[serverUrl]=dbsvr
       return connections[serverUrl]       
   except Exception,ex:
       logging.debug("Service Connection Error:")
       logging.debug(ex.faultCode)
       logging.debug(ex.faultString)

def userID(serverUrl):
   try:
      connection=getConnection(serverUrl)
      result=connection.execute("prodMgrRequest.userID",[])
      return result
   except Exception,ex:
       logging.debug("userID Service Call Error:")
       logging.debug(ex.faultCode)
       logging.debug(ex.faultString)
   
def acquireAllocation(serverUrl,request_id,amount):
   try:
       connection=getConnection(serverUrl)
       allocations=connection.execute("prodMgrProdAgent.acquireAllocation",[request_id,15])
       return allocations
   except Exception,ex:
       logging.debug("acquireAllocation Service Connection Error:")
       logging.debug(ex.faultCode)
       logging.debug(ex.faultString)

def acquireJob(serverUrl,request_id,parameters):
   try:
       connection=getConnection(serverUrl)
       jobs=connection.execute("prodMgrProdAgent.acquireJob",[request_id,parameters])
       return jobs
   except Exception,ex:
       logging.debug("acquireJob Service Connection Error:")
       logging.debug(ex.faultCode)
       logging.debug(ex.faultString)

def releaseJob(serverUrl,jobspec,events_completed):
   try:
       connection=getConnection(serverUrl)
       finished=connection.execute("prodMgrProdAgent.releaseJob",[str(jobspec),events_completed])
       return finished
   except Exception,ex:
       logging.debug("releaseJob Service Connection Error:")
       logging.debug(ex.faultCode)
       logging.debug(ex.faultString)



