import base64
import cPickle
import logging

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdAgentDB.Connect import connect
import ProdMgrInterface.Clarens

# one time loading of a configuration
config = loadProdAgentConfiguration()
compCfg = config.getConfig("ProdAgent")

# certificate used for connecting to a server.
prodAgentCert=compCfg['ProdAgentCert']
prodAgentKey=compCfg['ProdAgentKey']

# connections to servers
connections={}

lastCall=''

def getConnection(serverUrl):
   global prodAgentCert
   global prodAgentKey

   try:
       if not connections.has_key(serverUrl):
           dbsvr=ProdMgrInterface.Clarens.client(serverUrl, certfile=str(prodAgentCert),\
               keyfile=str(prodAgentKey),debug=0)
           connections[serverUrl]=dbsvr
       return connections[serverUrl]       
   except Exception,ex:
       raise ProdAgentException("Service Connection Error: "+str(ex))

def logServiceCall(serverUrl,method_name,args,componentID="defaultComponent"):
   global lastCall

   try:
       conn=connect(False)
       dbCur=conn.cursor()
       dbCur.execute("START TRANSACTION")
       sqlStr="""INSERT INTO ws_last_call(server_url,component_id,service_call,service_parameters,call_state)
           VALUES("%s","%s","%s","%s","%s") ON DUPLICATE KEY UPDATE
           service_parameters="%s", call_state="%s";
           """ %(serverUrl,componentID,method_name,base64.encodestring(cPickle.dumps(args)),"call_placed",base64.encodestring(cPickle.dumps(args)),"call_placed")
       dbCur.execute(sqlStr)
       dbCur.execute("COMMIT")
       lastCall=(serverUrl,method_name,componentID)
       dbCur.close()
       conn.close()
   except Exception,ex:
       dbCur.execute("ROLLBACK")
       dbCur.close()
       conn.close()
       raise ProdAgentException("Service logging Error: "+str(ex))

def commit(serverUrl=None,method_name=None,componentID=None):
   global lastCall

   try:
       conn=connect(False)
       dbCur=conn.cursor()
       dbCur.execute("START TRANSACTION")
       if (serverUrl==None) or (method_name==None) or (componentID==None):
           serverUrl,method_name,componentID=lastCall
       sqlStr="""UPDATE ws_last_call SET call_state="result_retrieved" WHERE
           server_url="%s" AND component_id="%s" AND service_call="%s";
           """ %(serverUrl,componentID,method_name)
       dbCur.execute(sqlStr)
       dbCur.execute("COMMIT")
       dbCur.close()
       conn.close()
   except Exception,ex:
       dbCur.execute("ROLLBACK")
       dbCur.close()
       conn.close()
       raise ProdAgentException("Service commit Error: "+str(ex))
