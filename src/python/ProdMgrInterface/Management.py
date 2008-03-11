import base64
import cPickle
import logging
import os
import time
import urllib

from ProdAgentCore.Configuration import loadProdAgentConfiguration
from ProdAgentCore.ProdAgentException import ProdAgentException
from ProdCommon.Database import Session
import ProdMgrInterface.Clarens

# one time loading of a configuration
config = loadProdAgentConfiguration()
compCfg = config.getConfig("ProdAgent")

# certificate used for connecting to a server.
prodAgentCert=compCfg['ProdAgentCert']
prodAgentKey=compCfg['ProdAgentKey']

# connections to servers
connections={}

# last call cache
lastCall=''
# number of retries when connecting or executing
# NOTE: should eventually be put in prodagent config file.
retries=4
# time out between retries
# NOTE: should eventually be put in prodagent config file.
timeout=4

def executeCall(serverUrl,method_name,parameters=[],componentID="defaultComponent"):
   global prodAgentCert
   global prodAgentKey
   global retries
   global timeout

   attempt=0
   while attempt<retries: 
       try:
           logging.debug("Setting up connection for "+serverUrl)
           connection=ProdMgrInterface.Clarens.client(serverUrl, certfile=str(prodAgentCert),\
               keyfile=str(prodAgentKey),debug=0)
           logging.debug("Created connection for "+serverUrl)
           break
       except Exception,ex:
           logging.debug("Clarens Server Connection Attempt "+str(attempt)+\
               "/"+str(retries)+" Failed: "+str(ex))
           attempt=attempt+1
           time.sleep(timeout)
   if attempt==retries:       
       raise ProdAgentException("Could not establish connection with Clarens server "+\
           serverUrl+" Please check log files for more error messages ")

   if method_name!='prodCommonRecover.lastServiceCall':
       tag=str(time.time())
       logCall(serverUrl,method_name,parameters,componentID,tag)
       parameters.append(componentID)
       parameters.append(tag)

   logging.debug("Making call: "+str(method_name)+\
       " with parameters:"+str(parameters))
   result=connection.execute(method_name,parameters)
   logging.debug("Retrieved result: "+str(result))
   return result


def executeRestCall(serverUrl,rest_part,componentID="defaultComponent"):
   global prodAgentCert
   global prodAgentKey
   global retries
   global timeout

   
   attempt=0
   while attempt<retries: 
       try:
           logging.debug("Setting up connection for "+serverUrl)
           connection=ProdMgrInterface.Clarens.client(serverUrl, certfile=str(prodAgentCert),\
               keyfile=str(prodAgentKey),debug=0)
           logging.debug("Created connection for "+serverUrl)
           break
       except Exception,ex:
           logging.debug("Clarens Server Connection Attempt "+str(attempt)+\
               "/"+str(retries)+" Failed: "+str(ex))
           attempt=attempt+1
           time.sleep(timeout)
   if attempt==retries:       
       raise ProdAgentException("Could not establish connection with Clarens server "+\
           serverUrl+" Please check log files for more error messages ")

   method_name=rest_part.split('?')[0]
   if method_name.find('prodCommonRecover/lastServiceCall')>-1:
       tag=str(time.time())
       logCall(serverUrl,method_name,[],componentID,tag)

   #NOTE: rest calls are currently not properly logged!
   connection.disable_deserialize() 
   result=connection.execute_rest(rest_part)
   #logging.debug("Retrieved result: "+str(result))
   connection.enable_deserialize() 
   return result


def logCall(serverUrl,method_name,args,componentID="defaultComponent",tag="0"):
   global lastCall

   last_session=Session.current_session
   try:
       Session.connect('ProdMgr')
       Session.start_transaction('ProdMgr')
       sqlStr="""INSERT INTO ws_last_call(server_url,component_id,service_call,service_parameters,call_state,tag)
           VALUES("%s","%s","%s","%s","%s","%s") ON DUPLICATE KEY UPDATE
           service_parameters="%s", call_state="%s", tag="%s";
           """ %(serverUrl,componentID,method_name,base64.encodestring(cPickle.dumps(args)),"call_placed",str(tag),base64.encodestring(cPickle.dumps(args)),"call_placed",tag)
       Session.execute(sqlStr)
       Session.commit()
       lastCall=(serverUrl,method_name,componentID)
       Session.close()
       Session.set_session(last_session)
   except Exception,ex:
       Session.rollback()
       Session.close()
       Session.set_session(last_session)
       raise ProdAgentException("Service logging Error: "+str(ex))

def commit(serverUrl=None,method_name=None,componentID=None):
   global lastCall

   # all other methods that access the database can do that
   # with their own private sessions and hence connect imediately
   # to the database. Commit is different since if it fails all
   # updates in the queues must fail (rollback).
   last_session=Session.current_session
   try:
       Session.connect('ProdMgr')
       Session.start_transaction('ProdMgr')
       if (serverUrl==None) or (method_name==None) or (componentID==None):
           serverUrl,method_name,componentID=lastCall
       sqlStr="""UPDATE ws_last_call SET call_state="result_retrieved" WHERE
           server_url="%s" AND component_id="%s" AND service_call="%s";
           """ %(serverUrl,componentID,method_name)
       Session.execute(sqlStr)
       Session.commit()
       Session.close()
       Session.set_session(last_session)
   except Exception,ex:
       Session.rollback()
       Session.close()
       Session.set_session(last_session)
       raise ProdAgentException("Service commit Error: "+str(ex))

def retrieve(serverURL=None,method_name=None,componentID=None):

   last_session=Session.current_session
   try:
       Session.connect('ProdMgr')
       Session.start_transaction('ProdMgr')
       #NOTE: we do several nested queries and assume that the query engine can rewrite them
       #NOTE: we should rewrite these queries ourselves.
       if serverURL==None and method_name==None and componentID==None:
           sqlStr="""SELECT server_url,service_call,component_id,tag FROM ws_last_call WHERE 
               call_state="call_placed" AND 
               id in (
               SELECT max(id)
               FROM ws_last_call
               WHERE call_state="call_placed" 
               AND
               log_time IN ( 
               SELECT  max(log_time) FROM ws_last_call
               WHERE call_state="call_placed" GROUP BY server_url) GROUP BY server_url);
               """ 
       elif serverURL==None and method_name==None and componentID!=None:
           sqlStr="""SELECT server_url,service_call,component_id,tag FROM ws_last_call WHERE  
               component_id="%s" AND call_state="call_placed" AND
               id in (
               SELECT max(id)
               FROM ws_last_call
               WHERE component_id="%s" AND call_state="call_placed" 
               AND log_time IN ( 
               SELECT max(log_time) FROM ws_last_call
               WHERE component_id="%s" AND call_state="call_placed" GROUP BY server_url) GROUP BY server_url);
               """ %(componentID,componentID,componentID)
       elif serverURL==None and method_name!=None and componentID!=None:
           sqlStr="""SELECT server_url,service_call,component_id,tag FROM ws_last_call WHERE 
               component_id="%s" AND service_call="%s" AND call_state="call_placed" AND
               id in (
               SELECT max(id)
               FROM ws_last_call
               WHERE component_id="%s" AND service_call="%s" AND call_state="call_placed" 
               AND log_time IN ( 
               SELECT  max(log_time) FROM ws_last_call
               WHERE component_id="%s" AND service_call="%s" AND call_state="call_placed" GROUP BY server_url) GROUP BY server_url;
               """ %(componentID,method_name,componentID,method_name,componentID,method_name)
       elif serverURL!=None and method_name==None and componentID!=None:
           sqlStr="""SELECT server_url,service_call,component_id,tag FROM ws_last_call WHERE 
               component_id="%s" AND server_url="%s" AND call_state="call_placed" AND
               id in (
               SELECT max(id)
               FROM ws_last_call
               WHERE component_id="%s" AND server_url="%s" AND call_state="call_placed" 
               AND log_time IN ( 
               SELECT  max(log_time) FROM ws_last_call
               WHERE component_id="%s" AND server_url="%s" AND call_state="call_placed" GROUP BY server_url) GROUP BY server_url);
               """ %(componentID,serverURL,componentID,serverURL,componentID,serverURL)
       elif serverURL!=None and method_name!=None and componentID!=None:
           sqlStr="""SELECT server_url,service_call,component_id,tag FROM ws_last_call WHERE 
               component_id="%s" AND server_url="%s" AND call_state="call_placed" AND service_call="%s"
               """ %(componentID,serverURL,method_name)
       dbCur.execute(sqlStr)
       rows=dbCur.fetchall()
       if len(rows)==0:
           raise ProdAgentException("No result in local last service call table with componentID :"+\
               str(componentID),3000)
       server_url=rows[0][0]
       service_call=rows[0][1]
       component_id=rows[0][2]
       tag=rows[0][3]
       Session.execute(sqlStr)
       Session.commit()
       Session.close()
       Session.set_session(last_session)
       return [server_url,service_call,component_id,tag]
   except ProdAgentException:
       Session.rollback()
       Session.close()
       Session.set_session(last_session)
       raise
   except Exception,ex:
       Session.rollback()
       Session.close()
       Session.set_session(last_session)
       raise ProdAgentException("Service commit Error: "+str(ex),3001)

def retrieveFile(url,local_destination,componentID="defaultComponent"):
   global prodAgentCert
   global prodAgentKey
   global retries
   
   attempt=0
   # make the directory if not exists
   while attempt<retries: 
       try:
           credentials={'key_file':prodAgentKey,'cert_file':prodAgentCert}
           retriever=urllib.URLopener(proxies=None,**credentials)
           retriever.retrieve(url,local_destination)
           break
       except Exception,ex:
           print("File download attempt "+str(attempt)+\
               "/"+str(retries)+" Failed: "+str(ex))
           attempt=attempt+1
           time.sleep(timeout)
   if attempt==retries:       
       raise ProdAgentException("Could not download file "+url+\
           " Please check log files for more error messages ")
   tag=str(time.time())
   logCall(url,'retrieveFile',[],componentID,tag)

