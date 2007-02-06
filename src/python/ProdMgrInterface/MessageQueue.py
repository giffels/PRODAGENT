
import cPickle
import logging
from ProdCommon.Database import Session

def hasURL(url):
    sqlStr="""SELECT COUNT(*) FROM ws_queue WHERE server_url="%s";
        """ %(url)
    Session.execute(sqlStr)
    rows=Session.fetchall()
    if rows[0][0]==0:
        return False
    return True

def insert(component_id,handler_id,server_url,state,parameters,delay="00:00:00"):
    sqlStr="""INSERT INTO ws_queue(component_id,handler_id,server_url,state,parameters,delay) 
        VALUES("%s","%s","%s","%s","%s","%s"); """ %(component_id,handler_id,server_url,state,str(cPickle.dumps(parameters)),delay)
    Session.execute(sqlStr)

def retrieve(component_id,handler_id,start=None,amount=None):
   if start==None:
       sqlStr="""SELECT server_url,state,parameters,id FROM ws_queue WHERE
           component_id="%s" AND handler_id="%s" AND
           ADDTIME(log_time,delay)<= CURRENT_TIMESTAMP """ %(component_id,handler_id)
   else:
       sqlStr="""SELECT server_url,state,parameters,id FROM ws_queue WHERE
           component_id="%s" AND handler_id="%s" AND
           ADDTIME(log_time,delay)<= CURRENT_TIMESTAMP LIMIT %s,%s; """ %(component_id,handler_id,str(start),str(amount))
   Session.execute(sqlStr)
   rows=Session.fetchall()
   if len(rows)==0:
      return []
   else:
      result=[]
      for row in rows:
          message={}
          message['server_url']=row[0]
          message['state']=row[1]
          message['parameters']=cPickle.loads(row[2])
          message['id']=row[3]
          result.append(message)
      return result

def reinsert(id,delay="00:00:00"):
   sqlStr="""UPDATE ws_queue SET delay="%s" WHERE id="%s";
       """ %(str(delay),str(id)) 
   Session.execute(sqlStr)

def remove(id):
   sqlStr="""DELETE FROM ws_queue WHERE id="%s"
       """ %(str(id))
   Session.execute(sqlStr)
