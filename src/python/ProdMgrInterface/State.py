import cPickle
import logging

from ProdAgentDB import Session

def get(id):
   sqlStr='SELECT id,state,parameters FROM pm_state WHERE '+\
          ' id="'+str(id)+'";'
   Session.execute(sqlStr)
   rows=Session.fetchall()
   if len(rows)==0:
       return {}
   result={}
   result['id']=rows[0][0]
   result['state']=rows[0][1]
   result['parameters']=cPickle.loads(rows[0][2])
   return result

def insert(id,state,parameters):
   sqlStr="""INSERT INTO pm_state(id,state,parameters)
       VALUES("%s","%s","%s") """ %(id,state,cPickle.dumps(parameters))
   Session.execute(sqlStr)


def setParameters(id,parameters={}):
    sqlStr='UPDATE pm_state SET parameters="'+str(cPickle.dumps(parameters))+\
        '" WHERE id="'+str(id)+'" ;'
    Session.execute(sqlStr)

def setState(id,state):
    sqlStr='UPDATE pm_state SET state="'+state+'";'
    Session.execute(sqlStr)

