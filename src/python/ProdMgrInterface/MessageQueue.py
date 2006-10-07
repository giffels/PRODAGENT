import cPickle
from ProdAgentDB import Session

def hasURL(url):
    sqlStr="""SELECT COUNT(*) FROM ws_queue WHERE server_url="%s";
        """ %(url)
    Session.execute(sqlStr)
    rows=Session.fetchall()
    if rows[0][0]==0:
        return False
    return True

def insert(component_id,handler_id,server_url,state,parameters):
    sqlStr="""INSERT INTO ws_queue(component_id,handler_id,server_url,state,parameters) 
        VALUES("%s","%s","%s","%s","%s"); """ %(component_id,handler_id,server_url,state,str(cPickle.dumps(parameters)))
    Session.execute(sqlStr)
