import logging

from ProdAgentDB import Session

def size():
    sqlStr="""SELECT COUNT(*) FROM pm_request;"""
    Session.execute(sqlStr)
    rows=Session.fetchall()
    result=int(rows[0][0])
    return result

def has(request_id):
    sqlStr="""SELECT COUNT(*) FROM pm_request
        WHERE id="%s" """ %(str(request_id))
    Session.execute(sqlStr)
    rows=Session.fetchall()
    result=int(rows[0][0])
    if result==1:
        return True
    return False 

def insert(request_id,priority,request_type,prodMgr):
    sqlStr="""INSERT INTO pm_request(id,url,request_type,priority) 
        VALUES("%s","%s","%s","%s") ON DUPLICATE KEY UPDATE priority="%s"; """ %(request_id,prodMgr,str(request_type),str(priority),str(priority))
    Session.execute(sqlStr)

def getUrl(request_id):
    sqlStr="""SELECT url FROM pm_request WHERE id="%s"
        """ %(str(request_id))
    Session.execute(sqlStr)
    rows=Session.fetchall()
    return rows[0][0]

def getWorkflowLocation(request_id):
    sqlStr="""SELECT retrieved_workflow FROM pm_request WHERE id="%s"
        """ %(str(request_id))
    Session.execute(sqlStr)
    rows=Session.fetchall()
    return rows[0][0]

def getHighestPriority(index=0):
    sqlStr='SELECT id,url,priority,request_type FROM pm_request ORDER by priority;'
    Session.execute(sqlStr)
    rows=Session.fetchall()
    if index>(len(rows)-1):
        return {}
    return {'id':rows[index][0],'url':rows[index][1],'priority':rows[index][2],'type':rows[index][3]}

def rm(request_id):
    sqlStr="""DELETE FROM pm_request where id="%s" """ %(request_id)
    Session.execute(sqlStr)

def withoutWorkflow():
    sqlStr="""SELECT id,url,request_type FROM pm_request where retrieved_workflow='false'
         """
    Session.execute(sqlStr)
    rows=Session.fetchall()
    return rows

def workflowSet(id,workflow_location):
    sqlStr="""UPDATE pm_request SET  retrieved_workflow='%s' 
        WHERE id='%s' """ %(str(workflow_location),str(id))
    Session.execute(sqlStr)
    Session.commit()
