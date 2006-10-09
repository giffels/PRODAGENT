import logging

from ProdAgentDB import Session

def size():
    sqlStr="""SELECT COUNT(*) FROM pm_request;"""
    Session.execute(sqlStr)
    rows=Session.fetchall()
    result=int(rows[0][0])
    return result

def insert(request_id,priority,prodMgr):
    sqlStr="""INSERT INTO pm_request(id,url,priority) 
        VALUES("%s","%s","%s"); """ %(request_id,prodMgr,str(priority))
    Session.execute(sqlStr)

def getUrl(request_id):
    sqlStr="""SELECT url FROM pm_request WHERE id="%s"
        """ %(str(request_id))
    Session.execute(sqlStr)
    rows=Session.fetchall()
    return rows[0][0]

def getHighestPriority(index=0):
    sqlStr='SELECT id,url,priority FROM pm_request ORDER by priority;'
    Session.execute(sqlStr)
    rows=Session.fetchall()
    if index>(len(rows)-1):
        return {}
    return {'id':rows[index][0],'url':rows[index][1],'priority':rows[index][2]}

def rm(request_id):
    sqlStr="""DELETE FROM pm_request where id="%s" """ %(request_id)
    Session.execute(sqlStr)
