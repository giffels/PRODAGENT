from ProdAgentDB import Session

def size(catagory):
    sqlStr="""SELECT COUNT(*) FROM pm_allocation WHERE catagory="%s"
           """ %(str(catagory))
    Session.execute(sqlStr)
    rows=Session.fetchall()
    result=int(rows[0][0])
    return result

def get(catagory,state,request_id):
    sqlStr="""SELECT id FROM pm_allocation WHERE state="%s" AND
        request_id="%s" AND catagory="%s" """ %(str(state),str(request_id),str(catagory))
    Session.execute(sqlStr)
    rows=Session.fetchall()
    result=[]
    for row in rows:
        result.append(row[0])
    return result

def getRequest(catagory):
    sqlStr="""SELECT request_id FROM pm_allocation WHERE catagory="%s" limit 1;
        """ %(str(catagory))
    Session.execute(sqlStr)
    rows=Session.fetchall()
    return rows[0][0]

def setState(catagory,allocation_id,state):
    sqlStr="""UPDATE pm_allocation SET state="%s" WHERE
        catagory="%s" AND id="%s" """ %(state,catagory,allocation_id)
    Session.execute(sqlStr)

def rm(catagory,request_id=None,allocation_id=None):
    if allocation_id!=None:
        sqlStr="""DELETE FROM pm_allocation WHERE 
            AND id="%s" """ %(str(allocation_id))
    elif request_id!=None:
        sqlStr="""DELETE FROM pm_allocation WHERE catagory="%s"
            AND request_id="%s" """ %(catagory,request_id)
    else:
        sqlStr="""DELETE FROM pm_allocation WHERE catagory="%s"
            """ %(catagory)
    Session.execute(sqlStr)

def mv(source_cat,target_cat,request_id):
    sqlStr="""UPDATE pm_allocation SET catagory="%s" WHERE
        catagory="%s" AND request_id="%s";
        """ %(target_cat,source_cat,request_id)
    Session.execute(sqlStr)

def insert(catagory,allocations,request_id):
    if len(allocations)>0:
        sqlStr="INSERT INTO pm_allocation(id,request_id,catagory,state) VALUES"
        comma=0
        for allocation in allocations:
            if comma==1:
                sqlStr+=','
            else:
                comma=1
            sqlStr+='("'+str(allocation)+'","'+request_id+'","'+catagory+'","idle")'
        sqlStr+=';'
        Session.execute(sqlStr)

