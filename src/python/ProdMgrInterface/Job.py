import logging

from ProdAgentDB import Session

def size(catagory):
    sqlStr="""SELECT COUNT(*) FROM pm_job WHERE catagory="%s"
           """ %(str(catagory))
    Session.execute(sqlStr)
    rows=Session.fetchall()
    result=int(rows[0][0])
    return result

def get(catagory):
    sqlStr="""SELECT id,url FROM pm_job WHERE
        catagory="%s" """ %(str(catagory))
    Session.execute(sqlStr)
    rows=Session.fetchall()
    result=[]
    for row in rows:
        result.append({'jobSpecId':row[0],'URL':row[1]})
    return result

def getRequest(catagory):
    sqlStr="""SELECT request_id FROM pm_job WHERE catagory="%s" limit 1;
        """ %(str(catagory))
    Session.execute(sqlStr)
    rows=Session.fetchall()
    return rows[0][0]

def rm(catagory,request_id=None):
    if request_id!=None:
        sqlStr="""DELETE FROM pm_job WHERE catagory="%s"
            AND request_id="%s" """ %(catagory,request_id)
    else:
        sqlStr="""DELETE FROM pm_job WHERE catagory="%s"
            """ %(catagory)
    Session.execute(sqlStr)

def mv(source_cat,target_cat,request_id):
    sqlStr="""UPDATE pm_job SET catagory="%s" WHERE
        catagory="%s" AND request_id="%s";
        """ %(target_cat,source_cat,request_id)
    Session.execute(sqlStr)


def insert(catagory,jobs,request_id):
    if len(jobs)>0:
        sqlStr="INSERT INTO pm_job(id,request_id,catagory,url) VALUES"
        comma=0
        for job in jobs:
            if comma==1:
                sqlStr+=','
            else:
                comma=1
            sqlStr+='("'+str(job['jobSpecId'])+'","'+request_id+'","'+catagory+'","'+job['URL']+'")'
        sqlStr+=';'
        Session.execute(sqlStr)

def isDownloaded(catagory,job_id):
    sqlStr="""SELECT downloaded FROM pm_job WHERE catagory="%s" 
        AND id="%s" """ %(catagory,job_id)
    Session.execute(sqlStr)
    rows=Session.fetchall()
    if rows[0][0]==0:
        return False
    return True

def downloaded(catagory,job_id):
    sqlStr="""UPDATE pm_job SET downloaded="%s" WHERE catagory="%s"
        AND id="%s"
        """ %(str(1),str(catagory),str(job_id))
    Session.execute(sqlStr)

