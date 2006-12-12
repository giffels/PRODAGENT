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
    sqlStr="""SELECT id,job_spec_url FROM pm_job WHERE
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

def getUrl(job_id):
    sqlStr="""SELECT server_url FROM pm_job WHERE id="%s";
        """ %(str(job_id))
    Session.execute(sqlStr)
    rows=Session.fetchall()
    return rows[0][0]

def getLocation(job_id):
    sqlStr="""SELECT job_spec_location FROM pm_job WHERE id="%s";
        """ %(str(job_id))
    Session.execute(sqlStr)
    rows=Session.fetchall()
    return rows[0][0]

def rm(job_id):
    sqlStr="""DELETE FROM pm_job WHERE id="%s"
        """ %(job_id)
    Session.execute(sqlStr)

def mv(source_cat,target_cat):
    sqlStr="""UPDATE pm_job SET catagory="%s" WHERE
        catagory="%s";
        """ %(target_cat,source_cat)
    Session.execute(sqlStr)

def registerJobSpecLocation(job_id,job_spec_location):
    sqlStr="""UPDATE pm_job SET job_spec_location="%s"
       WHERE id="%s" """ %(str(job_spec_location),str(job_id))
    Session.execute(sqlStr)

def insert(catagory,jobs,request_id,server_url):
    if len(jobs)>0:
        sqlStr="INSERT INTO pm_job(id,request_id,catagory,job_spec_url,server_url) VALUES"
        comma=0
        for job in jobs:
            if comma==1:
                sqlStr+=','
            else:
                comma=1
            sqlStr+='("'+str(job['jobSpecId'])+'","'+request_id+'","'+catagory+'","'+job['URL']+'","'+server_url+'")'
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

def jobCutsFinished(job_id):
    sqlStr="""SELECT COUNT(*) FROM pm_job_cut WHERE job_id="%s"
        AND status="running" """ %(job_id)
    Session.execute(sqlStr)
    rows=Session.fetchall()
    if int(rows[0][0])==0:
        return True
    return False

def id(job_cut_id):
    cuts=job_cut_id.split('_')
    jobId=''
    for i in xrange(0,len(cuts)-1):
        jobId+=cuts[i]
        if i!=(len(cuts)-2):
            jobId+='_'
    return jobId 

