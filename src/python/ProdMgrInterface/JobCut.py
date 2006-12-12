import logging

from ProdAgentDB import Session


def rm(job_id):
    sqlStr="""DELETE FROM pm_job_cut WHERE job_id="%s"
        """ %(job_id)
    Session.execute(sqlStr)

def insert(job_cuts,job):
    if len(job_cuts)>0:
        sqlStr="INSERT INTO pm_job_cut(id,job_id,job_cut_spec_location) VALUES"
        comma=0
        for job_cut in job_cuts:
            if comma==1:
                sqlStr+=','
            else:
                comma=1
            sqlStr+='("'+str(job_cut['id'])+'","'+job+'","'+job_cut['spec']+'")'
        sqlStr+=';'
        Session.execute(sqlStr)

def events(job_id):
    sqlStr="""SELECT SUM(events_processed) FROM pm_job_cut WHERE
        job_id="%s" """ %(job_id)
    Session.execute(sqlStr)
    rows=Session.fetchall()
    return int(rows[0][0])

def eventsProcessed(job_cut_id,events_processed):
    sqlStr=""" UPDATE pm_job_cut SET events_processed="%s",
        status="finished" WHERE id="%s" 
        """ %(str(events_processed),str(job_cut_id))
    Session.execute(sqlStr)

def getLocation(job_cut_id):
    sqlStr="""SELECT job_cut_spec_location FROM pm_job_cut WHERE id="%s";
        """ %(str(job_cut_id))
    Session.execute(sqlStr)
    rows=Session.fetchall()
    return rows[0][0]


