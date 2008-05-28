from TaskTracking.TaskStateAPI import *
import time, os, datetime,commands
import logging

def composeDestinationCondition(destination = 'all'):
    if destination == 'all' or destination == ():
       dest_condition=""
    else:
       dest_condition="and ("
       for sub in destination:
          dest_condition+="destination like '%"+sub+"%' or "
       dest_condition+=" false)"
    return dest_condition

def getQueues(destination = 'all'):
    dest_condition = composeDestinationCondition(destination);
    queryString = "select distinct(destination) from bl_runningjob where 1 "+dest_condition
    queues = queryMethod(queryString, None)
    return queues

# # #
#  js_taskInstance
#


def getJs__tasInstance():
    queryString = "SELECT status, notificationSent FROM js_taskInstance;"
    taskCheck = queryMethod(queryString, None)
    return taskCheck


def getNumTaskFinished():

    queryString = "SELECT * FROM js_taskInstance  WHERE notificationSent > 1 and status <> \'killed\' and status <> \'not submitted\';"
    taskCheck = queryMethod(queryString, None)
    return len(taskCheck)


def getNumTaskSubmitted():

    queryString = "SELECT * FROM js_taskInstance  WHERE status = \'submitted\';"
    taskCheck = queryMethod(queryString, None)
    return len(taskCheck)


def getNumTaskNotFinished():

   queryString = "SELECT * FROM js_taskInstance  WHERE notificationSent < 2;"
   taskCheck = queryMethod(queryString, None)
   return len(taskCheck)

def getNumTaskKilled():

   queryString = "SELECT * FROM js_taskInstance  WHERE status = \'killed\';"
   taskCheck = queryMethod(queryString, None)
   return len(taskCheck)


def getNumTaskNotSubmitted():

   queryString = "SELECT * FROM js_taskInstance  WHERE status = \'not submitted\';"
   taskCheck = queryMethod(queryString, None)
   return len(taskCheck)

def getNumTask(statusName,ended = True):
    if ended == True:
        queryString = "SELECT * FROM js_taskInstance  WHERE status =\'"+statusName+ "\' and notficationSent =2;"
    else:
        queryString = "SELECT * FROM js_taskInstance  WHERE status =\'"+statusName+ "\' and notificationSent <2;"
    taskCheck = queryMethod(queryString, None)
    return len(taskCheck)

# # #
#  ms_message
#
def getNumJobResubmitting():

    queryString = "select count(*) from ms_message where ms_message.dest = (select procid  from ms_process where name = \'JobSubmitter\');"
    taskCheck = queryMethod(queryString, None)
    num_rows=taskCheck[0]
    return num_rows[0]


# # #
#  bl_runningjob
#
def getInfoRunningJobError():
    queryString = "select  wrapper_return_code, application_return_code, from bl_runningjob ;"
    taskCheck = queryMethod(queryString, None)
    return taskCheck

def getInfoRunningJobStatus():
    queryString = "SELECT status from bl_runningjob;"
    taskCheck = queryMethod(queryString, None)
    return taskCheck



def getNum_Wrap_Error():

    queryString = "select count(*) from bl_runningjob where wrapper_return_code is not null or wrapper_return_code <> 0;"
    taskCheck = queryMethod(queryString, None)
    num_rows=taskCheck[0]
    return num_rows[0]


def getNum_Application_Error():

    queryString = "select count(*) from bl_runningjob where application_return_code is not null or application_return_code <> 0;"
    taskCheck = queryMethod(queryString, None)
    num_rows=taskCheck[0]
    return num_rows[0]


def getList_Application_Error(destination='all'):
    if destination == 'all':
        dest_condition=""
    else:
        dest_condition=" and destination like '%"+destination+"%'"
    queryString = "select application_return_code, count(*) from bl_runningjob where application_return_code <> 0 or application_return_code is not null "+dest_condition+" group by  application_return_code"
    taskCheck = queryMethod(queryString, None)
    return taskCheck


##AF - add possibility to specify destination 
def getList_Wapper_Error(destination='all'):
    if destination == 'all':
        dest_condition=""
    else:
       dest_condition=" and destination like '%"+destination+"%'"

    queryString = "select wrapper_return_code, count(*) from bl_runningjob where wrapper_return_code <> 0 or application_return_code is not null "+dest_condition+" group by wrapper_return_code"
    taskCheck = queryMethod(queryString, None)
    return taskCheck


def getNumJobCleared():

    queryString = "SELECT count(*) from bl_runningjob where status = 'E';"
    taskCheck = queryMethod(queryString, None)
    num_rows=taskCheck[0]
    return num_rows[0]


def getNumJobNotCleared():

    queryString = "SELECT count(*) from bl_runningjob where status = 'SD';"
    taskCheck = queryMethod(queryString, None)
    num_rows=taskCheck[0]
    return num_rows[0]

# # # VM
def getBossLiteRunningJobs(key,destination='all'):
    dest_condition = composeDestinationCondition(destination);
    queryString = "select count("+key+"),"+key+" from bl_runningjob where 1 "+dest_condition+" group by "+key+" "
    taskCheck = queryMethod(queryString, None)
    logging.info("====getBLRJ======> %s"%queryString)
    return taskCheck

# # # VM
def getNumBossLiteRunningJobs(key,date,destination='all'):
    dest_condition = composeDestinationCondition(destination);
    strDate = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(date)))
    queryString = "select count(*),"+key+" from bl_runningjob where submission_time < '"+strDate+"' "+dest_condition+" group by "+key+" " 
    taskCheck = queryMethod(queryString, None)
    return taskCheck

# # # VM
def getNumLastBossLiteRunningJobs(key,past=0,destination='all'):
    dest_condition = composeDestinationCondition(destination);
    tnow=time.time(); #-time.altzone
    if past>0:
        date=tnow-past
        strDate = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(date)))
        DateCondition = "where submission_time > '"+strDate+"' "
    else:
        DateCondition = "where 1 "
    queryString = "select count(*),"+key+" from bl_runningjob "+DateCondition+" "+dest_condition+" group by "+key+" " 
    taskCheck = queryMethod(queryString, None)
    logging.info("=============> %s"%queryString)
    return taskCheck

# # # VM
def getLastDeltaTimeBossLiteRunningJobs(from_time,to_time, past=0,destination = 'all'):
    dest_condition = composeDestinationCondition(destination);
    logging.info("=====LastDeltaTime========> %s"%dest_condition)
    tnow=time.time(); #-time.altzone
    if past>0:
       date=tnow-past
       strDate = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(date)))
       DateCondition = "where submission_time > '"+strDate+"' "
    else:
       DateCondition = "where 1 "
      
    limitsQueryString = "select max("+to_time+"-"+from_time+") from bl_runningjob "+DateCondition+" and "+to_time+"-"+from_time+">0 and "+to_time+"-"+from_time+"<31536000 "+dest_condition+";"
    max = queryMethod(limitsQueryString, None)
    if str(max[0][0]) != 'None':   #db gives NULL result
       max = float(max[0][0]);
    else:
       max = 3600.;

    limitsQueryString = "select count(*) from bl_runningjob "+DateCondition+" and "+to_time+"-"+from_time+">0 and "+to_time+"-"+from_time+"<31536000 "+dest_condition+";"
    Nbin = queryMethod(limitsQueryString, None)
    if Nbin[0][0]>100:
       Nbin = Nbin[0][0]/10
    else:
       Nbin = Nbin[0][0]
    if Nbin>400:
       Nbin = 400
    if Nbin<=0:
      Nbin = 60
   
    h = int(max / Nbin)+1

    queryString = "select count(*),floor(("+to_time+"-"+from_time+")/"+str(h)+") from bl_runningjob  "+DateCondition+" and "+to_time+"-"+from_time+">0 and "+to_time+"-"+from_time+"<31536000 "+dest_condition+" group by floor(("+to_time+"-"+from_time+")/"+str(h)+") "
    taskCheck = queryMethod(queryString, None)
   
    return max, Nbin, taskCheck

# # # VM
def getDeltaTimeBossLiteRunningJobs(from_time,to_time, destination = 'all'):
    dest_condition = composeDestinationCondition(destination);
      
    limitsQueryString = "select max("+to_time+"-"+from_time+") from bl_runningjob where "+to_time+"-"+from_time+">0 and "+to_time+"-"+from_time+"<31536000 "+dest_condition+";"
    max = queryMethod(limitsQueryString, None)
    if str(max[0][0]) != 'None':   #db gives NULL result
       max = float(max[0][0]);
    else:
       max = 3600.;

    limitsQueryString = "select count(*) from bl_runningjob where "+to_time+"-"+from_time+">0 and "+to_time+"-"+from_time+"<31536000 "+dest_condition+";"
    Nbin = queryMethod(limitsQueryString, None)
    if Nbin[0][0]>100:
       Nbin = Nbin[0][0]/10
    else:
       Nbin = Nbin[0][0]
    if Nbin>400:
       Nbin = 400
    if Nbin<=0:
      Nbin = 60
   
    h = int(max / Nbin)+1

    queryString = "select count(*),floor(("+to_time+"-"+from_time+")/"+str(h)+") from bl_runningjob where "+to_time+"-"+from_time+">0 and "+to_time+"-"+from_time+"<31536000 "+dest_condition+" group by floor(("+to_time+"-"+from_time+")/"+str(h)+") "
    taskCheck = queryMethod(queryString, None)
   
    return max, Nbin, taskCheck


# # # VM
def getSites(past=0,Sites='flat'):
    end_t = time.time(); #-time.altzone
    strEnd = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(end_t)))
    if past>0:
       begin_t = end_t-past
       strBegin = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(begin_t)))
       dateCondition = "where submission_time < '"+strEnd+"' and submission_time > '"+strBegin+"' "
    else:
       dateCondition = "where submission_time < '"+strEnd+"' "
    outputSites={};
    OverallCount = 0;
    if Sites=='flat':
       queryString = "select count(*),destination from bl_runningjob "+dateCondition+" group by destination;"
       taskCheck = queryMethod(queryString, None)
       for site in taskCheck:
#         outputsite = (str(site[1])).split(':')[0]
          outputsite = str(site[1])
          outputSites[outputsite] = site[0]
          OverallCount+=site[0]
    else:
       for site in Sites.keys():
          dest_condition = composeDestinationCondition(Sites[site]);
          queryString = "select count(*),destination from bl_runningjob "+dateCondition+" "+dest_condition+"group by destination;"
          taskCheck = queryMethod(queryString, None)
          tmpcount = 0;
          for queue in taskCheck:
             tmpcount += queue[0]
             OverallCount+=queue[0]
          outputSites[site] = tmpcount;
    return outputSites, OverallCount;

# # # VM
def getNum_WrapperErrors(destination = 'all',past=0): #begin_t=0,end_t=time.time()-time.altzone):
    dest_condition = composeDestinationCondition(destination);
    end_t = time.time(); #-time.altzone
    strEnd = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(end_t)))
    if past>0:
       begin_t = end_t-past
       strBegin = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(begin_t)))
       dateCondition = "submission_time < '"+strEnd+"' and submission_time > '"+strBegin+"' "
    else:
       dateCondition = "submission_time < '"+strEnd+"' "
    queryString = "select count(*) from bl_runningjob where (wrapper_return_code is not null or wrapper_return_code <> 0) "+dest_condition+" and "+dateCondition+" ;"
    taskCheck = queryMethod(queryString, None)
    num_rows=taskCheck[0]
    return num_rows[0]

# # # VM
def getNum_ApplicationErrors(destination = 'all', past=0): 
    dest_condition = composeDestinationCondition(destination);
    end_t = time.time(); #-time.altzone
    strEnd = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(end_t)))
    if past>0:
       begin_t = end_t-past
       strBegin = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(begin_t)))
       dateCondition = "submission_time < '"+strEnd+"' and submission_time > '"+strBegin+"' "
    else:
       dateCondition = "submission_time < '"+strEnd+"' "
    queryString = "select count(*) from bl_runningjob where (application_return_code is not null or application_return_code <> 0) "+dest_condition+" and "+dateCondition+" ;"
    taskCheck = queryMethod(queryString, None)
    num_rows=taskCheck[0]
    return num_rows[0]

# # # VM
def getList_ApplicationErrors(destination='all', past=0): 
    dest_condition = composeDestinationCondition(destination);
    end_t = time.time(); #-time.altzone
    strEnd = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(end_t)))
    if past>0:
       begin_t = end_t-past
       strBegin = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(begin_t)))
       dateCondition = "submission_time < '"+strEnd+"' and submission_time > '"+strBegin+"' "
    else:
       dateCondition = "submission_time < '"+strEnd+"' "
    queryString = "select application_return_code, count(*) from bl_runningjob where (application_return_code <> 0 or application_return_code is not null) "+dest_condition+" and "+dateCondition+" group by  application_return_code"
    taskCheck = queryMethod(queryString, None)
    return taskCheck

# # # VM
def getList_WrapperErrors(destination='all',past=0):
    dest_condition = composeDestinationCondition(destination);
    end_t = time.time(); #-time.altzone
    strEnd = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(end_t)))
    if past>0:
       begin_t = end_t-past
       strBegin = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(begin_t)))
       dateCondition = "submission_time < '"+strEnd+"' and submission_time > '"+strBegin+"' "
    else:
       dateCondition = "submission_time < '"+strEnd+"' "
    queryString = "select wrapper_return_code, count(*) from bl_runningjob where (wrapper_return_code <> 0 or application_return_code is not null) "+dest_condition+" and "+dateCondition+" group by wrapper_return_code"
    taskCheck = queryMethod(queryString, None)
    return taskCheck

# # # VM
def getNumJobs(destination='all',past=0):
    dest_condition = composeDestinationCondition(destination);
    end_t = time.time(); #-time.altzone
    strEnd = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(end_t)))
    if past>0:
       begin_t = end_t-past
       strBegin = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(begin_t)))
       dateCondition = "submission_time < '"+strEnd+"' and submission_time > '"+strBegin+"' "
    else:
       dateCondition = "submission_time < '"+strEnd+"' "
    queryString = "select count(*) from bl_runningjob where "+dateCondition+" "+dest_condition+" ;"
    taskCheck = queryMethod(queryString, None)
    logging.info("=============> %s"%queryString)
    return taskCheck

# # # VM
def getNumSuccessJob(destination='all',past=0):
    dest_condition = composeDestinationCondition(destination);
    end_t = time.time(); #-time.altzone
    strEnd = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(end_t)))
    if past>0:
       begin_t = end_t-past
       strBegin = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(begin_t)))
       dateCondition = "submission_time < '"+strEnd+"' and submission_time > '"+strBegin+"' "
    else:
       dateCondition = "submission_time < '"+strEnd+"' "
    queryString = "select count(*) from bl_runningjob where getoutput_time>0 and "+dateCondition+" and application_return_code=0 and wrapper_return_code=0 "+dest_condition
    taskCheck = queryMethod(queryString, None)
    logging.info("=============> %s"%queryString)
    return taskCheck

# # # VM
def getNumFailWrapperJob(destination='all',past=0):
    dest_condition = composeDestinationCondition(destination);
    end_t = time.time(); #-time.altzone
    strEnd = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(end_t)))
    if past>0:
       begin_t = end_t-past
       strBegin = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(begin_t)))
       dateCondition = "submission_time < '"+strEnd+"' and submission_time > '"+strBegin+"' "
    else:
       dateCondition = "submission_time < '"+strEnd+"' "
    queryString = "select count(*) from bl_runningjob where "+dateCondition+" and wrapper_return_code!=0 "+dest_condition
    taskCheck = queryMethod(queryString, None)
    logging.info("=============> %s - %s"%(queryString, str(past)))
    return taskCheck

    

# ########
#  AF adding methods useful for efficency plots
# #######
def getTimeNumBossLiteRunningJobs(key,begin_t=0,end_t=time.time()-time.altzone,destination='all'):
    if destination == 'all':
       dest_condition=""
    else:
       dest_condition=" and destination like '%"+destination+"%'"
    strBegin = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(begin_t)))
    strEnd = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(end_t)))
    queryString = "select count(*),"+key+" from bl_runningjob where submission_time < '"+strEnd+"' and submission_time > '"+strBegin+"' "+dest_condition+" group by "+key+" "
    logging.info("=============> %s"%queryString)
    taskCheck = queryMethod(queryString, None)
    return taskCheck

def getNumJobSubmitted(destination='all',begin_t=0,end_t=time.time()-time.altzone):
    if destination == 'all':
       dest_condition=""
    else:
       dest_condition=" and destination like '%"+destination+"%'" 
    strBegin = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(begin_t)))
    strEnd = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(end_t)))
    queryString = "select count(*) from bl_runningjob where submission_time > '"+strBegin+"' and submission_time < '"+strEnd+"' "+dest_condition
    logging.info("=============> %s"%queryString)
    taskCheck = queryMethod(queryString, None)
    return taskCheck

def getNumJobRetrieved(destination='all',begin_t=0,end_t=time.time()-time.altzone,timetype="submission_time"):
    if destination == 'all':
       dest_condition=""
    else:
       dest_condition=" and destination like '%"+destination+"%'"
    strBegin = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(begin_t)))
    strEnd = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(end_t)))
    queryString = "select count(*) from bl_runningjob where getoutput_time>0 and "+timetype+" > '"+strBegin+"' and "+timetype+" < '"+strEnd+"' "+dest_condition
    logging.info("=============> %s"%queryString)
    taskCheck = queryMethod(queryString, None)
    return taskCheck

def getNumJobSuccess(destination='all',begin_t=0,end_t=time.time()-time.altzone,timetype="submission_time"):
    if destination == 'all':
       dest_condition=""
    else:
       dest_condition=" and destination like '%"+destination+"%'"
    strBegin = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(begin_t)))
    strEnd = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(end_t)))
    queryString = "select count(*) from bl_runningjob where getoutput_time>0 and "+timetype+" > '"+strBegin+"' and "+timetype+" < '"+strEnd+"' and application_return_code=0 and wrapper_return_code=0 "+dest_condition
    logging.info("=============> %s"%queryString)
    taskCheck = queryMethod(queryString, None)
    return taskCheck



def getNumJobFailWrapper(destination='all',begin_t=0,end_t=time.time()-time.altzone):
    if destination == 'all':
       dest_condition=""
    else:
       dest_condition=" and destination like '%"+destination+"%'"
    strBegin = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(begin_t)))
    strEnd = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(end_t)))
    queryString = "select count(*) from bl_runningjob where getoutput_time > '"+strBegin+"' and getoutput_time < '"+strEnd+"' and application_return_code=0 and wrapper_return_code!=0 "+dest_condition
    logging.info("=============> %s"%queryString)
    taskCheck = queryMethod(queryString, None)
    return taskCheck

def getList_Destination(begin_t=0): 
    strBegin = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(begin_t))) 
    queryString = "select distinct(destination) from bl_runningjob where destination is not null and destination!='' and submission_time > '"+strBegin+"'"
    taskCheck = queryMethod(queryString, None)
    return taskCheck

def getNumJobSubmittedBySite(begin_t=0): 
    strBegin = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(begin_t)))
    queryString = "select count(*),destination from bl_runningjob where submission_time>'"+strBegin+"' and destination is not null and destination!='' group by destination "
    taskCheck = queryMethod(queryString, None)
    return taskCheck


# Statistics users

def getUser(from_time):
    queryString = "select count(distinct  bl_task.user_proxy ),  bl_task.user_proxy, bl_runningjob.creation_timestamp from bl_task join bl_runningjob where bl_task.id = bl_runningjob.task_id and bl_runningjob.creation_timestamp > DATE_SUB(Now(),INTERVAL "+str(from_time)+" DAY) group by bl_task.user_proxy order by creation_timestamp;"
    taskCheck = queryMethod(queryString, None)
    return taskCheck


def getNameFromProxy(path):
   cmd="voms-proxy-info -file "+path+" -subject"
   if os.path.exists(path) == True:
       name = commands.getstatusoutput(cmd)
   else:
       name=[1,'/CN=Proxy not found']

   return name




# Componets Monitor

def getPIDservice(search_service,service):
#	cmd = 'ps -e | grep '+search_service+' | cut -d " " -f 1'
#	cmd = 'ps ax |grep '+search_service+' |grep -v "grep '+search_service+'" | cut -d " " -f 1 | head -1'
    cmd = 'ps -ef |grep "'+search_service+'"  |grep -v "grep '+search_service+'" | cut -d " " -f 6 | head -1'

    shellOut = commands.getstatusoutput(cmd)
    pid = shellOut[1]
    if  not os.path.exists("/proc/"+str(pid)) or str(pid) == "" :
        msg = [service,"Not Running"]
    else:
        msg = [service,"PID : "+pid ]

    return msg

def getpidof(procname,service):
    cmd = os.popen("ps -A -o pid,command")
    for l in cmd.readlines():
        s = l.strip().split(' ')[1]
        pid = 0
        if procname in s and s[0] =='/':
           pid = l.strip().split(' ')[0]
           break
    if  not os.path.exists("/proc/"+str(pid)) or str(pid) == 0 :
        msg = [service,"Not Running"]
    else:
        msg = [service,"PID : "+pid ]

    return msg

