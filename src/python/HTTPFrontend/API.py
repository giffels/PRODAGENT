from TaskTracking.TaskStateAPI import *
import time, os, datetime



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

def getNumJobResubmitting():

   queryString = "select count(*) from ms_message where ms_message.dest = (select procid  from ms_process where name = \'JobSubmitter\');"
   taskCheck = queryMethod(queryString, None)
   num_rows=taskCheck[0]
   return num_rows[0]


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


def getList_Application_Error():
        queryString = "select application_return_code, count(*) from bl_runningjob where application_return_code <> 0 or application_return_code is not null group by  application_return_code"
        taskCheck = queryMethod(queryString, None)
        return taskCheck


def getList_Wapper_Error():
        queryString = "select wrapper_return_code, count(*) from bl_runningjob where wrapper_return_code <> 0 or application_return_code is not null group by wrapper_return_code"
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


def getNumBossLiteRunningJobs(key,date):
   strDate = time.strftime('%Y-%m-%d %H:%M:%S',(time.gmtime(date)))
   queryString = "select count(*),"+key+" from bl_runningjob where lb_timestamp < '"+strDate+"' group by "+key+" "
   taskCheck = queryMethod(queryString, None)
   return taskCheck

