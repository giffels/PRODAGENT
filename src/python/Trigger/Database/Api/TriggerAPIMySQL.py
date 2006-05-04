
from ProdAgentDB.Connect import connect

def setFlag(triggerId,jobSpecId,flagId):
   try:
       conn=connect()
       dbCur=conn.cursor()
       sqlStr="""UPDATE tr_Trigger SET FlagValue="finished" WHERE
           TriggerID="%s" AND JobSpecID="%s" 
           AND FlagID="%s" ; """ %(triggerId,jobSpecId,flagId)
       dbCur.execute("START TRANSACTION")
       rowsModified=dbCur.execute(sqlStr)
       if rowsModified==0:
           raise Exception("ERROR:","Flag "+triggerId+','+jobSpecId+','+ \
               flagId+ " does not exists")
       dbCur.execute("COMMIT")
       # check if all flags are set:
       sqlStr=""" SELECT COUNT(*) FROM (SELECT COUNT(*) as total_count FROM tr_Trigger WHERE TriggerID="%s" AND JobSpecID="%s") as total_count, (SELECT COUNT(*) as total_count FROM tr_Trigger WHERE FlagValue="finished" AND TriggerID="%s" AND JobSpecID="%s") as finished_count WHERE total_count.total_count=finished_count.total_count; """ %(triggerId,jobSpecId,triggerId,jobSpecId)
       dbCur.execute(sqlStr)
       rows=dbCur.fetchall()
       result=rows[0][0]
       dbCur.close()
       if(result==1):
          return True
       return False
   except:
       dbCur.execute("ROLLBACK")
       dbCur.close()
       raise


def resetFlag(triggerId,jobSpecId,flagId):
   try:
       conn=connect()
       dbCur=conn.cursor()
       sqlStr="""UPDATE tr_Trigger SET FlagValue="start" WHERE
           TriggerID="%s" AND JobSpecID="%s" 
           AND FlagID="%s" """ %(triggerId,jobSpecId,flagId)
       dbCur.execute("START TRANSACTION")
       rowsModified=dbCur.execute(sqlStr)
       if rowsModified==0:
           raise Exception("ERROR:","Flag "+triggerId+','+jobSpecId+','+ \
               flagId+ " does not exists")
       dbCur.execute("COMMIT")
       dbCur.close()
   except:
       dbCur.execute("ROLLBACK")
       dbCur.close()
       raise

def flagSet(triggerId,jobSpecId,flagId):
   conn=connect()
   dbCur=conn.cursor()
   sqlStr="""SELECT FlagValue FROM tr_Trigger WHERE TriggerID="%s" 
       AND FlagID="%s" AND JobSpecID="%s"; """ %(triggerId,flagId,jobSpecId)
   dbCur.execute(sqlStr)
   rows=dbCur.fetchall()
   if (len(rows)==0):
       return False
   if rows[0][0]!="finished":
       return False
   return True

def allFlagSet(triggerId,jobSpecId):
   conn=connect()
   dbCur=conn.cursor()
   # check if all flags are set:
   sqlStr=""" SELECT COUNT(*) FROM (SELECT COUNT(*) as total_count 
       FROM tr_Trigger WHERE TriggerID="%s" AND JobSpecID="%s") as 
       total_count, (SELECT COUNT(*) as total_count FROM tr_Trigger 
       WHERE FlagValue="finished" AND TriggerID="%s" AND JobSpecID="%s") 
       as finished_count WHERE 
       total_count.total_count=finished_count.total_count; 
       """ %(triggerId,jobSpecId,triggerId,jobSpecId)
   dbCur.execute(sqlStr)
   rows=dbCur.fetchall()
   result=rows[0][0]
   dbCur.close()
   if(result==1):
      return True
   return False

def addFlag(triggerId,jobSpecId,flagId):
   conn=connect()
   dbCur=conn.cursor()
   try:
      dbCur.execute("START TRANSACTION")
      sqlStr="""INSERT INTO tr_Trigger(JobSpecID,TriggerID,FlagID,FlagValue)
             VALUES("%s","%s","%s","start") ;""" %(jobSpecId,triggerId,flagId)
      rowsModified=dbCur.execute(sqlStr)
      dbCur.execute("COMMIT")
      dbCur.close()
   except:
      dbCur.execute("ROLLBACK")
      dbCur.close()
      raise Exception("ERROR:","Flag "+triggerId+','+jobSpecId+','+flagId+ \
          " already exists or jobspec ID does not exist.")

def setAction(jobSpecId,triggerId,actionName):
   conn=connect()
   dbCur=conn.cursor()
   try:
      dbCur.execute("START TRANSACTION")
      sqlStr="""INSERT INTO tr_Action(jobSpecId,triggerId,actionName)
             VALUES("%s","%s","%s") ;""" %(jobSpecId,triggerId,actionName)
      dbCur.execute(sqlStr)
      dbCur.execute("COMMIT")
      dbCur.close()
   except:
      dbCur.execute("ROLLBACK")
      dbCur.close()
      raise Exception("ERROR:","Trigger "+triggerId+' does not exist')

def getAction(triggerId,jobSpecId):
   conn=connect()
   dbCur=conn.cursor()
   sqlStr="""SELECT ActionName FROM tr_Action WHERE TriggerID="%s" 
          AND JobSpecID="%s" ; """ %(triggerId,jobSpecId)
   dbCur.execute(sqlStr)
   rows=dbCur.fetchall()
   if(len(rows)==1):
       return rows[0][0]
   else:
       raise Exception("ERROR","No Action Associated")
        

def cleanout(jobSpecId):
   try:
      conn=connect()
      dbCur=conn.cursor()
      dbCur.execute("START TRANSACTION")
      sqlStr="""DELETE FROM tr_Trigger WHERE JobSpecID="%s" """ %(jobSpecId)
      dbCur.execute(sqlStr)
      sqlStr="""DELETE FROM tr_Action WHERE JobSpecID="%s" """ %(jobSpecId)
      dbCur.execute(sqlStr)
      dbCur.execute("COMMIT")
      dbCur.close()
   except:
      dbCur.execute("ROLLBACK")
      dbCur.close()
      raise

