#!/usr/bin/env python

import os
from ProdCommon.Database.Operation import Operation
from ProdAgentDB.Config import defaultConfig as dbConfig
from ProdAgentCore.Configuration import ProdAgentConfiguration


class AlertMonitor:
      """
      _AlertMonitor_
      This class provides the main interface of Alert monitoring. It fetches the updated alerts data from
      database and present it in the graphical user interface
      Alert types other than 'critical' and 'error' will automatically move to history after some time interval
      where as 'critical' and 'error' alerts will stay current unless user move them to history 

      AlertMonitor page refresh time and move current alerts to history alert time can be confingured by following 
      parameters in HTTPFrontend Config block in ProdAgentConfiguration

      <Parameter Name='AlertMonitorRefreshTime' Value='10'/> # time in seconds
      <Parameter Name='MoveAlertTime' Value='20'/>     # time in seconds
      """

      def __init__ (self, baseURL):
          """
          _init_

          Initization function
          """ 
          self.args = {}

          self.args['ComponentURL'] = baseURL + "/alertmonitor" 
	  self.args['BaseURL'] = baseURL  
	  self.args['AlertMonitorRefreshTime'] = 2*60  # time in seconds          
          self.args['MoveAlertTime'] = 5*60  # time in seconds

          self.args['component'] = 'ALL'

          try:
               config = os.environ.get("PRODAGENT_CONFIG", None)
               if config == None:
                  msg = "No ProdAgent Config file provided\n"
                  raise RuntimeError, msg

               cfgObject = ProdAgentConfiguration()
               cfgObject.loadFromFile(config)
               alertHandlerConfig = cfgObject.get("HTTPFrontend")

               self.args.update (**alertHandlerConfig)
          except Exception, ex:
               msg = 'AlertMonitor Initialization Failed'
               raise RuntimeError, msg

 
             

          return 

      def index (self, *arg, **args):
          """
          _index_

          Default method called by CHERRYPY webserver upon URL request that was mapped to this class
          """
          html = None  
          self.args.update(**args)
          updated = self.updateDB() 
          
          if updated is None:  
             html = self.generateHTML()  
          else:
             html = updated 

          return html

      def updateDB (self):
          """
          _updateDB_
          Method to move alerts automatically from current status to history 
          """
          alertDBOperations = Operation(dbConfig)
          
          try:

             tableName = "alert_current"
            
             #// Fetching record to insert into history table
             sqlStr = "select * from " + tableName + " where severity not in ('critical','error') and  time<date_sub(current_timestamp,interval %s second)" % str(self.args['MoveAlertTime'])
  
             alertDBOperations.execute(sqlStr)
             records = alertDBOperations.connection.convert(rows=alertDBOperations.connection.fetchall())

             #// Delete that record from current alerts table
             if len(records) != 0 :  
                sqlStr = "delete  from " + tableName + " where severity not in ('critical','error') and  time<date_sub(current_timestamp,interval %s second)" % str(self.args['MoveAlertTime']) 
                alertDBOperations.execute(sqlStr)
                alertDBOperations.commit()

                #// Insert record into history table
                tableName = "alert_history"
                for record in records:
                   sqlStr = "Insert into " + tableName +"(severity, message, component,generationtime,historytime) values("+"\'"+str(record['severity'])+"\',\'"+str(record['message'])+"\',\'"+str(record['component'])+"\',\'"+str(record['time'])+"\',"+'current_timestamp'+")"
             
                   alertDBOperations.execute(sqlStr)
                alertDBOperations.commit()

          except Exception, ex:

             msg = "<html><body>Failed to update Alert Database<br><br>" +str(ex)+"</body></html>"
             return msg


          return None 

      def generateHTML(self):

           """
           Method to generate HTML CODE for main page      
           """
           
           
           config = os.environ.get("PRODAGENT_CONFIG", None)
           if config == None:
              msg = "No ProdAgent Config file provided\n"
              msg += "either set $PRODAGENT_CONFIG variable\n"
              msg += "or provide the --config option"
              raise RuntimeError,msg

           cfgObject = ProdAgentConfiguration()
           cfgObject.loadFromFile(config)
           components = cfgObject.listComponents()
 
           html =  """
           <html><head><title>ALERT MONITORING TOOL</title>
           <script type=text/javascript>
             function formsubmit()
             {
 alert('form submit')
}
	     function reload()
	     {
	     var t = setTimeout ('refresh()',""" + str(int(self.args['AlertMonitorRefreshTime'])*1000) + """)
	     	   

	     }
	     function refresh()
	     {
	     window.location.href = \'""" + self.args['ComponentURL'] +"""\' 
	     
	     }
	     
	   </script>  

	   </head>
	  
	   <body onload=reload()>
           <a href="""+ self.args['BaseURL']+""">Home</a>
           <h4>Select Component:
          <form name="components" action='""" +self.args['ComponentURL']+"""' target='_self' >
<select name="component" size="1" onChange=components.submit()>"""
           components.append('ALL') 
           for component in components: 
                 
               html+= "<option value=" +component
               if self.args['component'] == component:
                   html+=" selected"  
               html+= ">"+ component+" </option>"

           html += """

</select></h4>
 
</form> 
 
           <center><h4>Component: """ +self.args['component']+ """</h4></center> 
           <table border=5 width = 90%  align = center>
<tr ><td colspan=11 bgcolor=#8888CC align=center><font color=black>AlertMonitor</font></td></tr>

<tr><td  bgcolor='white' align=center colspan=5>CurrentAlerts</td>
<td bgcolor=#EEEEFF>&nbsp;</td>
<td   bgcolor=white colspan=5 align=center>Alert History</td>


</tr>
<tr>
<td  colspan=5><div><object style="overflow-x:hidden; width:100%; height:400;" type=text/html data='"""+self.args['ComponentURL']+"""/currentalert?component="""+self.args['component']+"""'></object></div></td>
<td bgcolor=#EEEEFF>
<td  colspan=5></div><object style="overflow-x:hidden; width:100%; height:400;" type=text/html data='"""+self.args['ComponentURL']+"""/historyalert?component="""+self.args['component']+"""'></object></div></td>


</tr>



</table>


              

           """ 

           return html
      index.exposed = True

  
class CurrentAlert:
      """
      Class that return html code to show current alerts 
      """ 
      
      def __init__ (self, baseURL):
          """
	  Initialization Method
	  """  
          self.args = {}
	  self.args['ComponentURL'] = baseURL + "/alertmonitor/currentalert"
	  self.args['BaseURL'] = baseURL                    
          self.args['component'] = 'ALL'

          self.alertDBOperations = Operation(dbConfig)


	  return
	  
      def index (self, *arg, **args):
          """
	  Default method called by CHERRYPY
	  """
         

          self.args.update(**args)
          

	  records = None
	  bgarray = ['#AA6666','#AA8888','#FFEEAA','#EEEEEE','#FFF8DC']	  
          types = ['CRITICAL','ERROR','WARNING','MINOR']  
	  
	  arr = ['Jobcreator component failed please check it','3 message','4 message','5 message','6 message']
          

	  html="<html><body><table id=current border=3>"
          html+="<tr><td width=60% colspan=1 align=center>"+"Message"+"</td><td width=10%>Component</td><td width=10% align=center>"+"Type"+"</td><td width=10% align=center>Creation time</td><td width=10% align=center>Move to History?</td></tr>"

	  
          bgArray = ['red','#AA8888','#FFEEAA','#EEEEEE','#FFF8DC']
          types = ['CRITICAL','ERROR','WARNING','MINOR']
	  i =0


         
	  try:
	    sqlStr = ""
            if self.args['component'] == 'ALL':
	       sqlStr = "select * from alert_current order by time desc"
            else:
               sqlStr = "select * from alert_current where component=\'%s\' order by time desc" % self.args['component']
 
	    self.alertDBOperations.execute(sqlStr)
	    records = self.alertDBOperations.connection.convert(rows=self.alertDBOperations.connection.fetchall())
            self.alertDBOperations.commit() 
	  except Exception, ex:
	  
	     
              msg = "<html><body>Exception caught while fetching alert records from database<br><br>" +str(ex)+"</body></html>"
              return msg
	      

          for item in records:
            if item['severity'].lower() == 'critical':
              bg = bgArray[0]
            elif item['severity'].lower() == 'error':
              bg =bgArray[1]
	    elif item['severity'].lower() == 'warning':
              bg =bgArray[2]
	    elif item['severity'].lower() == 'minor':
              bg =bgArray[3]
            
            html+="<tr bgcolor="+bg+"><td width=65% colspan=1 align= left>"+str(item['message'])+"</td><td align=center width=5%><font size=2>"+str(item['component'])+"</font></td><td width=10% align=center><font size=2>"+str(item['severity'])+"</font></td><td width=10% align=center><font size=1>"+str(item['time'])+"</font></td><td width=10% align=center><font size=1>"
            if str(item['severity']).lower() in ['critical','error']:
              html+="<a href="+ self.args['ComponentURL']+"/updateDB?id="+str(item['id'])+"> Move</a> </font></td></tr>"
            else:
              html+= "Automatic</font></td></tr>"  

          html+="</table></body></html>"
	    

	  return html


      def updateDB (self, **args):
          """
          _updateDB_
          Moves Current Alerts to History Alerts
          """

          try:
 
             tableName = "alert_current"

             #// Fetching record to insert into history table
             sqlStr = "select * from " + tableName + " where id = \'%s\'" %args['id']
             self.alertDBOperations.execute(sqlStr)
             records = self.alertDBOperations.connection.convert(rows=self.alertDBOperations.connection.fetchall())

             #// Delete that record from current alerts table 
             if len(records) != 0:
                sqlStr = "delete  from " + tableName + " where id = \'%s\'" %args['id'] 
                self.alertDBOperations.execute(sqlStr)
                self.alertDBOperations.commit()

                #// Insert record into history table 
                tableName = "alert_history"
                sqlStr = "Insert into " + tableName +"(severity, message, component,generationtime,historytime) values("+"\'"+str(records[0]['severity'])+"\',\'"+str(records[0]['message'])+"\',\'"+str(records[0]['component'])+"\',\'"+str(records[0]['time'])+"\',"+'current_timestamp'+")" 
         
                self.alertDBOperations.execute(sqlStr)
                self.alertDBOperations.commit()  

          except Exception, ex:

             msg = "<html><body>Failed to update Alert Database<br><br>" +str(ex)+"</body></html>"
             return msg 
             

              
          html = """
        
          <html><body><script type=text/javascript>

          window.parent.location.href=window.parent.location.href
                 """
         
          html +=""" </script></body></html>"""

          return html

      updateDB.exposed = True 	  
      index.exposed = True  
 
   
class HistoryAlert:
      """
      Class that return html code to show current alerts 
      """ 
      
      def __init__ (self, baseURL):
          """
	  Initialization Method
	  """  
          self.args ={}
	  self.args['ComponentURL'] = baseURL + "/alertmonitor/historyalert"
          self.args['BaseURL'] = baseURL 
          self.args['component'] = 'ALL'
 
	  return
	  
      def index (self, *arg, **args):
          """
	  Default method called by CHERRYPY
	  """  
          self.args.update(**args)

	  records= None
	  
	  bgArray = ['red','#AA8888','#FFEEAA','#EEEEEE','#FFF8DC']
          types = ['CRITICAL','ERROR','WARNING','MINOR']
	  i =0

	  historyAlertDB = Operation(dbConfig)

	  try:
	   

            sqlStr = ""
            if self.args['component'] == 'ALL':
               sqlStr = "select * from alert_history order by historytime desc"
            else:
               sqlStr = "select * from alert_history where component=\'%s\' order by historytime desc" % self.args['component']


	    historyAlertDB.execute(sqlStr)
	    records = historyAlertDB.connection.convert(rows=historyAlertDB.connection.fetchall())
            historyAlertDB.commit() 
	  except Exception, ex:
	  
	       msg = "<html><body>Exception caught while fetching alert records from database<br><br>" +str(ex)+"</body></html>"
               return msg
      

	  html="<html><body><table id=current border=3>"
          html+="<tr><td width=65% colspan=1 align=center>"+"Message"+"</td><td width=5% align=center>"+"Component"+"</td><td width=10% align=center>"+"Type"+"</td><td width=10% align=center>Creation time</td><td width=10% align=center>Moved to History</td></tr>"


          for item in records:
            if item['severity'].lower() == 'critical':
              bg = bgArray[0]
            elif item['severity'].lower() == 'error':
              bg =bgArray[1]
	    elif item['severity'].lower() == 'warning':
              bg =bgArray[2]
	    elif item['severity'].lower() == 'minor':
              bg =bgArray[3]         


            html+="<tr bgcolor="+bg+"><td width=65% colspan=1 align=left>"+str(item['message'])+"</td><td width=5% align=center><font size=2>"+str(item['component'])+"</font></td><td width=10% align=center><font size=2>"+str(item['severity'])+"</font></td><td width=10% align=center><font size=1>"+str(item['generationtime'])+"</font></td><td width=10% align=center><font size=1>"+str(item['historytime'])+"</font></td></tr>"

          html+="</table></body></html>"
	    

	  return html
      index.exposed = True  

