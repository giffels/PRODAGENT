<?
			include_once("../Conf.php");
                        include_once "local/monParams-FTS.php";
			

			echo " <title>Start/Stop Prodagent Components</title>";
			echo " <form name=myform action=$PHP_SELF? method=post>";


			$action=$_POST["action"];
			if($action=="Restart prodagent"){
			$tail = `$prodagent --restart`;
			}
			elseif($action=="Stop prodagent"){
			$tail = `$prodagent --shutdown`;
			}
			elseif($action=="Start prodagent"){
			$tail = `$prodagent --start`;
			}
			elseif(strpos($action,"top ")){ 
			$component=substr($action,5);
			$tail = `$prodagent --shutdown --component=$component`;
			}
			elseif(strpos($action,"tart ")){ 
			$component=substr($action,6);
			$tail = `$prodagent --start --component=$component`;
			}


//row1cell1
			echo "<center>";
			echo "<h2>ProdAgent Status</h2>";
                        $stato = `$prodagent --status`;
			echo "<table border=0>";

			$running=0;
				if(strpos($stato, "JobTracking Running")) { $running+=1; echo "<tr style=\"background-color: rgb(153, 255, 153); vertical-align: center;\"><TD>JobTracking</td><TD> Running</TD><td align=center><input type=submit name=action value=\"Stop JobTracking\" onClick=\"return confirm('Do you really want to stop JobTracking?')\" ></td></TR>";}
				else { echo "<tr style=\"background-color: rgb(255, 102, 102); vertical-align: center;\"><TD>JobTracking</td><TD>Not Running</TD><td align=center><input type=submit name=action value=\"Start JobTracking\" onClick=\"return confirm('Do you really want to start JobTracking?')\"></td></TR>";}
				if(strpos($stato, "DBSInterface Running")) {$running+=1;  echo "<tr style=\"background-color: rgb(153, 255, 153); vertical-align: center;\"><TD>DBSInterface</td><TD> Running</TD><td align=center><input type=submit name=action value=\"Stop DBSInterface\" onClick=\"return confirm('Do you really want to stop DBSInterface?')\"></td></TR>";}
				else { echo "<tr style=\"background-color: rgb(255, 102, 102); vertical-align: center;\"><TD>DBSInterface</td><TD>Not Running</TD><td align=center><input type=submit name=action value=\"Start DBSInterface\" onClick=\"return confirm('Do you really want to start DBSInterface?')\"></td></TR>";}
				if(strpos($stato, "MergeSensor Running")) { $running+=1; echo "<tr style=\"background-color: rgb(153, 255, 153); vertical-align: center;\"><TD>MergeSensor</td><TD> Running</TD><td align=center><input type=submit name=action value=\"Stop MergeSensor\" onClick=\"return confirm('Do you really want to stop MergeSensor?')\"></td></TR>";}
				else { echo "<tr style=\"background-color: rgb(255, 102, 102); vertical-align: center;\"><TD>MergeSensor</td><TD>Not Running</TD><td align=center><input type=submit name=action value=\"Start MergeSensor\" onClick=\"return confirm('Do you really want to start MergeSensor?')\"></td></TR>";}
				if(strpos($stato, "ErrorHandler Running")) {$running+=1;  echo "<tr style=\"background-color: rgb(153, 255, 153); vertical-align: center;\"><TD>ErrorHandler</td><TD> Running</TD><td align=center><input type=submit name=action value=\"Stop ErrorHandler\" onClick=\"return confirm('Do you really want to stop ErrorHandler?')\"></td></TR>";}
				else { echo "<tr style=\"background-color: rgb(255, 102, 102); vertical-align: center;\"><TD>ErrorHandler</td><TD>Not Running</TD><td align=center><input type=submit name=action value=\"Start ErrorHandler\" onClick=\"return confirm('Do you really want to start ErrorHandler?')\"></td></TR>";}
				if(strpos($stato, "JobCleanup Running")) { $running+=1; echo "<tr style=\"background-color: rgb(153, 255, 153); vertical-align: center;\"><TD>JobCleanup</td><TD> Running</TD><td align=center><input type=submit name=action value=\"Stop JobCleanup\" onClick=\"return confirm('Do you really want to stop JobCleanup?')\"></td></TR>";}
				else { echo "<tr style=\"background-color: rgb(255, 102, 102); vertical-align: center;\"><TD>JobCleanup</td><TD>Not Running</TD><td align=center><input type=submit name=action value=\"Start JobCleanup\" onClick=\"return confirm('Do you really want to start JobCleanup?')\"></td></TR>";}
				if(strpos($stato, "DLSInterface Running")) {$running+=1;  echo "<tr style=\"background-color: rgb(153, 255, 153); vertical-align: center;\"><TD>DLSInterface</td><TD> Running</TD><td align=center><input type=submit name=action value=\"Stop DLSInterface\" onClick=\"return confirm('Do you really want to stop DLSInterface?')\"></td></TR>";}
				else { echo "<tr style=\"background-color: rgb(255, 102, 102); vertical-align: center;\"><TD>DLSInterface</td><TD>Not Running</TD><td align=center><input type=submit name=action value=\"Start DLSInterface\" onClick=\"return confirm('Do you really want to start DLSInterface?')\"></td></TR>";}
				if(strpos($stato, "AdminControl Running")) {$running+=1;  echo "<tr style=\"background-color: rgb(153, 255, 153); vertical-align: center;\"><TD>AdminControl</td><TD> Running</TD><td align=center><input type=submit name=action value=\"Stop AdminControl\" onClick=\"return confirm('Do you really want to stop AdminControl?')\"></td></TR>";}
				else { echo "<tr style=\"background-color: rgb(255, 102, 102); vertical-align: center;\"><TD>AdminControl</td><TD>Not Running</TD><td align=center><input type=submit name=action value=\"Start AdminControl\" onClick=\"return confirm('Do you really want to start AdminControl?')\"></td></TR>";}
				if(strpos($stato, "StatTracker Running")) {$running+=1; echo "<tr style=\"background-color: rgb(153, 255, 153); vertical-align: center;\"><TD>StatTracker</td><TD> Running</TD><td align=center><input type=submit name=action value=\"Stop StatTracker\" onClick=\"return confirm('Do you really want to stop StatTracker?')\"></td></TR>";}
				else { echo "<tr style=\"background-color: rgb(255, 102, 102); vertical-align: center;\"><TD>StatTracker</td><TD>Not Running</TD><td align=center><input type=submit name=action value=\"Start StatTracker\" onClick=\"return confirm('Do you really want to start StatTracker?')\"></td></TR>";}
				if(strpos($stato, "RequestInjector Running")) {$running+=1;  echo "<tr style=\"background-color: rgb(153, 255, 153); vertical-align: center;\"><TD>RequestInjector</td><TD> Running</TD><td align=center><input type=submit name=action value=\"Stop RequestInjector\" onClick=\"return confirm('Do you really want to stop RequestInjector?')\"></td></TR>";}
				else { echo "<tr style=\"background-color: rgb(255, 102, 102); vertical-align: center;\"><TD>RequestInjector</td><TD>Not Running</TD><td align=center><input type=submit name=action value=\"Start RequestInjector\" onClick=\"return confirm('Do you really want to start RequestInjector?')\"></td></TR>";}
				if(strpos($stato, "JobCreator Running")) {$running+=1;  echo "<tr style=\"background-color: rgb(153, 255, 153); vertical-align: center;\"><TD>JobCreator</td><TD> Running</TD><td align=center><input type=submit name=action value=\"Stop JobCreator\" onClick=\"return confirm('Do you really want to stop JobCreator?')\"></td></TR>";}
				else { echo "<tr style=\"background-color: rgb(255, 102, 102); vertical-align: center;\"><TD>JobCreator</td><TD>Not Running</TD><td align=center><input type=submit name=action value=\"Start JobCreator\" onClick=\"return confirm('Do you really want to start JobCreator?')\"></td></TR>";}
				if(strpos($stato, "JobSubmitter Running")) {$running+=1;  echo "<tr style=\"background-color: rgb(153, 255, 153); vertical-align: center;\"><TD>JobSubmitter</td><TD> Running</TD><td align=center><input type=submit name=action value=\"Stop JobSubmitter\" onClick=\"return confirm('Do you really want to stop JobSubmitter?')\"></td></TR>";}
				else { echo "<tr style=\"background-color: rgb(255, 102, 102); vertical-align: center;\"><TD>JobSubmitter</td><TD>Not Running</TD><td align=center><input type=submit name=action value=\"Start JobSubmitter\" onClick=\"return confirm('Do you really want to start JobSubmitter?')\"></td></TR>";}

			echo "</table>";
			if($running >0){
			echo " <input type=submit name=action value=\"Restart prodagent\" onClick=\"return confirm('Do you really want to restart all Prodagent Components?')\">";
			echo " <input type=submit name=action value=\"Stop prodagent\" onClick=\"return confirm('Do you really want to stop all Prodagent Components?')\">";
			 }
			 else {
			echo " <input type=submit name=action value=\"Start prodagent\" onClick=\"return confirm('Do you really want to start all Prodagent Components?')\">";
			 
			 }

			echo " <br><br><input type=button name=back value=\"Back to the main page\" onClick=\"window.location='../index.php'\">";
			echo "</center>";


?>
