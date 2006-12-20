<?
include_once "local/monParams-FTS.php";
echo "
<HTML>
<HEAD>
<TITLE>Databases browsing</TITLE>
</HEAD>
<body bgcolor=#EBFAFC>

<FORM ACTION=browse_DB.php METHOD=post>";
$db_name=$_POST["db_name"];
$table_name=$_POST["table_name"];
$Action=$_POST["Action"];
$limite_inf=$_POST["limite_inf"];
$num_record=$_POST["num_record"];
$command=$_POST["command"];
$search_field=$_POST["search_field"];
$search_op=$_POST["search_op"];
$search_value=$_POST["search_value"];
$order_field=$_POST["order_field"];


//$db_name="ProdAgentDB_BOSS";


if (!$db_name){

	echo "<H3>Select the Database</H3>";
	echo "<select name=db_name>";
	echo "<option>ProdAgentDB_BOSS\n";
	echo "<option>ProdAgentDB\n";
	echo "<option>ProdAgentDB_BOSS_rt\n";
	echo "<option>ProdAgentDB_BOSS_csa06minbias\n";
	echo "<option>ProdAgentDB_csa06minbias\n";
	echo "<option>ProdAgentDB_BOSS_rt_csa06minbias\n";
	echo "<option>ProdAgentDB_BOSS_ttbar\n";
	echo "<option>ProdAgentDB_ttbar\n";
	echo "<option>ProdAgentDB_BOSS_rt_ttbar\n";
	echo "<option>ProdAgentDB_BOSS_EWKSoup\n";
	echo "<option>ProdAgentDB_EWKSoup\n";
	echo "<option>ProdAgentDB_BOSS_rt_EWKSoup\n";
	echo "<option>gridice_cmsprd\n";
	echo "</select>";

	echo "<input type=submit value=\"Select\">";

}
else {
	$DB_NAME1 = $DB_NAME;
	$DB_HOST1 = $DB_HOST;
	$DB_PORT1 = $DB_PORT;
	$DB_USER1 = $DB_USER;
	$DB_PASS1 = $DB_PASS;

	echo "<input type=hidden name=db_name value=$db_name>\n";
	if ($Action=="" ||$Action=="Back" ){
		echo "<table border=1>\n";
		echo "<tr><td colspan=2><h3>Database name: ".$db_name."</h3></td></tr>";
	
		$link = mysql_connect($DB_SPEC,$DB_USER1,$DB_PASS1);
		$result= mysql_list_tables($db_name);
		echo "<tr>\n<td width=600><h3>Select the table</h3></td>\n<td><select name=table_name onChange=\"this.form.submit()\"><option>";
		$table_found=0;
		for($i=0;$i<mysql_num_rows($result);$i++){
 
			$tablename = mysql_tablename($result,$i);
			if($tablename==$table_name){
			echo "<option selected value=".$tablename.">".$tablename."\n";
			$table_found=1;
			}
			else{echo "<option value=".$tablename.">".$tablename."\n";}
		}
		echo "</select></td>\n</tr>\n";
		if (!$table_found) exit();
		$fields=mysql_list_fields($db_name,$table_name,$link);

		$columns=mysql_num_fields($fields);
		$fields_seq="";
		for($j=0;$j<$columns;$j++){
				if(strpos(mysql_field_flags($fields,$j),"primary_key")){
					$key_primary=mysql_field_name($fields,$j);
				}
				if(!strpos(mysql_field_flags($fields,$j),"auto_increment")){
					$fields_seq.=mysql_field_name($fields,$j)."=\'\',";
				}

		}
		$fields_seq=substr($fields_seq,0, strlen($fields_seq)-1);
		echo "<tr>\n<td>Describe the table</td>\n<td align=right><input type=submit name=Action value=Describe></td>\n</tr>\n";
		echo "<tr>\n<td>Show <input type=text size=5 name=num_record value=100 onChange=\"this.form.num_record[1].value=this.form.num_record[0].value\"> records starting from record <input type=text size=5 name=limite_inf value=0></td>\n
		<td align=right><input type=submit name=Action value=\"Mostra i records\" ></td>\n </tr>\n";
		echo "<tr>\n<td>Show the last <input type=text size=5 name=num_record value=100 onChange=\"this.form.num_record[0].value=this.form.num_record[1].value\"> records </td>\n
		<td align=right><input type=submit name=Action value=\"Mostra records dalla fine\"></td>\n </tr>\n";
		echo "<tr>\n<td>Execute the command<br> <textarea name=command rows=4 cols=70></textarea><br>";
		echo "<input type=button value=SELECT onClick=\"this.form.command.value='SELECT * FROM $table_name WHERE $key_primary=\'\' ORDER BY $key_primary '\">";
		echo "<input type=button value=QUERY_1 onClick=\"this.form.command.value='select SCHED_ID,SCHED_edg.*,N_EVT from JOB,SCHED_edg,cmssw where JOB.TASK_ID=SCHED_edg.TASK_ID and SCHED_edg.TASK_ID=cmssw.TASK_ID'\">";
		
		echo "<input type=button value=QUERY_2 onClick=\"this.form.command.value='select PRIMDATASET,N_RUN,N_EVT,TASK_EXIT,Comment,SE_OUT,cmsRun_STOP-cmsRun_START,MEM,CPU from cmssw WHERE PRIMDATASET=\'CSA06-081-os-minbias\''\">";
		echo "<input type=button value=QUERY_3 onClick=\"this.form.command.value='select PRIMDATASET,N_RUN,N_EVT,TASK_EXIT,Comment,SE_OUT,cmsRun_EXIT,cmsRun_STOP-cmsRun_START,MEM,CPU from cmssw WHERE PRIMDATASET=\'CSA06-081-os-minbias\''\">";

		echo "</td>\n<td align=right><input type=submit name=Action value=\"Esegui il comando\"></td>\n</tr>\n";
		if($table_name){
			echo "<tr>\n<td valign=center>Show records from table <b>$table_name</b><br>\n ";
			echo "WHERE";

			echo "<select name=search_field>\n<option>\n";
			for($j=0;$j<$columns;$j++){
				echo "<option>".mysql_field_name($fields,$j)."\n";
			}
			echo "</select>\n";
			echo "<select name=search_op>\n";
			echo "<option value=\" > \">maggiore\n";
			echo "<option value=\" < \">minore\n";
			echo "<option value=\" = \">uguale\n";
			echo "<option value=\" >= \">maggiore o uguale\n";
			echo "<option value=\" <= \">minore o uguale\n";
			echo "<option value=\" LIKE \">LIKE\n";
			echo "</select>";
			echo "<input type=text name=search_value><br>\n";
			echo "ORDER BY";

			echo "<select name=order_field><option>\n";
			for($j=0;$j<$columns;$j++){
				echo "<option>".mysql_field_name($fields,$j)."\n";
			}
			echo "</select>\n";

			echo "</td>\n";
			echo "<td align=right><input type=submit name=Action value=\"Cerca i records\"></td></tr>";
		}
	}

	if ($Action=="Describe"){
		echo "<h3>Database name: ".$db_name."</h3>";
		echo "<H3>$table_name table description</H3>";
		$link = mysql_connect($DB_SPEC,$DB_USER1,$DB_PASS1);
		$fields=mysql_list_fields($db_name,$table_name,$link);
		$columns=mysql_num_fields($fields);
		echo "<table border=1>";
		echo "<tr><th>Field name</th><th>type</th><th>length</th><th>opzioni</th></tr>";
		for($j=0;$j<$columns;$j++){
			echo "<tr><td>".mysql_field_name($fields,$j)."</td><td>".mysql_field_type($fields,$j)."</td><td>".mysql_field_len($fields,$j)."</td><td>".mysql_field_flags($fields,$j)."</td></tr>";
		}
		echo "</table>";

		echo "<input type=submit name=Action value=Back>";
		echo "<input type=hidden name=table_name value=$table_name>";
	}
	if ($Action=="Mostra i records"){
		echo "<h3>Database name: ".$db_name."</h3>";
		echo "<H3>Records from table $table_name</H3>";
		echo "<input type=submit name=Action value=Back>";

		$link = mysql_connect($DB_SPEC,$DB_USER1,$DB_PASS1);
		$fields=mysql_list_fields($db_name,$table_name,$link);
		$columns=mysql_num_fields($fields);
		echo "<table border=1><tr>";
		for($j=0;$j<$columns;$j++){
			echo "<th>".mysql_field_name($fields,$j)."</th>";
		}
		$result=mysql_db_query($db_name,"SELECT * FROM  $table_name limit $limite_inf,$num_record");
		$num_rows=mysql_numrows($result);
		if ($num_rows==0){echo "<tr><td>Nessun record trovato</td></tr>";}
		else{
			while($row=mysql_fetch_array($result)){
				echo "<tr>";

				for($j=0;$j<$columns;$j++){
			
					if(strlen($row[$j])>120){echo "<td>".substr($row[$j], 0, 32)."...</td>";}
					else {echo "<td>".$row[$j]."</td>";}

				}
				echo "</tr>";
			}
		}		
		echo "</table>";
		echo "<input type=submit name=Action value=Back>";
		echo "<input type=hidden name=table_name value=$table_name>";

	}
	if ($Action=="Mostra records dalla fine"){
		echo "<h3>Database name: ".$db_name."</h3>";
		echo "<H3>Last records from table $table_name</H3>";
		echo "<input type=submit name=Action value=Back>";
		$link = mysql_connect($DB_SPEC,$DB_USER1,$DB_PASS1);
		$fields=mysql_list_fields($db_name,$table_name,$link);
		$columns=mysql_num_fields($fields);
		echo "<table border=1><tr>";
		for($j=0;$j<$columns;$j++){
			echo "<th>".mysql_field_name($fields,$j)."</th>";

		}
		$result=mysql_db_query($db_name,"SELECT * FROM  $table_name");
		$num_rows=mysql_numrows($result);
		$limite_inf=$num_rows-$num_record;
		$result=mysql_db_query($db_name,"SELECT * FROM  $table_name limit $limite_inf,$num_record");
		$num_rows=mysql_numrows($result);
		if ($num_rows==0){echo "<tr><td>Nessun record trovato</td></tr>";}
		else{
			while($row=mysql_fetch_array($result)){
				echo "<tr>";

				for($j=0;$j<$columns;$j++){
					if(strlen($row[$j])>120){echo "<td>".substr($row[$j], 0, 32)."...</td>";}
					else {echo "<td>".$row[$j]."</td>";}

				}
				echo "</tr>";
			}
		}		
		echo "</table>";

		echo "<input type=submit name=Action value=Back>";
		echo "<input type=hidden name=table_name value=$table_name>";
	}

	if ($Action=="Cerca i records"){
		echo "<h3>Database name: ".$db_name."</h3>";
		echo "<H3>Records from table $table_name</H3>";
		$link = mysql_connect($DB_SPEC,$DB_USER1,$DB_PASS1);
		$fields=mysql_list_fields($db_name,$table_name,$link);
		$columns=mysql_num_fields($fields);
		echo "<table border=1><tr>";
		for($j=0;$j<$columns;$j++){
			echo "<th>".mysql_field_name($fields,$j)."</th>";
		}
		if($order_field) {$query = "SELECT * FROM  $table_name Where $search_field $search_op '$search_value'  ORDER BY $order_field";}
		else {$query = "SELECT * FROM  $table_name Where $search_field $search_op '$search_value'";}
		echo $query;
		$result=mysql_db_query($db_name,$query);
			$num_rows=mysql_numrows($result);
					if ($num_rows==0){echo "<tr><td>Nessun record trovato</td></tr>";}
					else{
			while($row=mysql_fetch_array($result)){
							echo "<tr>";
				for($j=0;$j<$columns;$j++){
					if(strlen($row[$j])>120){echo "<td>".substr($row[$j], 0, 32)."...</td>";}
					else {echo "<td>".$row[$j]."</td>";}


				}
				echo "</tr>";
			}
		}		
		echo "</table>";

		echo "<input type=submit name=Action value=Back>";
		echo "<input type=hidden name=table_name value=$table_name>";
	}

	if ($Action=="Esegui il comando"){
		$link = mysql_connect($DB_SPEC,$DB_USER1,$DB_PASS1);
		$command=str_replace("\\","",$command);
		$query=$command;

		$comando=substr($command,0, strpos($command," "));
		
		$result=mysql_db_query($db_name,$query);
		if(mysql_errno()){
		echo $query."<br>\n";
		echo "command '$comando' non executed. Errore di MySQL:".mysql_errno().": ".mysql_error()."<br>";
		}
		else {
			if(strpos($command,"ELECT")+strpos($command,"elect")){
				echo "<h3>Database name: ".$db_name."</h3>";
				echo "<H3>Elenco dei Records della tabella $table_name</H3>";
				$columns=mysql_num_fields($result);
				echo "<table border=1><tr>";
				for($j=0;$j<$columns;$j++){
					echo "<th>".mysql_field_name($result,$j)."</th>";
					if(strpos(mysql_field_flags($result,$j),"primary_key")){echo "<input type=hidden name=primary_key value=\"\">";}

				}
				$num_rows=mysql_numrows($result);
				if ($num_rows==0){echo "<tr><td>Nessun record trovato</td></tr>";}
				else{
					while($row=mysql_fetch_array($result)){
						echo "<tr>";

						for($j=0;$j<$columns;$j++){

						echo "<td >";
						//if(strlen($row[$j])>120){echo substr($row[$j], 0, 32)."...</td>";}
						//else {echo $row[$j]."</td>";}
						echo $row[$j]."</td>";
		
					}
					echo "</tr>";
				}
			}		
			echo "</table>";

			}
			else {
			
			echo "Command '$comando' succesfully executed <br>";
			}
		}

				       
		echo "<input type=submit name=Action value=Back>\n";
		echo "<input type=hidden name=table_name value=$table_name>\n";
	}

}
echo "</form>
</BODY>
</HTML>";
?>


