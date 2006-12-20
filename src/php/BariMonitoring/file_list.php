<?
echo "<HTML>\n<HEAD>\n<TITLE>SE listing</TITLE>\n</HEAD>\n<body bgcolor=#EBFAFC>\n";


	$site=$_GET["site"];
	$full_list=$_GET["full_list"];
	if($full_list!=1){$full_list=0;}
	$production=$_GET["production"];
	include_once "local/monParams-FTS.php";

	if($job_type=="merge"){$production_plus="%merge%";}


	$db_name1=str_replace("_BOSS","",$db_name);
	//echo "dbname1=$db_name1   dbname=$db_name";
	$link = mysql_connect($DB_HOST.":".$DB_PORT,$DB_USER,$DB_PASS);
	
	if($site=="all"){
	echo "<h2><a name=top>List of the files stored on all SE's, poduction $production</a></h2>";}
	else {
	echo "<h2><a name=top>List of the files stored on the $site SE's, poduction $production</a></h2>";}

	$filename="file_lists/$production-dbsunmerg.txt";
	echo "sto cercando il file: ".$filename;
	if(file_exists($filename)){
		$fd = fopen ($filename, "r");
		$content_unmerge = fread ($fd, filesize ($filename));
		fclose ($fd);
	}
	else {echo "file $filename not available<br>";}

	$filename="file_lists/$production-dbsmerg.txt";	
	if(file_exists($filename)){
	$fd = fopen ($filename, "r");
	$content_merge = fread ($fd, filesize ($filename));
	fclose ($fd);
	}
	else {echo "file $filename not available<br>";}




	$tier=checkSiteNamebyCE($production);
	for($i=0;$i<count($tier);$i++){
		if($tier[$i]==$site||$site=="all"){

			echo "<table><tr><td>$tier[$i]</td><td><a href=\"$tier[$i]_unmerged\"> unmerged</a></td><td><a href=\"$tier[$i]_merged\"> merged</a></td></tr>";
		}
	}
	echo "</table>";
	
	for($i=0;$i<count($tier);$i++){
		if($tier[$i]==$site||$site=="all"){

			if(strpos($tier[$i],"FN-T1")){$filename="file_lists/$production/list_files_INFN-T1.txt";}
			else {$filename="file_lists/$production/list_files_".$tier[$i].".txt";}
			if(file_exists($filename)){
				$last_change=filectime ($filename);
				echo "<h4><a name=\"$tier[$i]_unmerged\"> <br> </a><br><br>Site $tier[$i] unmerged files. Last update at ".date("G:i d/m",$last_change)."&nbsp; &nbsp; &nbsp; <a href=\"#top\"> &nbsp; top</a></h4>\n";
				$fcontents = file ($filename);
				while (list ($line_num, $line) = each ($fcontents)) {
					$subline=substr( $line,strrpos($line, " ")+1,41);
					$TASK_ID="";
					$ID="";
					$TASK_name="";
					$LFN="";
					$SE_OUT="";
					$query1="SELECT job_index from st_job_attr where attr_value like '%$subline%' and attr_class='output_files'";
					$result1=mysql_db_query($db_name1,$query1);
					$num_rows1=mysql_numrows($result1);
					$row1=mysql_fetch_array($result1);
						
					$query2="SELECT attr_value from st_job_attr where job_index='$row1[0]' and attr_class='run_numbers'";
					$result2=mysql_db_query($db_name1,$query2);
					$num_rows2=mysql_numrows($result2);
					$row2=mysql_fetch_array($result2);
					//echo "run_number= $row2[0]   job_index= $row1[0]<br>";
					$query3="SELECT attr_value,job_index from st_job_attr where attr_value='$row2[0]' and attr_class='run_numbers'";
					$result3=mysql_db_query($db_name1,$query3);
					$num_rows3=mysql_numrows($result3);
					$row3=mysql_fetch_array($result3);
					if($num_rows3==1) {$merged="<font color=RED>NOT MERGED </font>";}
					elseif ($num_rows3==2) {
						$merged="<font color=green>    MERGED </font>";
						$row3=mysql_fetch_array($result3);
						$query5="SELECT attr_value from st_job_attr where job_index='$row3[1]' and attr_class='run_numbers'";
						$result5=mysql_db_query($db_name1,$query5);
						while($row5=mysql_fetch_array($result5)){
							$query4="
							SELECT TASK_ID,ID,TASK_name,LFN,SE_OUT from cmssw where N_RUN='$row5[0]' $prod_success_job_cond";
							$result4=mysql_db_query($db_name,$query4);
							$num_rows4=mysql_numrows($result4);
							$row4=mysql_fetch_array($result4);
							if($num_rows4>1){
								while($row4=mysql_fetch_array($result4)){
									$merged.="<a href=\"http://$PAserver/~prodagent/$prodarea_alias/JobTracking/BossJob_".$row4[0]."_1/Submission_$row4[1]\">".$row4[0].".".$row4[1]."</a> ";
								}
							}
						}
					}
					else {$merged="<font color=RED>--ERROR: $num_rows3</font>";}
				

					$query="
					SELECT TASK_ID,ID,TASK_name,LFN,SE_OUT from cmssw where N_RUN='$row2[0]' $prod_success_job_cond";
					//echo $query."<br>\n";
					$result=mysql_db_query($db_name,$query);
					$num_rows=mysql_numrows($result);
					$row=mysql_fetch_array($result);
					$TASK_ID=$row[0];
					$ID=$row[1];
					$TASK_name=$row[2];
					$LFN=$row[3];
					$SE_OUT=$row[4];
					
					if(strpos($content_unmerge,$subline)){
					echo "<pre> " . substr( $line,0,strlen($line)-1). "  <font color=green>    IN DBS</font> <a href=\"http://$PAserver/~prodagent/$prodarea_alias/JobTracking/BossJob_".$TASK_ID."_1/Submission_$ID\">$TASK_ID</a> $ID $TASK_name $merged </pre>\n";
					}
					else {
					echo "<pre> " . substr( $line,0,strlen($line)-1). "  <font color=red>Not IN DBS</font> <a href=\"http://$PAserver/~prodagent/$prodarea_alias/JobTracking/BossJob_".$TASK_ID."_1/Submission_$ID\">$TASK_ID</a> $ID $TASK_name $merged </pre>\n";
					}
			}

				
			}
			if(strpos($tier[$i],"FN-T1")){$filename="file_lists/$production/list_files_INFN-T1_merge.txt";}
			else {$filename="file_lists/$production/list_files_".$tier[$i]."_merge.txt";}
			
			if(file_exists($filename)){
				$last_change=filectime ($filename);
				echo "<h4><a name=\"$tier[$i]_merged\"> <br> </a><br><br>Site $tier[$i] merged files. Last update at ".date("G:i d/m",$last_change)."&nbsp; &nbsp; &nbsp; <a href=\"#top\"> &nbsp; top</a></h4>\n";
				$fcontents = file ($filename);
				while (list ($line_num, $line) = each ($fcontents)) {
					$subline=substr( $line,strrpos($line, " ")+1,41);
					$TASK_ID="";
					$ID="";
					$TASK_name="";
					$LFN="";
					$SE_OUT="";
					$query1="SELECT job_index from st_job_attr where attr_value like '%$subline%' and attr_class='output_files'";
					$result1=mysql_db_query($db_name1,$query1);
					$num_rows1=mysql_numrows($result1);
					$row1=mysql_fetch_array($result1);
						
					$query2="SELECT attr_value from st_job_attr where job_index='$row1[0]' and attr_class='run_numbers'";
					$result2=mysql_db_query($db_name1,$query2);
					$num_rows2=mysql_numrows($result2);
					if(strpos($content_merge,$subline)){
					echo "<pre> " . substr( $line,0,strlen($line)-1). "  <font color=green>    IN DBS</font> ";
					}
					else {
					echo "<pre> " . substr( $line,0,strlen($line)-1). "  <font color=red>Not IN DBS</font> ";
					}
					$str=" TASK_ID.ID of merged JOBS:";
					while($row2=mysql_fetch_array($result2)){
						$query="
						SELECT TASK_ID,ID,TASK_name,LFN,SE_OUT from cmssw where N_RUN='$row2[0]' $prod_success_job_cond";
						//echo $query."<br>\n";
						$result=mysql_db_query($db_name,$query);
						$num_rows=mysql_numrows($result);
						
						$row=mysql_fetch_array($result);
						$str.="<a href=\"http://$PAserver/~prodagent/$prodarea_alias/JobTracking/BossJob_".$row[0]."_1/Submission_$row[1]\">".$row[0].".".$row[1]."</a> ";
						if($num_rows>1){
						$row=mysql_fetch_array($result);
						echo "<a href=\"http://$PAserver/~prodagent/$prodarea_alias/JobTracking/BossJob_".$row[0]."_1/Submission_$row[1]\">".$row[0].".".$row[1]."</a> $row[2]";
						}

					}
					echo "$str </pre>\n";
					//echo "nrow1=$num_rows1  nrow2=$num_rows2 run_number= $row2[0]   job_index= $row1[0]<br>";

					
				}
			}
		}
	}

?>


