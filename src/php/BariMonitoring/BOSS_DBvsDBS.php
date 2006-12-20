<link rel="stylesheet" type="text/css" media="all" href="modules/style.css" />
<?
set_time_limit(120);
echo "<HTML>\n<HEAD>\n<TITLE>BOSS_DB versus DBS </TITLE>\n</HEAD>\n<body bgcolor=#EBFAFC>\n";
$job_type=$_GET["job_type"];
$job_status=$_GET["job_status"];
$production=$_GET["production"];
$lower_limit=$_GET["lower_limit"];
$upper_limit=$_GET["upper_limit"];
$site=$_GET["site"];
$full_list=$_GET["full_list"];
$debug=$_GET["debug"];
if($full_list!=1){$full_list=0;}
include_once "local/monParams-FTS.php";
include_once("common/dbLib-FTS.php");
$db_name1=str_replace("_BOSS","",$db_name);;

if($job_type=="merge"){$production_plus="%merge%";}


//echo $contents;
//production
if($job_type=="prod"){
	echo "<h3><font color=blue>$production production</font> jobs executed at site: <font color=blue>$site</font> and ended with job exit status: <font color=blue>$job_status</font></h3> ";

	$filename="file_lists/".$production."-dbsunmerg.txt";
	$fd = fopen ($filename, "r");
	$contents = fread ($fd, filesize ($filename));
	fclose ($fd);

}

//merge
if($job_type=="merge"){
	echo "<h3><font color=blue>$production merge jobs</font> executed at site: <font color=blue>$site</font> and ended with job exit status: <font color=blue>$job_status</font></h3> ";
	$filename="file_lists/".$production."-dbsmerg.txt";
	$fd = fopen ($filename, "r");
	$contents = fread ($fd, filesize ($filename));
	fclose ($fd);
}
if(filesize ($filename)<10){echo "<h3><font color=red> The DBS input list is empty at the moment. Please retry shortly </font></h3>"; }

$last_change=filectime ($filename);
$in_dbs=0;
$error_cond=0;
$not_matched=0;
$N=0;
//echo $db_name." ".$DB_HOST.":".$DB_PORT." ".$DB_USER." ".$DB_PASS."<hr>";
$link = mysql_connect($DB_HOST.":".$DB_PORT,$DB_USER,$DB_PASS);


if(strpos($production_plus, 'EWKSoup0')>0 || strpos($production, 'EWKSoup0')>0){
$site_query_ce = str_replace ( " SCHED_edg", " ENDED_SCHED_edg",$site_query_ce);
$query="
SELECT 
ENDED_cmssw.TASK_ID,ENDED_cmssw.ID,TASK_name,LFN,ENDED_cmssw.SE_OUT,N_RUN 
from 
ENDED_cmssw,ENDED_JOB,ENDED_SCHED_edg 
WHERE 
ENDED_JOB.TASK_ID=ENDED_cmssw.TASK_ID  AND ENDED_JOB.ID=ENDED_cmssw.ID AND ENDED_SCHED_edg.TASK_ID=ENDED_cmssw.TASK_ID  AND ENDED_SCHED_edg.ID=ENDED_cmssw.ID AND 
ENDED_JOB.LOG_FILE like '$production_plus' 
$prod_success_job_cond  
$site_query_ce  
order by TASK_ID,ID";
}
//$records=getJobDetails($production_plus,$prod_failed_job_cond,$job_status,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);
else{
$query="
SELECT 
cmssw.TASK_ID,cmssw.ID,TASK_name,LFN,cmssw.SE_OUT,N_RUN 
from 
cmssw,JOB,SCHED_edg 
WHERE JOB.TASK_ID=cmssw.TASK_ID  AND JOB.ID=cmssw.ID AND SCHED_edg.TASK_ID=cmssw.TASK_ID  AND SCHED_edg.ID=cmssw.ID AND 
JOB.LOG_FILE like '$production_plus' 
$prod_success_job_cond  
$site_query_ce  
order by TASK_ID,ID";
}
//$records=getJobDetails_old($production_plus,$prod_failed_job_cond,$job_status,$job_type,$lower_limit,$upper_limit,$site_query_ce." ".$prod_success_job_cond,$query_id,$cnt_field,$group_par,$order_by);

//echo $query."<br>\n";
/*
   $result=mysql_db_query($db_name,$query);
   $num_rows=mysql_numrows($result);
   while($row=mysql_fetch_array($result)){
   $query1="SELECT job_index from st_job_attr where attr_value='$row[5]' and attr_class='run_numbers'";
   $result1=mysql_db_query($db_name1,$query1);
   $num_rows1=mysql_numrows($result1);
   if($num_rows1){
   $row1=mysql_fetch_array($result1);
//echo "run_number= $row[5]   job_index= $row1[0]<br>";
$query2="SELECT attr_value from st_job_attr where job_index='$row1[0]' and attr_class='output_files'";
$result2=mysql_db_query($db_name1,$query2);
$num_rows2=mysql_numrows($result2);
if($num_rows2){
$row2=mysql_fetch_array($result2);
//echo "LFN: $row2[0]<br>";
if(strpos($contents,$row2[0])){
$in_dbs+=1;
}
}

}

}

 */
//echo $db_name."<br>".$query;exit(0);
echo "<table>\n";
echo "<tr><th>N</th><th>Task_id</th><th>ID</th><th>Task_name</th><th>SE_OUT</th><th>IN DBS</th><th>LNF</th></tr>";
$result=mysql_db_query($db_name,$query);
$num_rows=mysql_numrows($result);
if ($num_rows==0){echo "<tr><td>Nessun record trovato</td></tr>";}
else{
	while($row=mysql_fetch_array($result)){
		$query1="SELECT job_index from st_job_attr where attr_value='$row[5]' and attr_class='run_numbers'";
		$result1=mysql_db_query($db_name1,$query1);
		$num_rows1=mysql_numrows($result1);
		if(($job_type=="prod"&&$num_rows1>=1)||($job_type=="merge"&&$num_rows1=2)){
			$row1=mysql_fetch_array($result1);
			if($job_type=="merge"){$row1=mysql_fetch_array($result1);}
			//echo "run_number= $row[5]   job_index= $row1[0]<br>";
			$query2="SELECT attr_value from st_job_attr where job_index='$row1[0]' and attr_class='output_files'";
			$result2=mysql_db_query($db_name1,$query2);
			$num_rows2=mysql_numrows($result2);
			if($num_rows2){
				$row2=mysql_fetch_array($result2);
				//echo "run_number= $row[5]   job_index= $row1[0]<br>";
				$search_str=substr($row2[0],2,strlen($row2[0]));
				if(strpos($contents,$search_str)){
					$in_dbs+=1;
					if($full_list==1){$N+=1;echo "<tr style=\"background-color: rgb(153, 255, 153);\"><td>$N</td><td>$row[0]</td><td>$row[1]</td><td>$row[2]</td><td>$row[4]</td><td>YES</td><td>$row2[0]</td></tr>";}
				}
				else{
					$N+=1;
					$not_matched+=1;
					echo "<tr style=\"background-color: rgb(255, 102, 102); \"><td>$N</td><td>$row[0]</td><td>$row[1]</td><td>$row[2]</td><td>$row[4]</td><td>NO</td><td>$row2[0]</td></tr>";
				}
			}
			else {
				$error_cond+=1;
				$N+=1;
				echo "<tr style=\"background-color: rgb(255, 102, 204); \"><td>$N</td><td>$row[0]</td><td>$row[1]</td><td>$row[2]</td><td>$row[4]</td><td>NO</td><td>Found $num_rows2 records in $db_name1  for run number=$row[5],  job_index=$row1[0]</td></tr>";
			}
		}
		else {
			$error_cond+=1;
			$N+=1;
			echo "<tr style=\"background-color: rgb(255, 102, 204); \"><td>$N</td><td>$row[0]</td><td>$row[1]</td><td>$row[2]</td><td>$row[4]</td><td>NO</td><td>Found $num_rows1 records in $db_name1  for run number=$row[5]</td></tr>";
		}

	}
}

$n_lines=substr_count ($contents, "\n");
echo "Last retrieval of the DBS information: ".date('j',$last_change)."/".date('m',$last_change)."/".date('Y',$last_change)." ".date('H',$last_change).":".date('i',$last_change).":".date('s',$last_change)."<br><br>";	
echo "<table>";
echo "<tr><td>Total Number of files in the DBS:</td><td align=right> $n_lines</td></tr>";
echo "<tr><td>Number of files from the BOSS DB matching the site:</td><td align=right> $num_rows</td></tr>";
echo "<tr><td>Number of files in DBS:</td><td align=right> $in_dbs</td></tr>"; 
echo "<tr><td>Number of files not in DBS:</td><td align=right> $not_matched</td></tr>"; 
echo "<tr><td>Number of cases with missing information:</td><td align=right> $error_cond</td></tr>"; 
echo "</table>";
echo "</br>";
if($full_list==0){echo "Click <a href=\"BOSS_DBvsDBS.php?job_type=$job_type&job_status=$job_status&lower_limit=$lower_limit&upper_limit=$upper_limit&production=$production&site=$site&full_list=1\">here</a> for the full matching and unmatching files list";}
if($full_list==1){echo "Click <a href=\"BOSS_DBvsDBS.php?job_type=$job_type&job_status=$job_status&lower_limit=$lower_limit&upper_limit=$upper_limit&production=$production&site=$site&full_list=0\">here</a> for the unmatching files list only";}

?>
