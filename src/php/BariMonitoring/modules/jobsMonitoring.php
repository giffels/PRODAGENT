<?php 
include_once("common/dbLib-FTS.php");
include_once("local/monParams-FTS.php");
$site_array=checkSiteNamebyCE($production);
?>
<table border=1>
<tr><td colspan=2>
<font size +2>Site:</font><select name=site onChange="this.form.submit()">
<?php
//row1cell2,3
			if($site=="all"){echo "<option selected>all";}else{echo "<option>all";}
				for($i=0;$i<count($site_array);$i++){
				if($site==$site_array[$i]){echo "<option selected>$site_array[$i]";}
				else{echo "<option>$site_array[$i]";}
				}
?>
</select>
</td><td colspan=10>
<font size +2>Production:</font><select name=production onChange="javascript:if(this.form.production.value=='ALL')if(this.form.site.value=='all'){alert('Please: select a site');return 0};this.form.submit()">
<?php
for($i=0;$i<count($production_list);$i++){
	if($production==$production_list[$i]){echo "<option selected>$production_list[$i]";}
	else{echo "<option>$production_list[$i]";}			
}
?>
</select>
/
<?php 
$records=get_stream($production);
?>
<select name="merged_dataset" onChange="this.form.submit()">
<option value="ALL">ALL
<?php
$stream_merge=array();
if($records)
while(!$records->EOF){ 
	$processed			=	$records->Fields("processed");
	$TASK_NAME_stream		=	$records->Fields("TASK_NAME");
	$records->MoveNext();
	$stream_merge[$processed]	.=	$TASK_NAME_stream.",";
}
foreach($stream_merge as $key => $value){
	if($key==$merged_dataset)
		echo "<option value=\"$key\" selected>$key</option>";
	else{
		echo "<option value=\"$key\">$key</option>";
	}
}?>
</select>

</td></tr>

<?php
$now=time();
$str= date('Y',$now)."-".date('m',$now)."-".date('d',$now)." 00:00:00";
$timestamp = strtotime($str);
//$today_time_stamp=floor(time()/86400)*86400;
//echo "timestamp=$timestamp    now=$now today time stamp:$today_time_stamp" ;
$today_time_stamp=$timestamp;
?>
<tr><th width=250 colspan=2>jobs </th><th width=100> before<br><?=date('j',$today_time_stamp-86400*6)."/".date('m',$today_time_stamp-86400*6)?></th>
<th width=100><?=date('j',$today_time_stamp-86400*6)."/".date('m',$today_time_stamp-86400*6)?></th>
<th width=100><?=date('j',$today_time_stamp-86400*5)."/".date('m',$today_time_stamp-86400*5)?></th>
<th width=100><?=date('j',$today_time_stamp-86400*4)."/".date('m',$today_time_stamp-86400*4)?></th>
<th width=100><?=date('j',$today_time_stamp-86400*3)."/".date('m',$today_time_stamp-86400*3)?></th>
<th width=100><?=date('j',$today_time_stamp-86400*2)."/".date('m',$today_time_stamp-86400*2)?></th>
<th width=100><?=date('j',$today_time_stamp-86400*1)."/".date('m',$today_time_stamp-86400*1)?></th>
<th width=100><?=date('j')."/".date('m')?></th><th width=100 style="background-color: lightgray;">total</th><th width=100>last hour</th></tr>
<?php

$tmeno6=($today_time_stamp-86400*6);
$tmeno5=($today_time_stamp-86400*5);
$tmeno4=($today_time_stamp-86400*4);
$tmeno3=($today_time_stamp-86400*3);
$tmeno2=($today_time_stamp-86400*2);
$tmeno1=($today_time_stamp-86400*1);
$t0=($today_time_stamp);
$tpiu1=($today_time_stamp+86400*1);
$sb=array();
$rn=array();
$sh=array();
$ab=array();

$tl=array();
$tl[0]=0;$tl[1]=$tmeno6;$tl[2]=$tmeno5;$tl[3]=$tmeno4;$tl[4]=$tmeno3;$tl[5]=$tmeno2;$tl[6]=$tmeno1;$tl[7]=$t0;$tl[8]=$tpiu1;$tl[9]=$now;
$cn[0]=0;$cn[1]=0;$cn[2]=0;$cn[3]=0;$cn[4]=0;$cn[5]=0;$cn[6]=0;$cn[7]=0;$cn[8]=0;

// Production Jobs submitted,  flheduled, running, aborted
$sb['prod'][0]=0;$sb['prod'][1]=0;$sb['prod'][2]=0;$sb['prod'][3]=0;$sb['prod'][4]=0;$sb['prod'][5]=0;$sb['prod'][6]=0;$sb['prod'][7]=0;$sb['prod'][8]=0;$sb['prod'][9]=0;
$rn['prod'][0]=0;$rn['prod'][1]=0;$rn['prod'][2]=0;$rn['prod'][3]=0;$rn['prod'][4]=0;$rn['prod'][5]=0;$rn['prod'][6]=0;$rn['prod'][7]=0;$rn['prod'][8]=0;$rn['prod'][9]=0;
$sh['prod'][0]=0;$sh['prod'][1]=0;$sh['prod'][2]=0;$sh['prod'][3]=0;$sh['prod'][4]=0;$sh['prod'][5]=0;$sh['prod'][6]=0;$sh['prod'][7]=0;$sh['prod'][8]=0;$sh['prod'][9]=0;
$ab['prod'][0]=0;$ab['prod'][1]=0;$ab['prod'][2]=0;$ab['prod'][3]=0;$ab['prod'][4]=0;$ab['prod'][5]=0;$ab['prod'][6]=0;$ab['prod'][7]=0;$ab['prod'][8]=0;$ab['prod'][9]=0;
$sc['prod'][0]=0;$sc['prod'][1]=0;$sc['prod'][2]=0;$sc['prod'][3]=0;$sc['prod'][4]=0;$sc['prod'][5]=0;$sc['prod'][6]=0;$sc['prod'][7]=0;$sc['prod'][8]=0;$sc['prod'][9]=0;
$fl['prod'][0]=0;$fl['prod'][1]=0;$fl['prod'][2]=0;$fl['prod'][3]=0;$fl['prod'][4]=0;$fl['prod'][5]=0;$fl['prod'][6]=0;$fl['prod'][7]=0;$fl['prod'][8]=0;$fl['prod'][9]=0;

$sb['merge'][0]=0;$sb['merge'][1]=0;$sb['merge'][2]=0;$sb['merge'][3]=0;$sb['merge'][4]=0;$sb['merge'][5]=0;$sb['merge'][6]=0;$sb['merge'][7]=0;$sb['merge'][8]=0;$sb['merge'][9]=0;
$rn['merge'][0]=0;$rn['merge'][1]=0;$rn['merge'][2]=0;$rn['merge'][3]=0;$rn['merge'][4]=0;$rn['merge'][5]=0;$rn['merge'][6]=0;$rn['merge'][7]=0;$rn['merge'][8]=0;$rn['merge'][9]=0;
$sh['merge'][0]=0;$sh['merge'][1]=0;$sh['merge'][2]=0;$sh['merge'][3]=0;$sh['merge'][4]=0;$sh['merge'][5]=0;$sh['merge'][6]=0;$sh['merge'][7]=0;$sh['merge'][8]=0;$sh['merge'][9]=0;
$ab['merge'][0]=0;$ab['merge'][1]=0;$ab['merge'][2]=0;$ab['merge'][3]=0;$ab['merge'][4]=0;$ab['merge'][5]=0;$ab['merge'][6]=0;$ab['merge'][7]=0;$ab['merge'][8]=0;$ab['merge'][9]=0;
$sc['merge'][0]=0;$sc['merge'][1]=0;$sc['merge'][2]=0;$sc['merge'][3]=0;$sc['merge'][4]=0;$sc['merge'][5]=0;$sc['merge'][6]=0;$sc['merge'][7]=0;$sc['merge'][8]=0;$sc['merge'][9]=0;
$fl['merge'][0]=0;$fl['merge'][1]=0;$fl['merge'][2]=0;$fl['merge'][3]=0;$fl['merge'][4]=0;$fl['merge'][5]=0;$fl['merge'][6]=0;$fl['merge'][7]=0;$fl['merge'][8]=0;$fl['merge'][9]=0;

$cnt_field="
INTERVAL(SUBT,$tmeno6,$tmeno5,$tmeno4,$tmeno3,$tmeno2,$tmeno1,$t0,$tpiu1) as days, 
sum(STATUS='running') as sum_running,
sum(STATUS='scheduled') as sum_scheduled,
sum(STATUS='aborted') as sum_aborted,
sum(STATUS='success') as sum_success,
sum(STATUS='failed') as sum_failed,
LOGFILE REGEXP 'mergejob' as LOGFILE_group
";
$group_par="GROUP BY  days,LOGFILE_group";
$order_by=" order by LOGFILE_group,days";

$records="false";

if($DB_type==2)
$records=getJobDetails_allStatus($production_plus,$prod_failed_job_cond,$job_status,$job_type,$lower_limit,$upper_limit,$ended_site_query_ce,$query_id,$cnt_field,$group_par,$order_by);
else
$records=getJobDetails_allStatus_old($production,$prod_failed_job_cond,$job_status,$job_type,$lower_limit,$upper_limit,$ended_site_query_ce,$query_id,$cnt_field,$group_par,$order_by);
if($records==""){
	echo "<h3>this Production isn't accessible</h3>
	you can try to fix <a href='dokument.php'>Production config file</a>";
	exit(0);
}
while(!$records->EOF){
		if($records->Fields("LOGFILE_group")==0){
			$type_prod='prod';
		}
		if($records->Fields("LOGFILE_group")==1){
			$type_prod='merge';
		}
		$days=$records->Fields("days");
		$sum_running=$records->Fields("sum_running");
		$sum_scheduled=$records->Fields("sum_scheduled");
		$sum_aborted=$records->Fields("sum_aborted");
		$sum_failed=$records->Fields("sum_failed");
		$sum_success=$records->Fields("sum_success");
		$sum_submitted=$sum_running+$sum_scheduled+$sum_aborted+$sum_failed+$sum_success;
		
		$sb[$type_prod][$days]=$sum_submitted;
		$sb[$type_prod][8]+=$sum_submitted;		
		$rn[$type_prod][$days]=$sum_running;
		$rn[$type_prod][8]+=$sum_running;		
		$sh[$type_prod][$days]=$sum_scheduled;
		$sh[$type_prod][8]+=$sum_scheduled;
		$ab[$type_prod][$days]=$sum_aborted;
		$ab[$type_prod][8]+=$sum_aborted;		
		$fl[$type_prod][$days]=$sum_failed;
		$fl[$type_prod][8]+=$sum_failed;		
		$sc[$type_prod][$days]=$sum_success;
		$sc[$type_prod][8]+=$sum_success;		
$records->MoveNext();
}


// last hour

$cnt_field="
INTERVAL(SUBT,".($now-3600).",$now) as days, 
sum(STATUS='running') as sum_running,
sum(STATUS='scheduled') as sum_scheduled,
sum(STATUS='aborted') as sum_aborted,
sum(STATUS='success') as sum_success,
sum(STATUS='failed') as sum_failed,
LOGFILE REGEXP 'mergejob' as LOGFILE_group
";
$group_par="GROUP BY  days,LOGFILE_group";
$order_by=" order by LOGFILE_group,days";
$records="false";
//if(strpos($production, 'EWKSoup0')>0)
if($DB_type==2)
$records=getJobDetails_allStatus($production_plus,$prod_failed_job_cond,$job_status,$job_type,$lower_limit,$upper_limit,$ended_site_query_ce,$query_id,$cnt_field,$group_par,$order_by);
else
$records=getJobDetails_allStatus_old($production,$prod_failed_job_cond,$job_status,$job_type,$lower_limit,$upper_limit,$ended_site_query_ce,$query_id,$cnt_field,$group_par,$order_by);
while(!$records->EOF){
		if($records->Fields("LOGFILE_group")==0){
			$type_prod='prod';
		}
		if($records->Fields("LOGFILE_group")==1){
			$type_prod='merge';
		}
		$days=$records->Fields("days");
		$sum_running=$records->Fields("sum_running");
		$sum_scheduled=$records->Fields("sum_scheduled");
		$sum_aborted=$records->Fields("sum_aborted");
		$sum_failed=$records->Fields("sum_failed");
		$sum_success=$records->Fields("sum_success");
		$sum_submitted=$sum_running+$sum_scheduled+$sum_aborted+$sum_failed+$sum_success;

		$records->MoveNext();
		if($days==1){
			$sb[$type_prod][9]+=$sum_submitted;		
			$rn[$type_prod][9]+=$sum_running;		
			$sh[$type_prod][9]+=$sum_scheduled;
			$ab[$type_prod][9]+=$sum_aborted;		
			$fl[$type_prod][9]+=$sum_failed;		
			$sc[$type_prod][9]+=$sum_success;		
		}

}


output_row("prod","submitted",$sb['prod'],$tl,$production,$production_plus,$site);
output_row("prod","scheduled",$sh['prod'],$tl,$production,$production_plus,$site);
output_row("prod","running",$rn['prod'],$tl,$production,$production_plus,$site);
output_row("prod","aborted",$ab['prod'],$tl,$production,$production_plus,$site);
output_row("prod","failed",$fl['prod'],$tl,$production,$production_plus,$site);
output_row("prod","success",$sc['prod'],$tl,$production,$production_plus,$site);

output_row("merge","submitted",$sb['merge'],$tl,$production,$production_plus,$site);
output_row("merge","scheduled",$sh['merge'],$tl,$production,$production_plus,$site);
output_row("merge","running",$rn['merge'],$tl,$production,$production_plus,$site);
output_row("merge","aborted",$ab['merge'],$tl,$production,$production_plus,$site);
output_row("merge","failed",$fl['merge'],$tl,$production,$production_plus,$site);
output_row("merge","success",$sc['merge'],$tl,$production,$production_plus,$site);
?>
</table>

<?php
function output_row($job_type,$job_status,$cnn,$tll,$production,$production_plus,$site){
	$merged_dataset=$GLOBALS["merged_dataset"];
	$background="background-color: lightgray;";
	if($job_status=="submitted"){
		?>

<td rowspan=6><?=$job_type?><br>
<!--
<a href="BOSS_DBvsDBS.php?job_type=<?=$job_type?>&job_status=success&lower_limit=<?=$tll[0]?>&upper_limit=<?=$tll[8]?>&production=<?=$production?>&site=<?=$site?>">
<font size=-2>BOSS-DBS</font></a>--></td>
			<?php }	elseif($job_status=="success"){
				echo "<tr class=\"solid-green\">";$background="background-color: rgb(153, 255, 153);";}
	else {echo "<tr>";}
	if($job_status=="success"){
	if($cnn[8]!=0)
echo "<td>
<a href=\"boss_details.php?job_type=$job_type&job_status=$job_status&lower_limit=$tll[0]&upper_limit=$tll[8]&production=$production&site=$site\">$job_status</a><!--<a href=\"simple_statistics.php?job_type=$job_type&job_status=$job_status&production=$production&site=$site\"> (plots)</a>--></td>";
	else
		echo "<td>&nbsp</td>";
	}
	else {
	if($cnn[8]!=0)
echo "<td><a href=\"boss_details.php?job_type=$job_type&job_status=$job_status&lower_limit=$tll[0]&upper_limit=$tll[8]&production=$production&site=$site\">$job_status</a></td>";
	else
		echo "<td>$job_status</td>";
}
$url_details_common="job_type=$job_type&job_status=$job_status&production=$production&production_plus=$production_plus&site=$site&merged_dataset=$merged_dataset";
for($c=0;$c<count($tll);$c++){
	echo "<td align=right>";
	if($c!=8 && $c!=9){
		if($cnn[$c]==0)
			echo $cnn[$c];
		else
			echo "<a href=\"details.php?lower_limit=".$tll[$c]."&upper_limit=".$tll[$c+1]."&$url_details_common\">".$cnn[$c]."</a></td>";
	}
	if($c==8){
		if($cnn[8]==0)
			echo $cnn[$c+1];
		else
			echo "<a href=\"details.php?lower_limit=$tll[0]&upper_limit=$tll[8]&$url_details_common\">$cnn[8]</a>";
	}
	if($c==9){
		if($cnn[9]==0)
			echo $cnn[9];
		else
			echo "<a href=\"details.php?lower_limit=".($tll[9]-3600)."&upper_limit=$tll[9]&$url_details_common\">".$cnn[9]."</a>";
	}
	echo "</td>";
}
echo "</tr>";	
}
?>
