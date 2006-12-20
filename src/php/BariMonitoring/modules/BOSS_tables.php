<table>
<TR class="hd"><TD colspan=13 align=center>Prodagent</TD>
<tr class="hd2"><td>Nrec</td><td>task_name</td><td>task_exit</td><td>task_id</td><td>id</td><td>Job name</td>
<td>comment</td><td>exec host</td><td>SE out</td><td>sub time</td><td>start time</td><td>stop time</td><td>SCHED_ID</td>
<?php


for ($i=0;$i<count($task_name);$i++){			
	if($DB_type==2){
		if($task_name[$i]=="Success")$output="Successes";
		if($task_name[$i]=="Failed")$output="Failures";
	}
	if($job_status=="success"||$job_status=="failed"){
?>
<tr onmouseover="this.className='on'" onmouseout="this.className='off'"><td><?=$i?></td><td>&nbsp;<?=$task_name[$i]?></td>
<?php
if($DB_type==2){
		if($task_name[$i]=="Success")
			echo "<td>&nbsp;$task_exit[$i]</td><td><a href=\"http://$PAserver/~prodagent/$prodarea_alias/$output/$retrive_file[$i].tar.gz\">$task_id[$i]</a></td>";
		if($task_name[$i]=="Failed"){
			preg_match("/\d{6,}/",$retrive_file[$i],$matches);
			$id=$matches[0];
			echo "<td>&nbsp;$task_exit[$i]</td><td><a href=\"http://$PAserver/~prodagent/$prodarea_alias_failed/$retrive_file[$i]-cache/JobTracking/Failed/\">$task_id[$i]</a></td>";
			}
}
else
                echo "<td>&nbsp;$task_exit[$i]</td><td><a href=\"http://$PAserver/~prodagent/$prodarea_alias/JobTracking/BossJob_$task_id[$i]_1/Submission_$id[$i]\">$task_id[$i]</a></td>";

		//echo "<td>&nbsp;$task_exit[$i]</td><td>$task_id[$i]</td>";
		echo "<td>$id[$i]</td><td>$job_name[$i]</td><td>&nbsp;$comment[$i]</td><td>&nbsp;$exec_host[$i]</td><td>&nbsp;$se_out[$i]</td>";
		echo "<td>".date("d/m/y G:i:s",$sub_time[$i])."</td><td>".date("d/m/y G:i:s",$start_t[$i])."</td>";
		echo "<td>".date("d/m/y G:i:s",$stop_t[$i])."</td><td><font size=-2>$sched_id[$i]</font></td>\n";
	}
	if($job_status=="submitted"){
		if($task_exit[$i]=="0"){?>

<tr onmouseover="this.className='on'" onmouseout="this.className='off'">
<td><?=$i?></td>
<td>&nbsp;<?=$task_name[$i]?></td>
<td>&nbsp;<?=$task_exit[$i]?></td>
<td><?=$task_id[$i]?></td>
<td><?=$id[$i]?></td>
<td><?=$job_name[$i]?></td>
<td>&nbsp;<?=$comment[$i]?></td>
<td>&nbsp;<?=$exec_host[$i]?></td>
<td>&nbsp;<?=$se_out[$i]?></td>
<td><?=date("d/m/y G:i:s",$sub_time[$i])?></td><td><?=date("d/m/y G:i:s",$start_t[$i])?></td>
<td><?=date("d/m/y G:i:s",$stop_t[$i])?></td><td><font size=-2><?=$sched_id[$i]?></font></td>

<?php }elseif($task_name[$i]=="Failed"){?>

<tr onmouseover="this.className='on'" onmouseout="this.className='off'"><td>
<?=$i?></td>
<td>&nbsp;<?=$task_name[$i]?></td>
<td>&nbsp;<?=$task_exit[$i]?></td>
<!--<td><a href="http://$PAserver/~prodagent/$prodarea_alias/JobTracking/BossJob_<?=$task_id[$i]?>_1/Submission_<?=$id[$i]?>"><?=$task_id[$i]?></a></td>-->
<?php
if($DB_type==2){
		echo "<td><a href=\"http://$PAserver/~prodagent/$prodarea_alias/$output\">$task_id[$i]</a></td>";
}
else
                echo "<td><a href=\"http://$PAserver/~prodagent/$prodarea_alias/JobTracking/BossJob_$task_id[$i]_1/Submission_$id[$i]\">$task_id[$i]</a></td>";
?>
<!--<td><a href="http://$PAserver/~prodagent/<?=$prodarea_alias?>/<?=$output?>"><?=$task_id[$i]?></a></td>-->
<td><?=$id[$i]?></td>
<!--<td><a href="../$prodarea_alias/JobCreator/<?=$job_name[$i]?>-cache"><?=$job_name[$i]?></a></td>-->
<td><?=$job_name[$i]?></td>
<td>&nbsp;<?=$comment[$i]?></td>
<td>&nbsp;<?=$exec_host[$i]?></td>
<td>&nbsp;<?=$se_out[$i]?></td><td>
<?=date("d/m/y G:i:s",$sub_time[$i])?></td>
<td><?=date("d/m/y G:i:s",$start_t[$i])?></td>
<td><?=date("d/m/y G:i:s",$stop_t[$i])?></td><td><font size=-2><?=$sched_id[$i]?></font></td>
<?php }	else {?>
<tr onmouseover="this.className='on'" onmouseout="this.className='off'"><td><?=$i?></td>
<td>&nbsp;<?=$task_name[$i]?></td>
<td>&nbsp;<?=$task_exit[$i]?></td>
<td><?=$task_id[$i]?></td>
<td><?=$id[$i]?></td>
<?php
			//echo "<td><a href=\"http://$PAserver/~prodagent/$prodarea_alias/JobCreator/$job_name[$i]-cache\">$job_name[$i]</a></td><td>&nbsp;$comment[$i]</td>";
			echo "<td>$job_name[$i]</td><td>&nbsp;$comment[$i]</td>";
			echo "<td>&nbsp;$exec_host[$i]</td><td>&nbsp;$se_out[$i]</td>";
			echo "<td>".date("d/m/y G:i:s",$sub_time[$i])."</td><td>".date("d/m/y G:i:s",$start_t[$i])."</td>";
			echo "<td>".date("d/m/y G:i:s",$stop_t[$i])."</td><td><font size=-2>$sched_id[$i]</font></td>\n";
		}
	}			

	if($job_status=="running"||$job_status=="scheduled"){
		?>
			<tr onmouseover="this.className='on'" onmouseout="this.className='off'"><td><?=$i?></td><td>&nbsp;<?=$task_name[$i]?></td><td>&nbsp;<?=$task_exit[$i]?></td><td>&nbsp;<?=$task_id[$i]?></td><td><?=$id[$i]?></td>
			<?php
			//echo "<td><a href=\"http://$PAserver/~prodagent/$prodarea_alias/JobCreator/$job_name[$i]-cache\">$job_name[$i]</a></td><td>&nbsp;$comment[$i]</td>";
			echo "<td>&nbsp;$job_name[$i]</td><td>&nbsp;$comment[$i]</td>";
		echo "<td>&nbsp;$exec_host[$i]</td><td>&nbsp;$se_out[$i]</td><td>".date("d/m/y G:i:s",$sub_time[$i])."</td><td>".date("d/m/y G:i:s",$start_t[$i])."</td>";
		echo "<td>".date("d/m/y G:i:s",$stop_t[$i])."</td><td><font size=-2>$sched_id[$i]</font></td>\n";
	}			

	if($job_status=="aborted"){
		?>
<tr onmouseover="this.className='on'" onmouseout="this.className='off'"><td><?=$i?></td><td>&nbsp;<?=$task_name[$i]?></td><td>&nbsp;<?=$task_exit[$i]?></td><td><?=$task_id[$i]?></td><td><?=$id[$i]?></td><td><?=$job_name[$i]?></td>
<?php
echo "<td>&nbsp;$comment[$i]</td><td>&nbsp;$exec_host[$i]</td><td>&nbsp;$se_out[$i]</td><td>".date("d/m/y G:i:s",$sub_time[$i])."</td>";
echo "<td>".date("d/m/y G:i:s",$start_t[$i])."</td><td>".date("d/m/y G:i:s",$stop_t[$i])."</td><td><font size=-2>$sched_id[$i]</font></td>\n";
	}

	$mod_sched_id = str_replace("_","_5f",$sched_id[$i]);			
	$mod_sched_id = substr($mod_sched_id, 0, strlen($sched_id[$i])+1);			
	//$mod_sched_id=$sched_id[$i];
}
?>
</table>
