<?php
$tot[0]=0;$tot[1]=0;$tot[2]=0;$tot[3]=0;$tot[4]=0;$tot[5]=0;$tot[6]=0;
//$result=mysql_db_query($db_name,$query);

?>
<table>
<tr>
<td>
<img src="#" id="dest_ce_bar"><br>
</td>
<td>
<table>
<tr><th>Destination CE</th><th aligh=right>submitted</th><th>success</th><th>failed</th><th>aborted</th><th>running</th><th>scheduled</th></tr>
<?php
//while($row=mysql_fetch_array($result)){
while(!$records->EOF){ 
	$type		=	$records->Fields("LOGFILE_group")==0;
	$curr_ce	=	$records->Fields("destce");
	$sum_aborted	=	$records->Fields("sum_aborted");
	$sum_running	=	$records->Fields("sum_running");
	$sum_Scheduled	=	$records->Fields("sum_scheduled");
	$sum_success	=	$records->Fields("sum_success");
	$sum_failed	=	$records->Fields("sum_failed");
	$sum_tot 	=	$sum_aborted+$sum_running+$sum_Scheduled+$sum_success+$sum_failed;
	if($sum_Scheduled=="")
		$sum_Scheduled=0;
	$records->MoveNext();
	if($job_type!='prod' && $type==1){
		continue;
	}
	if($job_type!='merge' && $type==0){
		continue;
	}
	if($curr_ce!='')
		$curr_site=getSiteNamebyCE($curr_ce);
	else {
		$curr_site='NULL';
		$curr_ce='NULL';
	}
	
	if($curr_site=='')$curr_site='Null';
	echo "<tr><td>$curr_ce</td>";
?>
<?php 
$url_commn="job_type=$job_type&lower_limit=$lower_limit&upper_limit=$upper_limit&production=$production&site=$curr_site&merged_dataset=$merged_dataset"
?>
<td align=right>
<a href="boss_details.php?job_status=submitted&<?=$url_commn?>"><?=$sum_tot?></a></td>
<td align=right>
<a href="boss_details.php?job_status=success&<?=$url_commn?>"><?=$sum_success?></a></td>
<td align=right><a href="boss_details.php?job_status=failed&<?=$url_commn?>"><?=$sum_failed?></a></td>
<?php
$url_commn2="job_type=$job_type&lower_limit=$lower_limit&upper_limit=$upper_limit&production=$production&site=$curr_site&merged_dataset=$merged_dataset";
?>
	<td align=right><a href="boss_details.php?job_status=aborted&<?=$url_commn2?>"><?=$sum_aborted?></a></td>
	<td align=right><a href="boss_details.php?job_status=running&<?=$url_commn2?>"><?=$sum_running?></a></td>
	<td align=right><a href="boss_details.php?job_status=scheduled&<?=$url_commn2?>"><?=$sum_Scheduled?></a></td></tr>
<?php
	$total+=$sum_tot;
	$DC[]=getSiteNamebyCE($curr_ce);
	$DCE[]=$curr_ce;
	$N_sub[]=$sum_tot;
	$N_abo[]=$sum_aborted;
	$N_run[]=$sum_running;
	$N_sch[]=$sum_Scheduled;
	$N_suc[]=$sum_success;
	$N_fai[]=$sum_failed;
	$sum_tot_row+=$sum_tot;
	$sum_success_row+=$sum_success;
	$sum_aborted_row+=$sum_aborted;
	$sum_running_row+=$sum_running;
	$sum_failed_row+=$sum_failed;
	$sum_Scheduled_row+=$sum_Scheduled;

}
?>
<tr><td><b>Total</b></td><td align=right><?=$sum_tot_row?></td><td align=right><?=$sum_success_row?></td>
<td align=right><?=$sum_failed_row?></td><td align=right><?=$sum_aborted_row?></td><td align=right><?=$sum_running_row?></td>
<td align=right><?=$sum_Scheduled_row?></td></tr>
</table>
</td>
</tr>
</table>

<?php
$graph = new Graph(650,500,"auto");

// Use text X-scale so we can text labels on the X-axis
$graph->SetScale("textlin");
$graph->title->Set("Site distribution");

// Use built in font (don't need TTF support)
$graph->title->SetFont(FF_FONT1,FS_BOLD);

$graph->yaxis->title->Set("N. of jobs");
$graph->xaxis->title->Set("SITE");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD,20);

// Make the margin around the plot a little bit bigger then default
$graph->img->SetMargin(40,80,30,140);	



// Display every 1:th tickmark
$graph->xaxis->SetTextTickInterval(1);

// Setup the labels
$graph->xaxis->SetTickLabels($DC);
$graph->xaxis->SetLabelAngle(90);


$b1 = new BarPlot($N_sub);
$b1->SetLegend("submitted");
$b1->SetFillColor("blue");
$b1->value->Show();
$b1->value->SetFormat('%d');

// The order the plots are added determines who's ontop
$b2 = new BarPlot($N_abo);
$b2->SetLegend("aborted");
$b2->SetFillColor("darkred");
$b2->value->Show();
$b2->value->SetFormat('%d');

$b3 = new BarPlot($N_run);
$b3->SetLegend("running");
$b3->SetFillColor("orange");
$b3->value->Show();
$b3->value->SetFormat('%d');

$b4 = new BarPlot($N_sch);
$b4->SetLegend("scheduled");
$b4->SetFillColor("pink");
$b4->value->Show();
$b4->value->SetFormat('%d');

$b5 = new BarPlot($N_fai);
$b5->SetLegend("failed");
$b5->SetFillColor("red");
$b5->value->Show();
$b5->value->SetFormat('%d');

$b6 = new BarPlot($N_suc);
$b6->SetLegend("succes");
$b6->SetFillColor("darkgreen");
$b6->value->Show();
$b6->value->SetFormat('%d');

$y2bplot = new GroupBarPlot(array($b1,$b6,$b5,$b2,$b4,$b3));
$y2bplot->SetWidth(0.9);
// The order the plots are added determines who's ontop
$graph->Add($y2bplot);

// Finally output the  image
$graph->Stroke("plots/dest_ce_bar.png");
?>
<script>
document.getElementById("dest_ce_bar").src="plots/dest_ce_bar.png";
</script>

<?php 
$SUBT_x=array();
$count_y=array();
$c=0;
while(!$plots_record->EOF){ 
	$SUBT_x[$c]=$plots_record->Fields("SUBT");
	$plots_record->MoveNext();
	if($count_y[$c-1])
		$count_y[$c]=$count_y[$c-1]+1;
	else 
		$count_y[$c]=1;
	$c++;
}

//echo "fare il grafico: ".count($SUBT_x)." ".count($count_y)."<hr>";
// Create the graph. These two calls are always required
function TimeCallback($aVal) {
    return Date('d-M H:i',$aVal);
}

$graph = new Graph(850,350,"auto");    
$graph->SetMargin(70,40,20,100);
$graph->SetScale("textlin");
$graph->SetScale("intlin",0,'auto',$SUBT_x[0],$SUBT_x[count($SUBT_x)-1]);
// Create the linear plot
$lineplot=new LinePlot($count_y,$SUBT_x);
//$lineplot=new LinePlot($count_y);
$lineplot->SetColor("blue");
$lineplot->SetFillColor("blue");
$graph->xaxis->SetLabelFormatCallback('TimeCallback');
//$graph->xaxis->SetTextLabelInterval(2);
$graph->xaxis->SetLabelAngle(90);
//$graph->title->Set("");
$graph->yaxis->title->Set("Number of jobs");
//$graph->xaxis->title->Set("Date");

// Add the plot to the graph
$graph->Add($lineplot);

// Display the graph
$graph->Stroke("plots/red.png");
?>
<img src="plots/red.png">
<!--andamento del numero di eventi per giorno - START -->
<?php 
$SUBT_x=array();
$count_y=array();
$c=0;
while(!$plots_record_nevt->EOF){ 
	$nevt_y_temp=$plots_record_nevt->Fields("NEVT");
	$SUBT_x_temp=$plots_record_nevt->Fields("SUBT");
	$STATUS=$plots_record_nevt->Fields("STATUS");
	$plots_record_nevt->MoveNext();
	if($STATUS=='Done'){
		$SUBT_x[$c]=$SUBT_x_temp;
		if($count_y[$c-1])
			$count_y[$c]=$nevt_y_temp+$count_y[$c-1];
		else 
			$count_y[$c]=$nevt_y_temp;
		$c++;
	}
}


$graph = new Graph(850,350,"auto");    
$graph->SetMargin(70,40,20,100);
$graph->SetScale("textlin");
$graph->SetScale("intlin",0,'auto',$SUBT_x[0],$SUBT_x[count($SUBT_x)-1]);
// Create the linear plot
$lineplot=new LinePlot($count_y,$SUBT_x);
//$lineplot=new LinePlot($count_y);
$lineplot->SetColor("green");
$lineplot->SetFillColor("green");
$graph->xaxis->SetLabelFormatCallback('TimeCallback');
//$graph->xaxis->SetTextLabelInterval(2);
$graph->xaxis->SetLabelAngle(90);
//$graph->title->Set("");
$graph->yaxis->SetTitleMargin(50);
$graph->yaxis->title->Set("Number of events");
//$graph->xaxis->title->Set("Date");

// Add the plot to the graph
$graph->Add($lineplot);

// Display the graph
$graph->Stroke("plots/nevt.png");
?>
<img src="plots/nevt.png">
<!--andamento del numero di eventi per giorno - END-->
