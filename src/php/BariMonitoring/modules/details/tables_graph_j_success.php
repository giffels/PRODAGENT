<table>
<tr>
<td>
<img src="#" id="dest_ce_bar">
</td>
<td>
<table>
<tr><th>Destination CE</td><td aligh=right>N. Jobs</td></tr>
<?php 
while(!$records[0]->EOF){ 
	$curr_ce=$records[0]->Fields("destce");
	if($curr_ce=='')
		$curr_ce='Submission Problem';
	if($curr_site=='')$curr_site='NULL';
	$curr_site=getSiteNamebyCE($curr_ce);
?>
<tr><td>
<?php if($curr_ce=='Submission Problem'){?>
<font color="red"><?=$curr_ce?></font>
<?php } else{?>
<?=$curr_ce?>
<?php }?>
</td><td align=right>
<?php if($curr_ce=='Submission Problem'){?>
<font color="red"><?=$records[0]->Fields("PRIMDATASET_count")?></font>
<?php } else{?>
<a href="boss_details.php?job_type=<?=$job_type?>&job_status=<?=$job_status?>&lower_limit=<?=$lower_limit?>&upper_limit=<?=$upper_limit?>&production=<?=$production?>&site=<?=$curr_ce?>&merged_dataset=<?=$merged_dataset?>">
<?=$records[0]->Fields("PRIMDATASET_count")?>
</a>
<?php }?>
</td></tr>
<?php
	$total+=$records[0]->Fields("PRIMDATASET_count");
	$DC[]=$curr_site;
	$DCE[]=$curr_ce;
	$N_CNT[]=$records[0]->Fields("PRIMDATASET_count");
	$N_EVT[]=$records[0]->Fields("NEVT_sum");
	$total_EVT+=$records[0]->Fields("NEVT_sum");
	$records[0]->MoveNext();
}
?>
<tr><td><b>Total</b></td><td align=right><?=$total?></td></tr>
</table>
</td>
</tr>
<?php if($total_EVT>0){?>
<tr>
<td>
<img src="#" id="dest_ce_evt"><br>
</td>
<td>
<table>
<tr><th>Destination CE</th><th>N. Events</th></tr>
<?php
for($i=0;$i<count($DC);$i++){?>
	<tr><td>
		<?=$DC[$i]?></td><td align=right><?=$N_EVT[$i]?>
	</td></tr>
<?php }?>
<tr><td>Total</td><td align=right><?=$total_EVT?></td></tr>
</table>

</td>
</tr>
<?php }?>
</table>

<?php
$graph = new Graph(800,400,"auto");
// Use text X-scale so we can text labels on the X-axis
$graph->SetScale("textlin");
$graph->title->Set("CE distribution");

// Use built in font (don't need TTF support)
$graph->title->SetFont(FF_FONT1,FS_BOLD);

$graph->yaxis->title->Set("N. of jobs");
$graph->yaxis->title->SetMargin(20);
$graph->xaxis->title->Set("Sites");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);

// Make the margin around the plot a little bit bigger then default
$graph->img->SetMargin(60,40,30,140);	

// Display every 1:th tickmark
$graph->xaxis->SetTextTickInterval(1);

// Setup the labels
$graph->xaxis->SetTickLabels($DC);
$graph->xaxis->SetLabelAngle(90);


$b1 = new BarPlot($N_CNT);
$b1->SetLegend($job_status);
$b1->SetFillColor("blue");
$b1->SetAbsWidth(30);
$b1->value->Show();
$b1->value->SetFormat('%d');
// The order the plots are added determines who's ontop
$graph->Add($b1);

// Finally output the  image
$graph->Stroke("plots/dest_ce_bar.png");
// events distribution
?>
<script>
document.getElementById("dest_ce_bar").src="plots/dest_ce_bar.png";
</script>




<?php
$graph = new Graph(800,400,"auto");

// Use text X-scale so we can text labels on the X-axis
$graph->SetScale("textlin");
$graph->title->Set("Events per CE");

// Use built in font (don't need TTF support)
$graph->title->SetFont(FF_FONT1,FS_BOLD);

$graph->yaxis->title->Set("N.of events");
$graph->yaxis->title->SetMargin(50);
$graph->xaxis->title->Set("Sites");
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);

// Make the margin around the plot a little bit bigger then default
$graph->img->SetMargin(90,40,30,140);	

// Display every 1:th tickmark
$graph->xaxis->SetTextTickInterval(1);

// Setup the labels
$graph->xaxis->SetTickLabels($DC);
$graph->xaxis->SetLabelAngle(90);

$b1 = new BarPlot($N_EVT);
$b1->SetLegend($job_status);
$b1->SetFillColor("yellow");
$b1->SetAbsWidth(30);
$b1->value->SetFormat('%d');
$b1->value->Show();
// The order the plots are added determines who's ontop
$graph->Add($b1);
// Finally output the  image
$graph->Stroke("plots/dest_ce_evt.png");
?>
<script>
document.getElementById("dest_ce_evt").src="plots/dest_ce_evt.png";
</script>

<hr>
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
if($plots_record_nevt!=''){
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
<?php }?>
<!--andamento del numero di eventi per giorno - END-->
