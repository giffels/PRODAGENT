<!-- Destination CE	N. Jobs	Task exit code / START -->
<table>
<tr><th>Destination CE</td><td aligh=right>N. Jobs</td><td>Task exit code</td></tr>
<?php while(!$records[2]->EOF){ ?>
<tr><td><?=$records[2]->Fields("destce")?></a></td>
<td align=right><?=$records[2]->Fields("PRIMDATASET_count")?></td>
<td align=right><?=$records[2]->Fields("TASKEXIT")?></td>
</tr>
<?php 
$total+=$records[2]->Fields("PRIMDATASET_count");
$records[2]->MoveNext();
}
?>
<tr><td><b>Total</b></td><td align=right><?=$total?></td></tr>
</table>
<!-- Destination CE	N. Jobs	Task exit code / END -->

<?php
$DC_site=array();
$N_CNT_site=array();
$total=0;
?>
<table>
<tr>
<td><img src="" id="Site_distribution"></td>
<td>
<!-- Destination CE	N. Jobs / START -->
<table>
<tr><th>Destination CE</td><td aligh=right>N. Jobs</td></tr>

<?php 
while(!$records[0]->EOF){ 
	$curr_site=$records[0]->Fields("destce");
	?>

		<tr><td><?=$curr_site?></td><td align=right>
		<a href="boss_details.php?job_type=<?=$job_type?>&job_status=<?=$job_status?>&lower_limit=<?=$lower_limit?>&upper_limit=<?=$upper_limit?>&production=<?=$production?>&site=<?=$curr_site?>&merged_dataset=<?=$merged_dataset?>"><?=$records[0]->Fields("PRIMDATASET_count")?></a>
		</td></tr>
		<?php
		$total+=$records[0]->Fields("PRIMDATASET_count");
	$DC_site[]=$curr_site;
	$N_CNT_site[]=$records[0]->Fields("PRIMDATASET_count");
	$records[0]->MoveNext();
}
?>
<tr><td><b>Total</b></td><td align=right><?=$total?></td></tr>
</table>
<!-- Destination CE	N. Jobs / END -->
</td>
</tr>

<tr>
<td><img src="#" id="code"></td>
<td>
<!-- Task exit code	N. Jobs / START -->
<table>
<tr><th>Task exit code</td><td aligh=right>N. Jobs</td></tr>
<?php
$DC=array();
$N_CNT=array();
$total=0;
while(!$records[1]->EOF){ 
?>
<tr><td><?=$records[1]->Fields("TASKEXIT")?></td><td align=right><?=$records[1]->Fields("PRIMDATASET_count")?></td></tr>
<?php
$total+=$records[1]->Fields("PRIMDATASET_count");
$DC[]=$records[1]->Fields("TASKEXIT");
$N_CNT[]+=$records[1]->Fields("PRIMDATASET_count");
$records[1]->MoveNext();
}
?>
<tr><td><b>Total</b></td><td align=right><?=$total?></td></tr>
</table>
<!-- Task exit code	N. Jobs / END -->
</td>
</tr>
</table>



<?php
$graph = new PieGraph(500,300);
$graph->SetShadow();

$graph->title->Set("Failed Jobs CE distribution");
$graph->title->SetFont(FF_FONT1,FS_BOLD);

if(count($N_CNT_site)>0){
	$p1 = new PiePlot3D($N_CNT_site);
	$p1->ExplodeSlice(1);
	$p1->SetCenter(0.45);
	//$p1->SetLegends($gDateLocale->GetShortMonth());
	$p1->SetLegends($DC_site);
	//$p1->SetLegends($N_CNT);

	$graph->Add($p1);
}
else {
	$txt=new Text( "The pie chart is no logic in this context");
	$txt->Pos( 30,80);
	$txt->SetColor( "red");
	$graph->AddText( $txt);
}
$graph->Stroke("plots/dest_ce.png");
	?>
<script>
document.getElementById("Site_distribution").src="plots/dest_ce.png";
</script>

<?php
$graph = new PieGraph(500,300,"auto","10^10");
$graph->SetShadow();

$graph->title->Set("Failed Jobs exit code distribution");
$graph->title->SetFont(FF_FONT1,FS_BOLD);

if(count($N_CNT)>1){
	$p1 = new PiePlot3D($N_CNT);
	$p1->ExplodeSlice(1);
	$p1->SetCenter(0.45);
	//$p1->SetLegends($gDateLocale->GetShortMonth());
	$p1->SetLegends($DC);

	$graph->Add($p1);
}
else {
	$txt=new Text( "The pie chart is no logic in this context");
	$txt->Pos( 30,80);
	$txt->SetColor( "red");
	$graph->AddText( $txt);
}
$graph->Stroke("plots/code.png");
?>
<script>
document.getElementById("code").src="plots/code.png";
</script>
<?php
//number of retry
$DC=array();
$N_CNT=array();
$total=0;
?>

<img src="" id="Resubmissions">
<table>
<tr>
</td>
<tr><th>TASK ID</td><td aligh=right>N. resubmission</td></tr>
<?php while(!$records[3]->EOF){ 
if($records[3]->Fields("ID_count")>1){
?>
<tr><td><?=$records[3]->Fields("TASKID")?></td><td align=right>
<a href="boss_details.php?job_type=<?=$job_type?>&job_status=<?=$job_status?>&lower_limit=<?=$lower_limit?>&upper_limit=<?=$upper_limit?>&production=<?=$production?>&id=<?=$records[3]->Fields("TASKID")?>"><?=$records[3]->Fields("ID_count")?></a>
<!--<?=$records[3]->Fields("ID_count")?>-->
</td></tr>
<?php
$total+=$records[3]->Fields("ID_count");
$DC[]=$records[3]->Fields("TASKID");
$N_CNT[]=$records[3]->Fields("ID_count");
	}
	$records[3]->MoveNext();
}
?>
<tr><td><b>Total</b></td><td align=right><?=$total?></td></tr>
</table>
<?php
$graph = new Graph(1200,300,"auto");

// Use text X-scale so we can text labels on the X-axis
$graph->SetScale("textlin");
$graph->title->Set("Resubmissions distribution > 1");
$graph->img->SetMargin(60,40,30,70);	

// Use built in font (don't need TTF support)
$graph->title->SetFont(FF_FONT1,FS_BOLD);

$graph->yaxis->title->Set("N. resubmissions");
$graph->xaxis->title->Set("TASK ID");
$graph->xaxis->title->SetMargin(20);
$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);

// Make the margin around the plot a little bit bigger then default



// Display every 1:th tickmark
$graph->xaxis->SetTextTickInterval(1);

// Setup the labels
$graph->xaxis->SetTickLabels($DC);
$graph->xaxis->SetLabelAngle(90);

if(count($N_CNT)>0){
$b1 = new BarPlot($N_CNT);
$b1->SetLegend($job_status);
$b1->SetFillColor("orange");
//$b1->SetAbsWidth(30);
// The order the plots are added determines who's ontop
$graph->Add($b1);

// Finally output the  image
$graph->Stroke("plots/detai_resub.png");
?>
<script>
document.getElementById("Resubmissions").src="plots/detai_resub.png";
</script>
<?php }?>

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
