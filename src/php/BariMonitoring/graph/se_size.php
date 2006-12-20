<?php
$src_jpg='/home/prodagent/public_html/jpgraph/src/';
include_once ($src_jpg."jpgraph.php");
include_once ($src_jpg."jpgraph_line.php");
include_once ($src_jpg."jpgraph_bar.php");

//$datay=array(12,8,19,3,10,5);
//$datax=array("Jan","Feb","Mar","Apr","May","Jun");
/*for($c=0;$c<count($datax);$c++){
	echo $datax[$c]." ".$datay[$c]."<hr>";
}*/

// Create the graph. These two calls are always required
$graph = new Graph(800,300,"auto","10^10");    
$graph->SetScale("textlin");

// Add a drop shadow
$graph->SetShadow();

// Adjust the margin a bit to make more room for titles
$graph->img->SetMargin(40,30,20,80);

// Create a bar pot
$bplot = new BarPlot($datay);
$bplot = new LinePlot($datay);

// Adjust fill color
//$bplot->SetFillColor('white');
//$bplot->SetColor('white');
$bplot->SetFillGradient('white','darkgreen');
//$bplot->SetWidth(1.0);
$graph->Add($bplot);

// Setup the titles
$graph->title->Set("SE SIZE");
$graph->SetBackgroundGradient('darkred','yellow',GRAD_HOR,BGRAD_PLOT);
$graph->xaxis->title->Set("Time (sec)");
$graph->xaxis->SetTitlemargin(40); 
$graph->yaxis->title->Set("Number of jobs");
$graph->xaxis->SetTickLabels($datax);
$graph->xaxis->SetFont(FF_FONT2,FS_BOLD,12);
$graph->xaxis->SetLabelAngle(90);
$graph->xaxis->SetTextLabelInterval(8);

$graph->title->SetFont(FF_FONT1,FS_BOLD);
$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);
$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);

// Display the graph
$graph->Stroke("se_size.png");

?>
<img src="se_size.png">

