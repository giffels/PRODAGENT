<?php
$src_jpg='/home/prodagent/public_html/jpgraph/src/';
include_once($src_jpg."jpgraph.php");
include ($src_jpg."jpgraph_pie.php");
include ($src_jpg."jpgraph_pie3d.php");

/*for($c=0;$c<count($data_x);$c++){
	echo "sono qui: ".$data_x[$c]."<hr>";
}*/
$graph = new PieGraph(800,300,"auto","10^10");
$graph->SetShadow();

$graph->title->Set("Dest CE");
$graph->title->SetFont(FF_FONT1,FS_BOLD);

$p1 = new PiePlot3D($data_x);
$p1->ExplodeSlice(1);
$p1->SetCenter(0.45);
//$p1->SetLegends($gDateLocale->GetShortMonth());
$p1->SetLegends($data_x_leg);

$graph->Add($p1);
$graph->Stroke("dest_ce.png");
?>
<img src="dest_ce.png">
