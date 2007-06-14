<link rel="stylesheet" type="text/css" media="all" href="modules/style.css" />
<?
$src_jpg='jpgraph/src/';
include_once ($src_jpg."jpgraph.php");
include_once ($src_jpg."jpgraph_line.php");
include_once ($src_jpg."jpgraph_bar.php");
?>
<HTML>
<HEAD>
<TITLE>BOSS job detail</TITLE>
</HEAD>
<body bgcolor=#EBFAFC>
<form name=kill action=kill_jobs.php method=POST>
<script>
function fselect_all()
{
	for (i=0;i<kill.elements.length;i++){
	if(kill.elements[i].type=="checkbox"&&kill.elements[i].name.indexOf("kill")>0){kill.elements[i].checked=1}
	}
	
}
function fdeselect_all()
{
	for (i=0;i<kill.elements.length;i++){
	if(kill.elements[i].type=="checkbox"){kill.elements[i].checked=0}
	}
	
}
function find_selected()
{
	str="";
	for (i=0;i<kill.elements.length;i++){
		if(kill.elements[i].type=="checkbox"&&kill.elements[i].name.indexOf("kill")>0&&kill.elements[i].checked==1){
			str=str+kill.elements[i].name.replace("_kill","")+"\n"			
			//alert(str)
		}
	}
	kill.job_list.value=str
	if(confirm("Are you sure you want to cancel the selected jobs?")) {
	alert("yes")
	kill.submit()
	}
}

</script>
<?

$debug=$_GET["debug"];	
$merged_dataset=$_GET["merged_dataset"];
$job_type=$_GET["job_type"];	
$job_status=$_GET["job_status"];	
$lower_limit=$_GET["lower_limit"];	
$upper_limit=$_GET["upper_limit"];	
$production=$_GET["production"];	
$site=$_GET["site"];
if (isset($_GET["id"])) {$query_id="AND TASKID=".$_GET["id"];}else {$query_id="";}

include_once "local/monParams-FTS.php";		
/*** built stream array - start ****/
include_once("common/dbLib-FTS.php");
$records_gs=get_stream($production);
$stream_merge=array();
if($records_gs)
while(!$records_gs->EOF){
        $processed                      =       $records_gs->Fields("processed");
        $TASK_NAME_stream               =       $records_gs->Fields("TASK_NAME");
        $records_gs->MoveNext();
        $stream_merge[$processed]       .=      $TASK_NAME_stream.",";
}
//echo "details.php<br>".$stream_merge[$processed]."<br>";
/***  built stream array - end **/

if($job_type=="merge"){$production_plus="%merge%";}			

//production			
if($job_type=="prod"&&($job_status=="success"||$job_status=="failed")&&($upper_limit-$lower_limit)<86401){				
	echo "<h3><font color=blue>$production production</font> jobs executed at site: <font color=blue>$site</font>";
	echo "and ended  on ".date('j',$lower_limit)."-".date('m',$lower_limit)."-".date('Y',$lower_limit)." with exit status: ";
	echo "<font color=blue>$job_status</font></h2>";
}			

if($job_type=="prod"&&($job_status=="success"||$job_status=="failed")&&($upper_limit-$lower_limit)>86401&&$upper_limit>=strtotime($str)){
	echo "<h3><font color=blue>$production total production</font> jobs executed at site: <font color=blue>$site</font>";
	echo" and ended  with exit status: <font color=blue>$job_status</font></h2>";
}			

if($job_type=="prod"&&($job_status=="success"||$job_status=="failed")&&($upper_limit-$lower_limit)>86401&&$upper_limit<strtotime($str)){
	?>
	<h3><font color=blue><?=$production?> production</font> jobs  executed at site: <font color=blue><?=$site?></font> and ended before 
	<?php echo date('j',$upper_limit)." ".date('m',$upper_limit)."-".date('Y',$upper_limit);?> </h2>
<?php
}			

if($job_type=="prod"&&($job_status=="submitted"||$job_status=="running"||$job_status=="scheduled"||$job_status=="aborted")
	&&($upper_limit-$lower_limit)<86401){				
	echo "<h3>Status of the <font color=blue>$production production</font> jobs  submitted at site: ";
	echo "<font color=blue>$site</font> on ".date('j',$lower_limit)."-".date('m',$lower_limit)."-".date('Y',$lower_limit)." ";
	echo "and with current  status: <font color=blue>$job_status</font></h2>";
				}			
if($job_type=="prod"&&($job_status=="submitted"||$job_status=="running"||$job_status=="scheduled"||$job_status=="aborted")&&
	($upper_limit-$lower_limit)>86401&&$upper_limit>=strtotime($str)){
	echo "<h3>Status of the  $production production jobs submitted at site: <font color=blue>$site</font>";
	echo "during the full production period and with current  status: <font color=blue>$job_status</font></h2>";				
}
	
if($job_type=="prod"&&($job_status=="submitted"||$job_status=="running"||$job_status=="scheduled"||$job_status=="aborted")&&
	($upper_limit-$lower_limit)>86401&&$upper_limit<strtotime($str)){				
	echo "<h3>Status of the $production production jobs  submitted at site: <font color=blue>$site</font> ";
	echo "before ".date('j',$upper_limit)."-".date('m',$upper_limit)."-".date('Y',$upper_limit)." ";
	echo "and with current  status: <font color=blue>$job_status</font></h2>";				}//merge			

if($job_type=="merge"&&($job_status=="success"||$job_status=="failed")&&($upper_limit-$lower_limit)<86401){				
	echo "<h3><font color=blue>$production merge</font> jobs executed at site: <font color=blue>$site</font> "; 
	echo "and ended  on ".date('j',$lower_limit)."-".date('m',$lower_limit)."-".date('Y',$lower_limit)." ";
	echo "with exit status: <font color=blue>$job_status</font></h2>";				}			

if($job_type=="merge"&&($job_status=="success"||$job_status=="failed")&&($upper_limit-$lower_limit)>86401&&$upper_limit>=strtotime($str)){
	echo "<h3><font color=blue>$production total merge</font>  jobs executed at site: <font color=blue>$site</font> ";
	echo "and ended  with exit status: <font color=blue>$job_status</font></h2>";				
}			

if($job_type=="merge"&&($job_status=="success"||$job_status=="failed")&&($upper_limit-$lower_limit)>86401&&$upper_limit<strtotime($str)){
	echo "<h3><font color=blue>$production merge</font>  jobs executed at site: <font color=blue>$site</font> ";
	echo "and ended before ".date('j',$upper_limit)."-".date('m',$upper_limit)."-".date('Y',$upper_limit)." </h2>";
}			

if($job_type=="merge"&&($job_status=="submitted"||$job_status=="running"||$job_status=="scheduled"||$job_status=="aborted")&&
	($upper_limit-$lower_limit)<86401){				
	echo "<h3>Status of the <font color=blue>$production merge</font>  jobs  submitted at site: <font color=blue>$site</font> ";
	echo "on ".date('j',$lower_limit)."-".date('m',$lower_limit)."-".date('Y',$lower_limit)." and with current  ";
	echo "status:<font color=blue>$job_status</font></h2>";				
}			

if($job_type=="merge"&&($job_status=="submitted"||$job_status=="running"||$job_status=="scheduled"||$job_status=="aborted")
	&&($upper_limit-$lower_limit)>86401&&$upper_limit>=strtotime($str)){				
	echo "<h3>Status of the  <font color=blue>$production merge</font>  jobs  submitted at site: <font color=blue>$site</font> ";
	echo "during the full production period and with current  status: <font color=blue>$job_status</font> </h2>";
}			

if($job_type=="merge"&&($job_status=="submitted"||$job_status=="running"||$job_status=="scheduled"||$job_status=="aborted")
	&&($upper_limit-$lower_limit)>86401&&$upper_limit<strtotime($str)){
	echo "<h3>Status of the <font color=blue>$production merge</font>  jobs submitted at site: <font color=blue>$site</font> ";
	echo "before ".date('j',$upper_limit)."-".date('m',$upper_limit)."-".date('Y',$upper_limit)." and with current  ";
	echo "status: <font color=blue>$job_status</font></h2>";				
}	
//echo "<input type=button name=select_all value=\"Select all\" onClick=\"fselect_all()\" >";
//echo "<input type=button name=deselect_all value=\"Deselect all\" onClick=\"fdeselect_all()\" >";
//echo "<input type=button name=action value=\"Cancel selected Jobs\" onClick=\"find_selected()\" >";

//*** qeury al db per latabella dettaglio job ****/
$cnt_field="destce,TASKEXIT,NEVT,LOGFILE, CHAIN_ID,ID,TASK_name,STATUS,comment, EXEC_HOST, START_T,STOP_T,SE_OUT,TASKID,TASKEXIT,SUBT,SCHEDID";
$group_par="";
$order_by=" order by TASKID";
//if($DB_type==2)
$records=getJobDetails($production,$job_type_filter,$job_status,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);
//else
//$records=getJobDetails_old($production,$job_type_filter,$job_status,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);
//*** qeury al db per latabella dettaglio job ****/

$delta_t_start= array();	
$delta_t_stop= array();	
$non_trovato=0;
$ended_zero=0;
$ended_nonzero=0;
$fixed=0;
$running=0;
$queued=0;

$num_rows=$records->RecordCount();
echo "<hr>Numero di records: ".$num_rows;

if ($num_rows==0){
	echo "<tr><td>Nessun record trovato Boss DB</td></tr>";
}		

else{
	$total_jobs=$num_rows;
	while (!$records->EOF){
		$task_name[]	=	$records->Fields("TASK_name");
		$task_id[]	=	$records->Fields("TASKID");			
		$id[]		=	$records->Fields("ID");
		//$sched_id[]	=	$records->Fields("CHAIN_ID");
		$task_exit[]	=	$records->Fields("TASKEXIT");			
		$comment[]	=	$records->Fields("comment");			
		$exec_host[]	=	$records->Fields("EXEC_HOST");			
		$sub_time[]	=	$records->Fields("SUB_T");			
		$start_t[]	=	$records->Fields("START_T");
		$stop_t[]	=	$records->Fields("STOP_T");			
		$se_out[]	=	$records->Fields("SE_OUT");			
		$retrive_file[]	=	preg_replace("/_\d+_\d+.log$/","",$records->Fields("LOGFILE"));//$records->Fields(";			
		$job_name[]     =       preg_replace("/-[^-]*$/", "", $records->Fields("LOGFILE"));
		$sched_id[]	=	$records->Fields("SCHEDID");
		$records->MoveNext();
	}		
}				

//$link = mysql_connect($DB_HOST2.":".$DB_PORT2,$DB_USER2,$DB_PASS2);		


?>
<!-- BOSS_tables  start -->
<?php include_once("modules/BOSS_tables.php");?>
<!-- BOSS_tables  end-->
<?php
if(count($task_id)){
	return 0;
	$gridice_ineff=$non_trovato/count($task_id);
	echo "<h4>Summary table for production = $production site: $site</h4>";
	echo "<table>";
	echo "<tr><td>Total number of jobs</td><td align=right>$total_jobs</td></tr>";
	echo "<tr><td>number of jobs not seen by gridIce</td><td align=right>$non_trovato</td></tr>";
	echo "<tr><td>number of jobs ended with exitStatus=0</td><td align=right>$ended_zero</td></tr>";
	echo "<tr><td>number of jobs ended with exitStatus not 0</td><td align=right>$ended_nonzero</td></tr>";
	echo "<tr><td>number of jobs running</td><td align=right>$running</td></tr>";
	echo "<tr><td>number of jobs in queue</td><td align=right>$queued</td></tr>";
	echo "<tr><td>number of jobs fixed</td><td align=right>$fixed</td></tr>";
	echo "<tr><td> </td><td> </td></tr>";
	echo "<tr><td>GridICE Inefficiency</td><td align=right>$gridice_ineff</td></tr>";

	echo "</table>";
	
	histo($delta_t_start,50,8,-200,"t_start-Prodagent  meno t_start-GridIce","Time (s)","dN/dt","t_start");
	histo($delta_t_stop,50,8,-200,"t_stop-Prodagent  meno t_stop-GridIce","Time (s)","dN/dt","t_stop");
	echo "<img src=\"plots/t_start.png\"><br>";
	echo "<img src=\"plots/t_stop.png\">";
	echo " <textarea name=job_list rows=10 cols=64 NOWRAP></textarea>";

}

echo "</form>";
echo "</BODY></HTML>";

function histo($data,$bin_num,$bin_width,$bin_init,$Title,$Xleg,$Yleg,$PNG_name){
	sort($data);
	$datax=array();
	$datay=array();
	$bin=0;
	$index=0;
	$Av=0;
	$Sdv=0;
	$entry=0;
	while($bin<=$bin_num){	
		$datax[$bin]=($bin_init + $bin_width*$bin);		
		$datay[$bin]=0;	
		while($data[$index]<($bin_init+$bin_width*$bin) && $index<count($data)){
				$datay[$bin]++;	
				$index++;		
				if($bin>0){
					$Av+=$data[$index];
					$entry+=1;
				}				
				else {$ind1=$index;}
			}
		$bin++;		
	}				
	if($entry) {$Av=$Av/$entry;}else{$Av=0;}
	$underflow=array_shift($datay);		
	$overflow=count($data)-$index;		
	for($i=0;$i<$bin_num;$i++){
		$Sdv+=$datay[$i]*($datax[$i]-$Av)*($datax[$i]-$Av);
	}		
	if($entry) {$Sdv=sqrt($Sdv/$entry);}else{$Sdv=0;}		
	$graph = new Graph(800,400,"auto","10^10");    		
	$graph->SetScale("textlin");		
	// Add a drop shadow
		
	$graph->SetShadow();		
	// Adjust the margin a bit to make more room for titles		
	$graph->img->SetMargin(40,160,20,80);		
	// Create a bar plot
	$bplot = new BarPlot($datay);		
	// Adjust fill color		
	$bplot->SetFillColor('white');		
	//$bplot->SetColor('white');	
	//$bplot->SetFillGradient('white','darkgreen');
	$bplot->SetWidth(0.99);		
	$graph->Add($bplot);		
	$txt=new Text("Mean=".number_format($Av,2,".","")."\nRMS=".number_format($Sdv,2,".","")."\nEntries=$entry\nUnderflow=$underflow\nOverflow=$overflow");
	$txt->Pos(650,8);		
	$txt->SetFont(FF_FONT1,FS_BOLD);		
	$txt->SetBox('gray','navy','gray');		
	$txt->SetColor("black");		
	$graph->AddText($txt);		
	// Setup the titles

	$graph->title->Set($Title);		
	$graph->SetBackgroundGradient('darkred','yellow',GRAD_HOR,BGRAD_PLOT);
	$graph->xaxis->title->Set($Xleg);		
	$graph->xaxis->SetTitlemargin(40); 		
	$graph->yaxis->title->Set($Yleg);		
	$graph->xaxis->SetTickLabels($datax);		
	$graph->xaxis->SetFont(FF_FONT2,FS_BOLD,12);		
	//$graph->xaxis->SetLabelAngle(90);
	$graph->xaxis->SetTextLabelInterval(10);		
	$graph->title->SetFont(FF_FONT1,FS_BOLD);		
	$graph->yaxis->title->SetFont(FF_FONT1,FS_BOLD);		
	$graph->xaxis->title->SetFont(FF_FONT1,FS_BOLD);		
	// Display the graph

	$graph->Stroke("plots/".$PNG_name.".png");
}
?>


