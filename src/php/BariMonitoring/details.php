<link rel="stylesheet" type="text/css" media="all" href="modules/style.css" />
<?
global $debug;
$debug=$_GET["debug"];
$src='jpgraph/src';
include ("$src/jpgraph.php");
include ("$src/jpgraph_line.php");
include ("$src/jpgraph_bar.php");
include ("$src/jpgraph_pie.php");
include ("$src/jpgraph_pie3d.php");
include_once ("../adodb/adodb.inc.php");
include_once("modules/details/function.php");
include_once ("common/dbLib-FTS.php");			
?>
<title>Details Page </title>
<?php
$job_type=$_GET["job_type"];
$merged_dataset=$_GET["merged_dataset"];
$job_status=$_GET["job_status"];
$lower_limit=$_GET["lower_limit"];
$upper_limit=$_GET["upper_limit"];
$production_plus=$_GET["production_plus"];
$production=$_GET["production"];
$site=$_GET["site"];

$tail= `rm plots/dest_ce_bar.png`;
$tail= `rm plots/dest_ce.png`;
$tail= `rm plots/code.png`;

/*** built stream array - start ***/
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

//****** query details - start *******/
include_once("modules/details/queryProduction.php"); 
//****** query details - end *******/

//****** header details start *******//
include_once("modules/details/headerDetails.php"); 
//****** header details end *******//

$total=0;
$total_EVT=0;

//****** graph and tables of job submitted - START ******/
if($job_status=="submitted"){
	include_once("modules/details/tables_graph_j_suruscab.php");
}
//****** graph and tables of job submitted - END ******/


//****** graph and tables of job success ,running,scheduled,aborted- START ******/
elseif($job_status=="success"||$job_status=="running"||$job_status=="scheduled"||$job_status=="aborted"){
	include_once("modules/details/tables_graph_j_success.php");
}
//****** graph and tables of job success ,running,scheduled,aborted- END ******/


//****** graph and tables of job failed - START ******/
else {//viene qui se failed
	include_once("modules/details/tables_graph_j_failed.php");
}
//****** graph and tables of job failed - END ******/
?>
