<html>
<head>
<link rel="stylesheet" type="text/css" media="all" href="modules/style.css" />
<?
$debug=$_GET["debug"];
$checkHisto=$_GET["checkHisto"];
$action=$_GET["action"];
$production=$_GET["production"];
$site=$_GET["site"];
$sw_tag=$_GET["sw_tag"];
$merged_dataset=$_GET["merged_dataset"];
//define the global parameters	and variables depending from the production		
include_once "local/monParams-FTS.php";
include_once "modules/phpFunction.php";

# open database connection   
$link = mysql_connect($DB_SPEC, $DB_USER, $DB_PASS);

$curr_server=getenv("SERVER_NAME");

?>

<title><?=$production?></title>
</head>
<body>
<form name=myform id='frm1' action="<?=$_SERVER['PHP_SELF']?>" method=GET>
<input id='checkHisto' name='checkHisto' type='hidden' value='<?=$checkHisto?>'>
<table border=2 width=100%>
<tr><td colspan=3 align=center class=externaLink>
<!-- ************ Header pages - start *************** -->
<?php include_once('modules/headerPages.php');?>
<!-- *********** Header pages - end ***************** -->
</td></tr>
<tr><td colspan=3>
<!--**********  external link - start ************* -->
<?php include_once('modules/externalLink.php');?>
<!--**********  external link - end ************* -->
</td></tr>
<tr>
  <td style="vertical-align: top; text-align: center;">
<!--**********  Prodagent component -start ************* -->
<?php include_once('modules/prodagentComponent.php');?>
<!--**********  Prodagent component -end ************* -->
</td>
<td valign=top>
<!--**********  Monitoring dei jobs - start ************* -->
<?php include_once('modules/jobsMonitoring.php');?>
<!--**********  Monitoring dei jobs - end ************* -->
</td>
<td valign=top>
<!--**********  Proxy status - start ************* -->
<!?php include_once('modules/proxyStatus.php');?>
<!--**********  Proxy status - end ************* -->
<!--**********  Software deployment Status - start ************* -->
<?php include_once('modules/softwareDeploymentStatus.php');?>
<!--**********  Software deployment Status - end ************* -->
<!--**********  available resources - start ************* -->
<a href="ResourcesStatus.php"><font size=+1><b>Status of the available resources</b></font></a><br>
<!-- **********  available resources - end ************* -->
<!--**********  plots di nicola - start ************* -->
<!?php include_once('modules/plots.php');?>
<!--**********  plots di nicola - end ************* -->
</td></tr>
<tr align=center><td colspan=3>
<!--**********  JobSubmitter, JobTracking, DBSInterface, MergeSensor - current activity - start ************* -->
<?php include_once("modules/ComponentLog.php");?>
<!--**********  JobSubmitter, JobTracking, DBSInterface, MergeSensor - current activity - end ************* -->
</td></tr>
</form>
</table>
<script>
if(document.getElementById('checkHisto').value==''){
	document.getElementById('checkHisto').value='on';
	document.getElementById('frm1').submit();
}
</script>
</html>
