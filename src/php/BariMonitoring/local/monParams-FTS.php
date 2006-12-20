<?php
include_once("getProd.php");

# get PA configuration
include_once("Configuration.php");

$site=$_GET["site"];
$production=$_GET["production"];
$allpp = new getProdParams();
$production_list=$allpp->getAllProds();
if($production=="") {$production=end($production_list);}
$production_plus=$production."-%";

$Site_list_obj = new getProdParams($production);
$Site_list     = $Site_list_obj->getAllSites();

if($Site_list)
$site_array=array_keys($Site_list);

$site_query_ce="";
$site_query_se="";
$ended_site_query_ce="";
$ended_site_query_se="";

$site =  getSiteNamebyCE($site);
if(!$Site_list[$site]){
	if($site == ''){
		;
	}
        elseif($site!='all' || $site == "NULL" || $site == "Null"){
		$site_query_ce="AND SCHED_edg.dest_ce='$site'";  
		$ended_site_query_ce="AND ENDED_SCHED_edg.dest_ce='$site'";  
	}
}
else {
	foreach($Site_list[$site] as $key => $value){
		if ($key == "celist") {
			if (count($value) > 1) {
				$i=0;
				while(list($subkey,$subvalue)=each($value)) {
					$subcequery="SCHED_edg.dest_ce='$subvalue'".$subcequery;
					$endedsubcequery="ENDED_SCHED_edg.dest_ce='$subvalue'".$endedsubcequery;
					$i++;
					if($i == count($value)) break;
					$subcequery=" OR ".$subcequery;
					$endedsubcequery=" OR ".$endedsubcequery;
				}
				$site_query_ce="AND (".$subcequery.")";
				$ended_site_query_ce="AND (".$endedsubcequery.")";
			} else {
				$site_query_ce="AND SCHED_edg.dest_ce='$value[0]'";  
				$ended_site_query_ce="AND ENDED_SCHED_edg.dest_ce='$value[0]'";  
			}
		}
		if ($key == "selist") {
			if (count($value) > 1) {
				$i=0;
				while(list($subkey,$subvalue)=each($value)) {
					$subsequery="cmssw.se_out='$subvalue'".$subsequery;
					$endedsubsequery="ENDED_cmssw.se_out='$subvalue'".$endedsubsequery;
					$i++;
					if($i == count($value)) break;
					$subsequery=" OR ".$subsequery;
					$endedsubsequery=" OR ".$endedsubsequery;
				}
				$site_query_se="AND (".$subsequery.")";
				$ended_site_query_se="AND (".$endedsubsequery.")";
			} else {
				$site_query_se="AND cmssw.se_out='$value[0]'";  
				$ended_site_query_se="AND ENDED_cmssw.se_out='$value[0]'";  
			}
		}
	}    
}

$pp = new getProdParams($production);
$DB_type				= $pp->getPar("DB_type");
//$PAserver				= $pp->getPar("PAserver");

# get PA configuration
$conf = new Configuration();

# get paramateres and store into variables used by other components
$DB_PA_NAME = $conf->getParameter(ProdAgentDB, dbName);
$db_name = $DB_PA_NAME . "_BOSS";
$DB_NAME = $db_name;
$DB_HOST = $conf->getParameter(ProdAgentDB, host);
$DB_USER = $conf->getParameter(ProdAgentDB, user);
$DB_PORT = $conf->getParameter(ProdAgentDB, portNr);
$DB_PASS = $conf->getParameter(ProdAgentDB, passwd);
$DB_SOCKET = $conf->getParameter(ProdAgentDB, socketFileLocation);
$PAserver = $conf->getParameter(ProdAgentDB, host);
$prodagent = $conf->getParameter(Environment, PRODAGENT_ROOT) . 
             "/bin/prodAgentd";
$PA_NAME = $conf->getParameter(ProdAgent, ProdAgentName);
$prodarea = $conf->getParameter(Environment, PRODAGENT_WORKDIR);

# check if DB_HOST:DB_PORT or DB_SOCKET has to be used
if ($DB_SOCKET != "") {
  $DB_SPEC = $DB_HOST . ":" . $DB_SOCKET;
} else {
  $DB_SPEC = $DB_HOST . ":" . $DB_PORT;
}

$Pr_status				= $pp->getPar("Status");
//$DB_NAME 				= $pp->getPar("MySQLDBName");
//$DB_HOST 				= $pp->getPar("MySQLServer");
//$DB_PORT 				= $pp->getPar("MySQLPort");
//$DB_USER 				= $pp->getPar("MySQLDBUser");
//$DB_PASS 				= $pp->getPar("MySQLDBpasswd");
$sw_tag 				= $pp->getPar("SWtag");
$prod_failed_job_cond	= $pp->getPar("prod_failed_job_cond");
$prod_success_job_cond	= $pp->getPar("prod_success_job_cond");
$merge_failed_job_cond	= $pp->getPar("merge_failed_job_cond");
$merge_success_job_cond	= $pp->getPar("merge_success_job_cond");
//$db_name				= $pp->getPar("MySQLDBName");
$special_query_req_merge= $pp->getPar("merge_job_special_cond");
//$setup					= $pp->getPar("Setup_file");
//$prodagent				= $pp->getPar("PA_root");
//$prodarea				= $pp->getPar("prodarea_path_abs");

//$prodarea_alias			= $pp->getPar("prodarea_path_rel");

$prodarea_alias = "JobCleanup";

//$prodarea_alias_failed		= $pp->getPar("prodarea_path_rel_failed");

$prodarea_alias_failed = "JobCreator";

//$faiplot				= $pp->getPar("prod_plot_command");

$faiplot = "web_faiplot_v045.sh";

//$faiplot_merged			= $pp->getPar("merge_plot_command");	

$faiplot_merged = "web_faiplot_merged_v045.sh";

//fine definizione delle variabili dipendenti dalla  produzione
function checkSiteNamebyCE($production) {
	$site_array 	=	$GLOBALS['site_array'];
	$records=getAllCE($production);
	$ce_list_fromDB=array();
	while(!$records->EOF){
		$ce_temp_fromDB=$records->Fields("destce");
		if($ce_temp_fromDB!='')
			$ce_list_fromDB[]=$ce_temp_fromDB;
		$records->MoveNext();
	}

	$Site_list_obj 	= 	new getProdParams($production);
	$CE_ls 		= 	$Site_list_obj->getAllCEs();
	
	$found_other_ce	=	false;
	$other_CEs	=	array();
	foreach($ce_list_fromDB as $key => $value){
		if (!in_array($value, $CE_ls)) {
			$site_array[]=$value;
			$other_CEs[]=$value;
			$found_other_ce=true;
		}
	}
	if($found_other_ce)
		include("popup.php");
	return $site_array;
}

function getSiteNamebyCE($arg) {
	global $Site_list;
	if($Site_list)
		reset($Site_list);
	$found="NO";

	if($Site_list)
		while(list($key,$val)=each($Site_list)) {
			foreach($val as $subkey => $subval){
				if($subkey == "celist") {
					if(in_array($arg,$subval)) {$found="YES";$name=$key;}
				}
			}
		}
	if ($found == "YES") return $name;
	else return $arg;
}
?>
