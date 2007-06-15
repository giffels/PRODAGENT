<?php
include_once("Configuration.php");
include_once("getProd.php");

# get PA configuration

$site=$_GET["site"];
$production=$_GET["production"];
$allpp = new getProdParams();
$production_list=$allpp->getAllProds();
if($production=="") {$production=end($production_list);}
array_push($production_list,"ALL");
$production_plus=$production."-%";

$Site_list_obj = new getProdParams($production);
//$Site_list     = $Site_list_obj->getAllSites();
$Site_list     = getAllSites_xml();

if($Site_list)
$site_array=array_keys($Site_list);

$site_query_ce="";
$site_query_se="";
$ended_site_query_ce="";
$ended_site_query_se="";

$ended_site_query_ce	.=	getSiteNamebyCE($site);
$site_query_ce		=	$ended_site_query_ce;
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
$FailureArchive = $conf->getParameter(JobCleanup, FailureArchive);
$FailureArchive = explode("prodarea/",$FailureArchive);
$FailureArchive = $FailureArchive[1];
$SuccessArchive = $conf->getParameter(JobCleanup, SuccessArchive);
$SuccessArchive = explode("prodarea/",$SuccessArchive);
$SuccessArchive = $SuccessArchive[1];


# check if DB_HOST:DB_PORT or DB_SOCKET has to be used
if ($DB_SOCKET != "") {
  $DB_SPEC = $DB_HOST . ":" . $DB_SOCKET;
} else {
  $DB_SPEC = $DB_HOST . ":" . $DB_PORT;
}

$Pr_status				= $pp->getPar("Status");
$sw_tag 				= $pp->getPar("SWtag");
$prod_failed_job_cond	= $pp->getPar("prod_failed_job_cond");
$prod_success_job_cond	= $pp->getPar("prod_success_job_cond");
$merge_failed_job_cond	= $pp->getPar("merge_failed_job_cond");
$merge_success_job_cond	= $pp->getPar("merge_success_job_cond");
$special_query_req_merge= $pp->getPar("merge_job_special_cond");
$prodarea_alias = "JobCleanup";
$prodarea_alias_failed = "JobCreator";
$faiplot = "web_faiplot_v045.sh";
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
		include("modules/popup.php");
	return $site_array;
}

function checkSiteName_CE($production) {
	$site_array_ce	=	$GLOBALS['site_array_ce'];	
	
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
		include("modules/popup.php");
	return $site_array;
}

function getSiteNamebyCE($arg) {
	if($arg=='all')
		return "";
	$site_query_ce_tmp=" AND (";
	$xml_file="local/Site.xml";
	$Name_list="sitename";
	$Name_list2="ce";
	$dokument = domxml_open_file($xml_file);
	$root = $dokument->document_element();
	$node_array = $root->get_elements_by_tagname($Name_list);
        $node_array2 = $root->get_elements_by_tagname($Name_list2);
	for ($i = 0; $i<count($node_array); $i++) {
		$node = $node_array[$i];
		if($node->get_content()==$arg){
			$node2 = $node_array2[$i];
			$ce_tmp = $node2->get_content();
			$ce_tmp_arr = explode(";",$ce_tmp);
			for($f=0;$f<count($ce_tmp_arr);$f++){
				$site_query_ce_tmp.="  SCHED_edg.dest_ce='$ce_tmp_arr[$f]' OR";
			}
		}
	}
	$site_query_ce_tmp.=")";
	$site_query_ce_tmp = str_replace("OR)",")",$site_query_ce_tmp);
	if($site_query_ce_tmp==" AND ()"){
		$site_query_ce_tmp=" AND(SCHED_edg.dest_ce='$arg')";
	}
	return $site_query_ce_tmp;
}
function getAllSites_xml(){
	$site_arr=array();
	//$site_arr['celist'][]='';
	$xml_file="local/Site.xml";
	$Name_list="sitename";
	$Name_list2="ce";
	$dokument = domxml_open_file($xml_file);
	$root = $dokument->document_element();
	$node_array = $root->get_elements_by_tagname($Name_list);
	$node_array2 = $root->get_elements_by_tagname($Name_list2);
	for ($i = 0; $i<count($node_array); $i++) {
		$node = $node_array[$i];
		$site_arr[$node->get_content()]=array();	

		$node2 = $node_array2[$i];
		$ce_tmp = $node2->get_content();
		$ce_tmp_arr = explode(";",$ce_tmp);
		for($f=0;$f<count($ce_tmp_arr);$f++){
			$site_arr[$node->get_content()]['celist'][]=$ce_tmp_arr[$f];
		}
	}
	return $site_arr;
}
$ADODB_CACHE_DIR = "/tmp$prodarea/adodb_cache";
?>
