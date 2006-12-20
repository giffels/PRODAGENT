<?php

function getDataSet($job) {
	global $production;
	$file="$prodarea/JobCreator/".$job."-cache/DashboardInfo.xml";
	if(file_exists($file)) {
		$fp=fopen($file,"r");
		$contents=fread($fp,filesize($file));
		fclose($fp);
		$inizio=strpos($contents,"Task=");
		$fine=strpos(strstr($contents,'Task'),"\">");
		$dataset=substr($contents,$inizio+16,$fine-16);       
	} else {
		$dataset="N/A";
	} 
	return !strcmp($prodtype,$dataset);
}
?>
