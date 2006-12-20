<?php
include_once "local/monParams-gridice.php";
//include_once "../adodb/adodb.inc.php";

function dbLibConnect2() {
        global $DB_HOST, $DB_PORT, $DB_NAME, $DB_USER, $DB_PASS;
        $db=NewADOConnection('mysql');
        $db->Connect($DB_HOST.":".$DB_PORT,$DB_USER,$DB_PASS,$DB_NAME);
        return $db;
}

function get_CPUModel() {
        $db=dbLibConnect();
	$query="
		select Name,CPUModel from cpu_info
	";
        $result=$db->Execute($query);
	//echo $query;
        return $result;
}

?>
