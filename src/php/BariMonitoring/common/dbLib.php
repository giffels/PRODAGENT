<?php
include_once "/var/www/html/mSE/local/monParams.php";
include_once "/var/www/adodb/adodb.inc.php";

function dbLibConnect() {
        global $DB_HOST, $DB_PORT, $DB_NAME, $DB_USER, $DB_PASS;
        $db=NewADOConnection('postgres');
        $db->Connect($DB_HOST,$DB_USER,$DB_PASS,$DB_NAME);
        return $db;
}


function selectSite_dest() {
        $db=dbLibConnect();
        $result=$db->Execute("SELECT dest FROM \"destination\"");
        return $result;
}

function selectSite_source() {
        $db=dbLibConnect();
        $result=$db->Execute("SELECT host FROM \"source\"");
        return $result;
}
function draw_graph($Machine_Source,$Machine_Destination,$date_start_sec,$date_stop_sec,$op_type){
        $db=dbLibConnect();
	$query="
		SELECT
		\"dest\",\"host\",\"n_byte\",
		\"end_time\",\"start_time\",
		\"op_type\"
		FROM 
		\"se_monitoring\"
		where 
		\"dest\" like '$Machine_Destination'
		and
		\"host\"='$Machine_Source'
		and
		\"start_time\">$date_start_sec
		and
		\"start_time\"<$date_stop_sec
		and
		\"op_type\" like '$op_type'
		order by \"start_time\"
	";
	//echo $query."<hr>";
        $result=$db->Execute($query);
        return $result;
}
?>
