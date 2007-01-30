<link rel="stylesheet" type="text/css" media="all" href="modules/style.css" />
<?
include_once ("common/dbLib-FTS.php");			
include_once("local/monParams-FTS.php");
echo " <title>rRsources Status</title>";
$tail=`lcg-infosites --vo cms ce`;
$line=explode("\n", $tail);
echo "<center><h2>Status of the available resources at ".date('j')."-".date('m')."-".date('Y')." ".date('H').":".date('m').":".date('s')."</h2>";
echo "<table border=1>";
$len=strlen($line[1]);
$cell=explode(chr(9),$line[1]);
echo "<tr><th>Site</th><th>$cell[0]</th><th>$cell[1]</th><th>$cell[2]</th><th>$cell[3]</th><th>$cell[4]</th></tr>";
/*
for($j=0;$j<count($site_array);$j++){
	$n_jobs=0;$n_running=0;$n_waiting=0;
	for($i=0;$i<count($line);$i++){
		if(strpos($line[$i],$CE[$j])){
			$cell=explode(chr(9),$line[$i]);
			$n_jobs+=$cell[2];$n_running+=$cell[3];$n_waiting+=$cell[4];
		}
	}
	echo "<tr><td>".$site_array[$j]."</td><td>$cell[0]</td><td>$cell[1]</td><td>$n_jobs</td><td>$n_running</td><td>$n_waiting</td></tr>";
}
*/
foreach($Site_list as $key => $value){
	$n_jobs=0;$n_running=0;$n_waiting=0;
	for($i=0;$i<count($line);$i++){
		if(strpos($line[$i],$value['celist'][0])){
			$cell=explode(chr(9),$line[$i]);
			$n_jobs+=$cell[2];$n_running+=$cell[3];$n_waiting+=$cell[4];
		}
	}
	echo "<tr><td>".$key."</td><td>$cell[0]</td><td>$cell[1]</td><td>$n_jobs</td><td>$n_running</td><td>$n_waiting</td></tr>";
}
echo "</table></center>";
echo $site."<hr>";
foreach($Site_list as $key => $value){
	//foreach($value as $key2 => $value2)
		//echo $key." #".$value['celist'][0]."#".$value['selist'][0]."<br>";
		//echo $key." ".$value." ".$key2." ".$value2."<br>";
}
?>
