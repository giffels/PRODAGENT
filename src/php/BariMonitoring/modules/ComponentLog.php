<table><tr><td>
<?php
//**** il vecchio file si chiamava modules/currentActivity.php ******//
//***  per modificare lo script che genera i logs...loggarsi alla macchina pccms27 e modificare il file /home/prodagent/public_html/ComponentLog.sh ***/
//echo "<h2>http://pccms27.ba.infn.it/~prodagent/tmp$prodarea/JobSubmitter.txt</h2>";

/********* JobSubmitter start **********/
//echo $PAserver." ".$curr_server."<hr>";
$tail ="";
$file = fopen("JobSubmitter.txt", "r");

if (!$file) {
	echo "<p>Impossibile aprire il file remoto.\n";
	exit;
}

$retval1=array();
while (!feof ($file)) {
	$retval1[] = fgets ($file, 1024);
}
$tail = implode("\n", $retval1);

$line=array();
$line_rev=array();
$line=explode("\n", $tail);
$line=array_reverse ($line);
$tail = implode("\n", $line);

if (ereg ("([0-9]{4})-([0-9]{1,2})-([0-9]{1,2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})", $tail, $regs)) {
	$str= "$regs[1]-$regs[2]-$regs[3] $regs[4]:$regs[5]:$regs[6]";
	$now=time();
	$timestamp = strtotime($str);
	if(($now-$timestamp)<300){echo "<tr><td colspan=2 width=50% style=\"background-color: rgb(153, 255, 153);\">";}
	elseif(($now-$timestamp)<600){echo "<tr><td colspan=2 width=50% style=\"background-color: rgb(255, 204, 204);\">";}
	else{echo "<tr><td colspan=2   style=\"background-color: rgb(255, 102, 102);\">";}

} 
else {
	echo "<tr><td colspan=2  >";
}
echo "<h3><a href=\"show_logs.php?command=JobSubmitter\">JobSubmitter current  activity</a><font size=-1></font></h3>";			
echo " <textarea rows=10 cols=64 NOWRAP>$tail</textarea>";
/********* JobSubmitter end **********/

/********* JobTracking start **********/
$tail ="";
$file = fopen ("JobTracking.txt", "r");
if (!$file) {
	echo "<p>Impossibile aprire il file remoto.\n";
	exit;
}
$retval1=array();
while (!feof ($file)) {
	$retval1[] = fgets ($file, 1024);
}
$tail = implode("\n", $retval1);

$line = array_slice ($line, 0, 0);
$line=explode("\n", $tail);
$line=array_reverse ($line);
$tail = implode("\n", $line);

if (ereg ("([0-9]{4})-([0-9]{1,2})-([0-9]{1,2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})", $tail, $regs)) {
	$str= "$regs[1]-$regs[2]-$regs[3] $regs[4]:$regs[5]:$regs[6]";
	$now=time();
	$timestamp = strtotime($str);
	if(($now-$timestamp)<300){echo "</td><td colspan=2 style=\"background-color: rgb(153, 255, 153);\">";}
	elseif(($now-$timestamp)<600){echo "</td><td colspan=2 style=\"background-color: rgb(255, 204, 204);\">";}
	else{echo "</td><td colspan=2 style=\"background-color: rgb(255, 102, 102);\">";}

} 
else {
	echo "</td><td colspan=2>";
}
echo "<h3><a href=\"show_logs.php?command=JobTracking\">JobTracking current activity</a><font size=-1></font></h3>";			
echo " <textarea rows=10 cols=64 NOWRAP>$tail</textarea>";
echo "</td></tr>";
/********* JobTracking end **********/

/********* DBSInterface start **********/
$tail ="";
$file = fopen ("DBSInterface.txt", "r");
if (!$file) {
	echo "<p>Impossibile aprire il file remoto.\n";
	exit;
}
$retval1=array();
while (!feof ($file)) {
	$retval1[] = fgets ($file, 1024);
}
$tail = implode("\n", $retval1);

$line = array_slice ($line, 0, 0);
$line=explode("\n", $tail);
$line=array_reverse ($line);
$tail = implode("\n", $line);

if (ereg ("([0-9]{4})-([0-9]{1,2})-([0-9]{1,2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})", $tail, $regs)) {
	$str= "$regs[1]-$regs[2]-$regs[3] $regs[4]:$regs[5]:$regs[6]";
	$now=time();
	$timestamp = strtotime($str);
	if(($now-$timestamp)<300){echo "<tr><td colspan=2 style=\"background-color: rgb(153, 255, 153);\">";}
	elseif(($now-$timestamp)<600){echo "<tr><td colspan=2   style=\"background-color: rgb(255, 204, 204);\">";}
	else{echo "<tr><td colspan=2   style=\"background-color: rgb(255, 102, 102);\">";}

} 
else {
	echo "<tr><td colspan=2  >";
}
echo "<h3><a href=\"show_logs.php?command=DBSInterface\">DBSInterface current activity</a><font size=-1></font></h3>";			
echo " <textarea rows=10 cols=64 NOWRAP>$tail</textarea>";
/********* DBSInterface end **********/

/********* MergeSensor start **********/
$tail ="";
$file_name= "MergeSensor.txt";
$file = fopen ("$file_name", "r");
if (!$file) {
	echo "<p>Impossibile aprire il file remoto.\n";
	exit;
}
$retval1=array();
while (!feof ($file)) {
	$retval1[] = fgets ($file, 1024);
}
$tail = implode("\n", $retval1);
/*
if($PAserver!=$curr_server){
	$chack_tag_cmd="ssh -2 prodagent@pccms28 'ssh pccms27 'tail -n12 $prodarea/MergeSensor/ComponentLog''";
	exec($chack_tag_cmd, $retval4);
	foreach($retval4 as $value){
		//echo $value."<br>";
	}
	$retval4 = array_slice ($retval4, 12);
	$tail = implode("\n", $retval4);
}
else {           $tail = `tail -n12 $prodarea/MergeSensor/ComponentLog`;}
*/
$line = array_slice ($line, 0, 0);
$line=explode("\n", $tail);
$line=array_reverse ($line);
$tail = implode("\n", $line);

if (ereg ("([0-9]{4})-([0-9]{1,2})-([0-9]{1,2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})", $tail, $regs)) {
	$str= "$regs[1]-$regs[2]-$regs[3] $regs[4]:$regs[5]:$regs[6]";
	$now=time();
	$timestamp = strtotime($str);
	if(($now-$timestamp)<300){echo "</td><td colspan=2 style=\"background-color: rgb(153, 255, 153);\">";}
	elseif(($now-$timestamp)<600){echo "</td><td colspan=2 style=\"background-color: rgb(255, 204, 204);\">";}
	else{echo "</td><td colspan=2 style=\"background-color: rgb(255, 102, 102);\">";}

} else {
	echo "</td><td colspan=2>";
}
?>
<h3><a href="show_logs.php?command=MergeSensor">MergeSensor current activity </a><font size=-1></font></h3>			
<textarea rows=10 cols=64 NOWRAP><?=$tail?></textarea>
</td></tr></table>
<?php
/********* MergeSensor end **********/
?>
