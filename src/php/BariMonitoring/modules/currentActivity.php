<table><tr><td>
<?php
$tail ="";
echo $PAserver." ".$curr_server." ".$prodarea."<hr>";
exit(0);
if($PAserver!=$curr_server){
	$chack_tag_cmd="ssh -2 prodagent@pccms28 'ssh pccms27 'tail -n12 $prodarea/JobSubmitter/ComponentLog''";
	exec($chack_tag_cmd, $retval1);
	foreach($retval1 as $value){
		//echo $value."<br>";
	}
	$retval1 = array_slice ($retval1, 12);
	$tail = implode("\n", $retval1);
}
else {           $tail = `tail -n12 $prodarea/JobSubmitter/ComponentLog`;}
/**** END1 ***/


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

/**** END2 ***/

echo "<h3><a href=\"show_logs.php?command=JobSubmitter\">JobSubmitter current  activity</a><font size=-1>($PAserver)</font></h3>";			
echo " <textarea rows=10 cols=64 NOWRAP>$tail</textarea>";

$tail ="";
if($PAserver!=$curr_server){
	$chack_tag_cmd="ssh -2 prodagent@pccms28 'ssh pccms27 'tail -n12 $prodarea/JobTracking/ComponentLog''";
	exec($chack_tag_cmd, $retval2);
	foreach($retval2 as $value){
		//echo $value."<br>";
	}
	$retval2 = array_slice ($retval2, 12);
	$tail = implode("\n", $retval2);
}
else {           $tail = `tail -n12 $prodarea/JobTracking/ComponentLog`;}
/*** END3 ***/


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
/**** END4 ***/

echo "<h3><a href=\"show_logs.php?command=JobTracking\">JobTracking current activity</a><font size=-1>($PAserver)</font></h3>";			
echo " <textarea rows=10 cols=64 NOWRAP>$tail</textarea>";

echo "</td></tr>";

$tail ="";
if($PAserver!=$curr_server){
	$chack_tag_cmd="ssh -2 prodagent@pccms28 'ssh pccms27 'tail -n12 $prodarea/DBSInterface/ComponentLog''";
	exec($chack_tag_cmd, $retval3);
	foreach($retval3 as $value){
		//echo $value."<br>";
	}
	$retval3 = array_slice ($retval3, 12);
	$tail = implode("\n", $retval3);
}
else {           $tail = `tail -n12 $prodarea/DBSInterface/ComponentLog`;}
/**** END5 ***/


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
/**** END6 ***/


echo "<h3><a href=\"show_logs.php?command=DBSInterface\">DBSInterface current activity</a><font size=-1>($PAserver)</font></h3>";			
echo " <textarea rows=10 cols=64 NOWRAP>$tail</textarea>";

$tail ="";
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
/**** END6 ***/

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
/**** END7 ***/
?>
<h3><a href="show_logs.php?command=MergeSensor">MergeSensor current activity </a><font size=-1>(<?=$PAserver?>)</font></h3>			
<textarea rows=10 cols=64 NOWRAP><?=$tail?></textarea>
</td></tr></table>
