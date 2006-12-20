<table border=0>
<?php
$filename="file_lists/software_deployment.txt";
$fcontents = file ($filename);

$last_change=filectime ($filename);
?>
<tr style="background-color: lightgray;">
	<td align=center><font size=+1><b>Software deployment Status </b></font><br> 
	<?php echo date('j',$last_change)."/".date('m',$last_change)."/".date('Y',$last_change)." ".date('H',$last_change).":".date('i',$last_change).":".date('s',$last_change)?><input type=text name=sw_tag value="<?=$sw_tag?>">

<?php if($sw_tag==""){?>
<tr style="background-color: lightgray;"><td>Software deployment information not available at the moment</td></tr>
<? }else {

	$str="";
	while (list ($line_num, $line) = each ($fcontents)) {
		for($i=0;$i<count($tier);$i++){
			if(strpos($CE[$i],$line)){
				if(!strpos($sw_tag,$line)){
					$str.=$tier[$i]."; ";
					break;
				}
			}
		}

	}
	if($str==""){
?>
<tr style="background-color: rgb(153, 255, 153);"><td>available on all sites</td></tr>
<?php }	else{?>
<tr style="background-color: rgb(255, 102, 102);"><td>not available on the following sites:<br><?=$str?></td></tr>
<?php }}?>
</table></p>
