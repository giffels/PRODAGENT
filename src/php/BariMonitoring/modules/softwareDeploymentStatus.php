<table border=0>
<?php
$filename="software_deployment.txt";
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
	$list_site_dw=array();
	//arsort($Site_list);
	arsort($site_array);
	while (list ($line_num, $line) = each ($fcontents)) {
		//for($i=0;$i<count($tier);$i++){
		//foreach($Site_list as $key => $value){
		foreach($site_array as $key => $value){
			if(strpos($line,$value['celist'][0])===false){
				;}
			else{
				if(!strpos($line,$sw_tag)){
					array_push($list_site_dw,$key." CE: ".$value['celist'][0]);//." ".$line);
					break;
				}
			}
		}

	}
	$list_site_dw=array_unique($list_site_dw);
	foreach($list_site_dw as $key => $value){
		$str.=$value."; ";
	}	
	if($str==""){
?>
<tr style="background-color: rgb(153, 255, 153);"><td>available on all sites</td></tr>
<?php }	else{?>
<tr style="background-color: rgb(255, 102, 102);"><td>not available on the following sites:<br><?=$str?></td></tr>
<?php }}?>
</table></p>
