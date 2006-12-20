<?php

//row1cell4	

//proxy
			//comando lanciato appena viene caricata la pagina
			// per aggiornaare informazioni sul proxy
			//$cmd="ssh -1 prodagent@pccms28.ba.infn.it 'voms-proxy-info -all >/home/prodagent/public_html/CSA06/voms.txt'";
			//system($cmd);
			if($PAserver!=$curr_server){
             	$chack_tag_cmd="ssh -2 prodagent@pccms28 'ssh pccms27 'tail -n1 /home/prodagent/public_html/CSA06/file_lists/voms.txt''";
				$tail=exec($chack_tag_cmd, $retval);
             	$chack_tag_cmd="ssh -2 prodagent@pccms28 'ssh pccms27 'ls -l /home/prodagent/public_html/CSA06/file_lists/voms.txt''";
				$tailmeno1=exec($chack_tag_cmd, $retval);
				$tailmeno1=substr($tailmeno1,45,12);
?>
<table>
<tr>
<td><font size="+1"><b>Last proxy 
<a href="http://<?=$PAserver?>/~prodagent/CSA06/file_lists/voms.txt">status</a></b></font> 
at <?=$tailmeno1." ".($PAserver)?></td>
<?php			}
            else {           $tail = `tail -n1 /home/prodagent/public_html/CSA06/file_lists/voms.txt`;
			$last_change=filectime ("file_lists/voms.txt");
			if(($now-$last_change)<3700){
?>
<table><tr><td ><font size=+1><b>Proxy <a href="http://$PAserver/~prodagent/CSA06/file_lists/voms.txt">status</a></b></font> at <?=date("G:i",$last_change)." ".($PAserver)?></td>";
<?php		}
			else {
?>
<table><tr><td style="background-color: rgb(255, 102, 102);"><font size=+1><b>Last proxy <a href="http://$PAserver/~prodagent/CSA06/file_lists/voms.txt">status</a></b></font> at <?=date("G:i d/m",$last_change)." ".($PAserver)?></td>
<?php }
            }
			$el=explode(":", $tail);
			if($el[1]>150){
				echo "<td style=\"background-color: rgb(153, 255, 153);\"><font size=+1><b>$tail</b></font></td></tr></table>";
			}
			else{
				echo "<td style=\"background-color: rgb(1255, 102, 102);\"><font size=+1><b>$tail</b></font></td></tr></table>";
			}
//resource statud

//links
?>
