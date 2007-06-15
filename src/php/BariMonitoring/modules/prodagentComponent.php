<?php
$chack_tag_cmd= "$prodagent --status";;
exec($chack_tag_cmd, $retval);
$stato = implode("\n", $retval);
?>
<p align=center>
<table width=90% border=0>
<tr><td>
    <table class="prodAgent">
          <tr><td class="prodAgentH" colspan=2>ProdAgent current Status<br><?=$PA_NAME?></td></tr>

<?php if(strpos($stato, "ProdMgrInterface Running")) {?>
          <tr class="prodAgentRunning"><td>ProdMgrInterface</td><td> Running</td></tr>
<?php }else {?>
          <tr class="prodAgentNotRunning"><td>ProdMgrInterface</td><td>Not Running</td></tr>
<?php }?>

<?php if(strpos($stato, "JobTracking Running")) {?>
          <tr class="prodAgentRunning"><td>JobTracking</td><td> Running</td></tr>
<?php }else {?>
          <tr class="prodAgentNotRunning"><td>JobTracking</td><td>Not Running</td></tr>
<?php }?>

<?php if(strpos($stato, "DBSInterface Running")) {?>
          <tr class="prodAgentRunning"><td>DBSInterface</td><td> Running</td></tr>
<?php }else {?>
          <tr class="prodAgentNotRunning"><td>DBSInterface</td><td>Not Running</td></tr>
<?php }?>

<?php if(strpos($stato, "MergeSensor Running")) {?>
          <tr class="prodAgentRunning"><td>MergeSensor</td><td> Running</td></tr>
<?php }else {?>
          <tr class="prodAgentNotRunning"><td>MergeSensor</td><td>Not Running</td></tr>
<?php }?>

<?php if(strpos($stato, "MergeAccountant Running")) {?>
          <tr class="prodAgentRunning"><td>MergeAccountant</td><td> Running</td></tr>
<?php }else {?>
          <tr class="prodAgentNotRunning"><td>MergeAccountant</td><td>Not Running</td></tr>
<?php }?>

<?php if(strpos($stato, "ErrorHandler Running")) {?>
          <tr class="prodAgentRunning"><td>ErrorHandler</td><td> Running</td></tr>
<?php }else {?>
          <tr class="prodAgentNotRunning"><td>ErrorHandler</td><td>Not Running</td></tr>
<?php }?>

<?php if(strpos($stato, "JobCleanup Running")) {?>
          <tr class="prodAgentRunning"><td>JobCleanup</td><td> Running</td></tr>
<?php }else {?>
          <tr class="prodAgentNotRunning"><td>JobCleanup</td><td>Not Running</td></tr>
<?php }?>

<?php if(strpos($stato, "AdminControl Running")) {?>
           <tr class="prodAgentRunning"><td>AdminControl</td><td> Running</td></tr>
<?php }else {?>
           <tr class="prodAgentNotRunning"><td>AdminControl</td><td>Not Running</td></tr>
<?php }?>

<?php if(strpos($stato, "StatTracker Running")) {?>
           <tr class="prodAgentRunning"><td>StatTracker</td><td> Running</td></tr>
<?php }else {?>
           <tr class="prodAgentNotRunning"><td>StatTracker</td><td>Not Running</td></tr>
<?php }?>

<?php if(strpos($stato, "DatasetInjector Running")) {?>
            <tr class="prodAgentRunning"><td>DatasetInjector</td><td> Running</td></tr>
<?php }else {?>
            <tr class="prodAgentNotRunning"><td>DatasetInjector</td><td>Not Running</td></tr>
<?php }?>

<?php if(strpos($stato, "RequestInjector Running")) {?>
            <tr class="prodAgentRunning"><td>RequestInjector</td><td> Running</td></tr>
<?php }else {?>
            <tr class="prodAgentNotRunning"><td>RequestInjector</td><td>Not Running</td></tr>
<?php }?>

<?php if(strpos($stato, "JobCreator Running")) {?>
            <tr class="prodAgentRunning"><td>JobCreator</td><td> Running</td></tr>
<?php }else {?>
            <tr class="prodAgentNotRunning"><td>JobCreator</td><td>Not Running</td></tr>
<?php }?>

<?php if(strpos($stato, "JobSubmitter Running")) {?>
            <tr class="prodAgentRunning"><td>JobSubmitter</td><td> Running</td></tr>
<?php }else {?>
            <tr class="prodAgentNotRunning"><td>JobSubmitter</td><td>Not Running</td></tr>
<?php }?>

</table>
</tr>
<tr>
<td >
<input  type=button name=action value="Start/Stop Prodagent Components" onClick="window.location='restricted_folder/StartStopProdagentComp.php'" >
</td></tr></table>
