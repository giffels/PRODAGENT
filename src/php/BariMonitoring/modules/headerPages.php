<table width=100% class=externaLink>
<tr>
	<td colspan=4 align=center>
	<font size=+2><b>LCG <font color=blue><?=$production?></font> production <?=$Pr_status?> on 
	<font color=blue><?=$PA_NAME?></font>: <?php echo date('j')."-".date('m')."-".date('Y')." ".date('H').":".date('i').":".date('s'); ?>
	</font></b><br>DB server in use: <b><?=$DB_HOST?></b>, DB name: <b><?=$DB_NAME?></b>
<?php

# show port or socket, but no both

if ($DB_SOCKET == "") {
  echo "Port: <b>" . $DB_PORT . "</b>";
} else { 
  echo "<br>Socket: <b>" . $DB_SOCKET . "</b>";
}
?>

        <br> <input type=submit name=action value="Update page">
	</td>
</tr>
</table>
