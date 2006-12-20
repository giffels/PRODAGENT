<script type="text/javascript" src="modules/popup.js"></script>
<!-- BEGIN FLOATING LAYER CODE //-->
<div id="theLayer" style="position:absolute;width:250px;left:180;top:180;visibility:visible">
<table border="0" width="250" bgcolor="#424242" cellspacing="0" cellpadding="5" class="popup">
<tr class="popup">
<td  class="popup" width="100%">
  <table border="0" width="100%" cellspacing="0" cellpadding="0" height="36" class="popup">
  <tr  class="popup">
  <td  class="popup" id="titleBar" style="cursor:move" width="100%">
  <ilayer width="100%" onSelectStart="return false">
  <layer width="100%" onMouseover="isHot=true;if (isN4) ddN4(theLayer)" onMouseout="isHot=false">
  <font face="Arial" color="#F11111">Site name missing</font>
  </layer>
  </ilayer>
  </td>
  <td  class="popup" style="cursor:hand" valign="top">
  <a  class="popup" href="#" onClick="hideMe();return false"><font color=#111111 size=2 face=arial  style="text-decoration:none">X</font></a>
  </td>
  </tr>
  <tr  class="popup">
  <td  class="popup" width="100%" bgcolor="#FFFFFF" style="padding:4px" colspan="2">
<!-- PLACE YOUR CONTENT HERE //-->  
This CEs aren't included in xml file<br>
<?php
foreach($other_CEs as $key => $value)
	echo "<b>".$value."</b><br>";
?>
<!-- END OF CONTENT AREA //-->
  </td>
  </tr>
  </table> 
</td>
</tr>
</table>
</div>
<!-- END FLOATING LAYER CODE //--> 
