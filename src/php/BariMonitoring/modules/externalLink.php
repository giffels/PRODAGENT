<table border=0 width=100% class=externaLink>
<tr  class=externaLink>
<?php
echo "            
                <!--<td align=center class=externaLink><a href=\"http://webcms.ba.infn.it/silici/showPAstatistics.php\"><font color=red><font size=2>Short Statistics page</font></font></a></td>-->
		<td align=center class=externaLink><a href=\"Production\"><font color=red><font size=2>Browse 
\"prodarea\" folder</font></font></a></td>
		<td align=center class=externaLink><a href=\"browse_DB.php\"><font color=red><font size=2>Browse the \"ProdAgentDB_BOSS\" DB</a></td>
		<td align=center class=externaLink><a href=\"https://uimon.cern.ch/twiki/bin/view/CMS/JobExitCodes\"><font color=red><font size=2>Error codes (>10000)</a></td>
             </tr>
        </table>
        <hr>
        <table border=0 width=100% class=externaLink>
             <tr class=externaLink>
		 <td align=center class=externaLink><font size=2>Link to the DBS DATA Discovery <a href=\"http://cmsdbs.cern.ch\"> new link </a> (<a href=\"http://cmsdoc.cern.ch/~sekhri/Html/mc.htm\"> old link </a>)</td>
		 <!--<td align=center class=externaLink><font size=2>List of files registered in the DBS:<a href=\"filesRegisteredDBS.php?job_type=prod&production=$production\"> unmerged </a> &nbsp; <a href=\"filesRegisteredDBS.php?job_type=merge&production=$production\"> merged </a></td>-->
		  <!--<td align=center class=externaLink><font size=2>List the files stored on the SE's <a href=\"file_list.php?production=$production&site=$site\"> selected site ($site)</a> &nbsp; <a href=\"file_list.php?production=$production&site=all\">all sites</a></td>-->";
?>
<td><a href="https://twiki.cern.ch/twiki/bin/view/CMS/ProdOps">Production operations</a></td>
<td><a href="restricted_folder/dokument.php">Modify/Add Production config file</a></td>
<td><a href="restricted_folder/dokument.php?xml_file=../local/Site.xml&root_tag=Site&Name_list=sitename">Modify/Add Site config file</a></td>
</tr>
</table>
