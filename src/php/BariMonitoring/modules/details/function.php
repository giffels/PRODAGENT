<?php
function site_from_ce($curr_ce,$site,$ce){
	for($i=0;$i<count($ce);$i++){
		if($curr_ce==$ce[$i]){return $site[$i];}
	}
}
?>
