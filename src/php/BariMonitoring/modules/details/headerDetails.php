<?php
if($job_type=="prod"&&($job_status=="success"||$job_status=="failed")&&($upper_limit-$lower_limit)<86401){?>
<h3><font color=blue><?=$production?> production</font> jobs executed at site: <font color=blue><?=$site?></font> and ended  on <?=date('j',$lower_limit)."-".date('m',$lower_limit)."-".date('Y',$lower_limit)?> with exit status: <font color=blue><?=$job_status?></font></h2>
<?php }

if($job_type=="prod"&&($job_status=="success"||$job_status=="failed")&&($upper_limit-$lower_limit)>86401&&$upper_limit>=strtotime($str)){?>
<h3><font color=blue><?=$production?> total production</font> jobs executed at site: <font color=blue><?=$site?></font> and ended  with exit status: <font color=blue><?=$job_status?></font></h2>
<?php }

if($job_type=="prod"&&($job_status=="success"||$job_status=="failed")&&($upper_limit-$lower_limit)>86401&&$upper_limit<strtotime($str)){?>
<h3><font color=blue><?=$production?> production</font> jobs  executed at site: <font color=blue><?=$site?></font> and ended before <?=date('j',$upper_limit)."-".date('m',$upper_limit)."-".date('Y',$upper_limit)?> </h2>
<? }

if($job_type=="prod"&&($job_status=="submitted"||$job_status=="running"||$job_status=="scheduled"||$job_status=="aborted")&&($upper_limit-$lower_limit)<86401){?>
<h3>Status of the <font color=blue><?=$production?> production</font> jobs  submitted at site: <font color=blue><?=$site?></font> on <?=date('j',$lower_limit)."-".date('m',$lower_limit)."-".date('Y',$lower_limit)?></h2>
<?php }

if($job_type=="prod"&&($job_status=="submitted"||$job_status=="running"||$job_status=="scheduled"||$job_status=="aborted")&&($upper_limit-$lower_limit)>86401&&$upper_limit>=strtotime($str)){?>
<h3>Status of the  <font color=blue><?=$production?> production</font> jobs submitted at site: <font color=blue><?=$site?></font> during the full production period  </h2>
<?php }

if($job_type=="prod"&&($job_status=="submitted"||$job_status=="running"||$job_status=="scheduled"||$job_status=="aborted")&&($upper_limit-$lower_limit)>86401&&$upper_limit<strtotime($str)){?>
<h3>Status of the  production jobs  submitted at site: <font color=blue><?=$site?></font> before <?=date('j',$upper_limit)."-".date('m',$upper_limit)."-".date('Y',$upper_limit)?></h2>
<?php }//merge

if($job_type=="merge"&&($job_status=="success"||$job_status=="failed")&&($upper_limit-$lower_limit)<86401){?>
<h3><font color=blue><?=$production?> mmerge</font> jobs executed at site: <font color=blue><?=$site?></font> and ended  on <?=date('j',$lower_limit)."-".date('m',$lower_limit)."-".date('Y',$lower_limit)?> with exit status: <font color=blue><?=$job_status?></font></h2>
<h3>Stream: <font color=blue><?=$merged_dataset?></font></h3>
<?php }

if($job_type=="merge"&&($job_status=="success"||$job_status=="failed")&&($upper_limit-$lower_limit)>86401&&$upper_limit>=strtotime($str)){?>
<h3><font color=blue><?=$production?> total merge</font>  jobs executed at site: <font color=blue><?=$site?></font> and ended  with exit status: <font color=blue><?=$job_status?></font></h2>
<h3>Stream: <font color=blue><?=$merged_dataset?></font></h3>
<?php }

if($job_type=="merge"&&($job_status=="success"||$job_status=="failed")&&($upper_limit-$lower_limit)>86401&&$upper_limit<strtotime($str)){?>
<h3><font color=blue><?=$production?> merge</font>  jobs executed at site: <font color=blue><?=$site?></font> and ended before <?=date('j',$upper_limit)."-".date('m',$upper_limit)."-".date('Y',$upper_limit)?></h2>
<h3>Stream: <font color=blue><?=$merged_dataset?></font></h3>
<?php }

if($job_type=="merge"&&($job_status=="submitted"||$job_status=="running"||$job_status=="scheduled"||$job_status=="aborted")&&($upper_limit-$lower_limit)<86401){?>
<h3>Status of the <font color=blue><?=$production?> merge</font>  jobs  submitted at site: <font color=blue><?=$site?></font> on <?=date('j',$lower_limit)."-".date('m',$lower_limit)."-".date('Y',$lower_limit)?></h2>
<h3>Stream: <font color=blue><?=$merged_dataset?></font></h3>
<?php }

if($job_type=="merge"&&($job_status=="submitted"||$job_status=="running"||$job_status=="scheduled"||$job_status=="aborted")&&($upper_limit-$lower_limit)>86401&&$upper_limit>=strtotime($str)){ ?>
<h3>Status of the  <font color=blue><?=$production?> merge</font>  jobs  submitted at site: <font color=blue><?=$site?></font> during the full production period </h2>
<h3>Stream: <font color=blue><?=$merged_dataset?></font></h3>
<?php }

if($job_type=="merge"&&($job_status=="submitted"||$job_status=="running"||$job_status=="scheduled"||$job_status=="aborted")&&($upper_limit-$lower_limit)>86401&&$upper_limit<strtotime($str)){?>
<h3>Status of the <font color=blue><?=$production?> merge</font>  jobs submitted at site: <font color=blue>$site</font> before <?=date('j',$upper_limit)."-".date('m',$upper_limit)."-".date('Y',$upper_limit)?></h2>
<h3>Stream: <font color=blue><?=$merged_dataset?></font></h3>
<?php
}
?>
