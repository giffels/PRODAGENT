<?php
//****** query production success start *******/
if($job_status=="success"){
	$cnt_field="destce,count(PRIMDATASET) as PRIMDATASET_count , sum(NEVT) as NEVT_sum,LOGFILE REGEXP 'mergejob' as LOGFILE_group";
	$group_par=" GROUP BY destce,LOGFILE_group";
	$order_by ="";
	if($DB_type==2)
	$records[0]=getJobDetails($production,$job_type_filter,$job_status,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);
	else
	$records[0]=getJobDetails_old($production,$job_type_filter,$job_status,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);
	
$cnt_field="SUBT,destce,STATUS";
$group_par=" ";
$order_by =" order by SUBT";
$site_query_ce.=" and STATUS='Success'"; 
if($DB_type==2)
	$plots_record=getJobDetails_allStatus($production_plus,$prod_failed_job_cond,$prod_success_job_cond,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);
	else
	$plots_record=getJobDetails_allStatus_old($production,$prod_failed_job_cond,$prod_success_job_cond,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);

$cnt_field="SUBT,NEVT,destce,STATUS";
$group_par=" ";
$order_by =" order by SUBT";
$site_query_ce.=" and STATUS='Success'"; 
if($DB_type==2)
	$plots_record_nevt=getJobDetails($production_plus,$prod_failed_job_cond,$prod_success_job_cond,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);
	else
	$plots_record_nevt=getJobDetails_old($production,$prod_failed_job_cond,$prod_success_job_cond,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);
}
//****** query production success end*******/

//****** query production failed start ******/
if($job_status=="failed"){

$cnt_field="SUBT,destce,STATUS";
$group_par=" ";
$order_by =" order by SUBT";
$site_query_ce_plus=$site_query_ce." and STATUS='failed'"; 
if($DB_type==2)
	$plots_record=getJobDetails_allStatus($production_plus,$prod_failed_job_cond,$prod_success_job_cond,$job_type,$lower_limit,$upper_limit,$site_query_ce_plus,$query_id,$cnt_field,$group_par,$order_by);
	else
	$plots_record=getJobDetails_allStatus_old($production,$prod_failed_job_cond,$prod_success_job_cond,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);

	$cnt_field="destce,count(PRIMDATASET) as PRIMDATASET_count , sum(NEVT) as NEVT_sum";
	$group_par=" GROUP BY destce";
	$order_by ="";
	if($DB_type==2)
	$records[0]=getJobDetails($production,$job_type_filter,$job_status,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);
	else
	$records[0]=getJobDetails_old($production,$job_type_filter,$job_status,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);
	$cnt_field="TASKEXIT, count(PRIMDATASET) as PRIMDATASET_count";
	$group_par=" GROUP BY  TASKEXIT";
	$order_by =" order by TASKEXIT";
	if($DB_type==2)
	$records[1]=getJobDetails($production,$job_type_filter,$job_status,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);
	else
	$records[1]=getJobDetails_old($production,$job_type_filter,$job_status,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);
	
	$cnt_field="destce,count(PRIMDATASET) as PRIMDATASET_count, TASKEXIT";
	$group_par=" GROUP BY destce,TASKEXIT";
	if($DB_type==2)
	$records[2]=getJobDetails($production,$job_type_filter,$job_status,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);
	else
	$records[2]=getJobDetails_old($production,$job_type_filter,$job_status,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);
	$cnt_field	=	" count(ID) as ID_count, TASKID";
	$group_par	=	" GROUP BY  TASKID";
	$order_by	=	" order by count(ID) desc";
	if($DB_type==2)
	$records[3]=getJobDetails($production,$job_type_filter,$job_status,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);
	else
	$records[3]=getJobDetails_old($production,$job_type_filter,$job_status,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);
}
//****** query production failed end******/
?>

<?php
if($job_status=="running"||$job_status=="scheduled"||$job_status=="aborted"){

	$cnt_field="SUBT,destce,STATUS";
	$group_par=" ";
	$order_by =" order by SUBT";
	$site_query_ce_plus=$site_query_ce." and (STATUS='$job_status')"; 
	if($DB_type==2)
		$plots_record=getJobDetails_allStatus($production_plus,$prod_failed_job_cond,$prod_success_job_cond,$job_type,$lower_limit,$upper_limit,$site_query_ce_plus,$query_id,$cnt_field,$group_par,$order_by);
	else
		$plots_record=getJobDetails_allStatus_old($production,$prod_failed_job_cond,$prod_success_job_cond,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);

	$cnt_field="destce,count(*) as PRIMDATASET_count ,LOGFILE REGEXP 'mergejob' as LOGFILE_group";
	$group_par=" and STATUS='$job_status' GROUP BY destce,LOGFILE_group";
	$order_by ="";
	if($DB_type==2)
	$records[0]=getJobDetails_allStatus($production_plus,$prod_failed_job_cond,$prod_success_job_cond,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);
	else
	$records[0]=getJobDetails_allStatus_old($production,$prod_failed_job_cond,$prod_success_job_cond,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);
}
if($job_status=="submitted"){
	//$job_status="all";
$cnt_field="SUBT,destce,STATUS";
$group_par=" ";
$order_by =" order by SUBT";
$site_query_ce_plus=$site_query_ce." and (STATUS='running' or STATUS='scheduled' or STATUS='aborted' or STATUS='success' or STATUS='failed')"; 
if($DB_type==2)
	$plots_record=getJobDetails_allStatus($production_plus,$prod_failed_job_cond,$prod_success_job_cond,$job_type,$lower_limit,$upper_limit,$site_query_ce_plus,$query_id,$cnt_field,$group_par,$order_by);
	else
	$plots_record=getJobDetails_allStatus_old($production,$prod_failed_job_cond,$prod_success_job_cond,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);

$cnt_field="SUBT,NEVT,destce,STATUS";
$group_par=" ";
$order_by =" order by SUBT";
if($DB_type==2)
	$plots_record_nevt=getJobDetails($production_plus,$prod_failed_job_cond,$prod_success_job_cond,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);
	else
	$plots_record_nevt=getJobDetails_old($production,$prod_failed_job_cond,$prod_success_job_cond,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);

	$cnt_field="
	destce as destce,
sum(STATUS='running') as sum_running,
sum(STATUS='scheduled') as sum_scheduled,
sum(STATUS='aborted') as sum_aborted,
sum(STATUS='success') as sum_success,
sum(STATUS='failed') as sum_failed,
LOGFILE REGEXP 'mergejob' as LOGFILE_group
";
	$group_par=" GROUP BY destce,LOGFILE_group";
	$order_by ="";
	if($DB_type==2)
	$records=getJobDetails_allStatus($production_plus,$prod_failed_job_cond,$prod_success_job_cond,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);
	else
	$records=getJobDetails_allStatus_old($production,$prod_failed_job_cond,$prod_success_job_cond,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by);
}
?>
