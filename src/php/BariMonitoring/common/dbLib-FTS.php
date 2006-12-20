<?php
$ADODB_CACHE_DIR = "/tmp/adodb_cache";
$ADODB_ACTIVE_CACHESECS = 60;
//include_once("../Conf.php");
include_once "local/monParams-FTS.php";
include_once "../adodb/adodb.inc.php";
function dbLibConnect() {
        global $DB_NAME, $DB_USER, $DB_PASS, $DB_SPEC;
        $db=NewADOConnection('mysql');
        //$db->debug = true;
        $db->Connect($DB_SPEC,$DB_USER,$DB_PASS,$DB_NAME);
        return $db;
}
function dbLibConnect_prodAgent() {
        global $DB_PA_NAME, $DB_USER, $DB_PASS, $DB_SPEC;
        $db=NewADOConnection('mysql');
        //$db->debug = true;
        $db->Connect($DB_SPEC,$DB_USER,$DB_PASS,$DB_PA_NAME);
        return $db;
}

function getAllCE($production){
		$query="
		select distinct destce from (
		(
			select distinct DEST_CE as destce 
			from SCHED_edg,JOB 
			where JOB.LOG_FILE like '%$production%' and JOB.TASK_ID=SCHED_edg.TASK_ID
		)
		) as temp_table;
		";

        $db=dbLibConnect();
        $result=$db->CacheExecute(3600*24*30*12,$query);
        return $result;
}

function get_stream($production){
	$query="
select  
distinct SUBSTRING_INDEX(merge_dataset.processed,'-11',1) as processed, 
merge_dataset.id as id,  
SUBSTRING_INDEX(merge_outputfile.mergejob,'-mergejob-xxx',1) as TASK_NAME  
from  
merge_dataset, merge_outputfile 
where  
merge_dataset.workflow='$production' and merge_outputfile.dataset=merge_dataset.id order by id;
	";
        $db=dbLibConnect_prodAgent();
        $result=&$db->CacheExecute(3600,$query);
	if (!$result) {
 		//print $db->ErrorMsg(); 
		return 0;
	}
	else 
		return $result;
}
function getJobDetails_old($production,$job_type_filter,$job_status,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by){
	$debug = $GLOBALS['debug'];
	if($job_type=='prod')
		$job_type=" and JOB.LOG_FILE REGEXP 'mergejob'=0";
	else
		$job_type=" and JOB.LOG_FILE REGEXP 'mergejob'=1";

	$query="
		SELECT
		from (
				(
		select	
SCHED_edg.SCHED_STATUS as STATUS,SCHED_edg.TASK_ID as TASKID,
SCHED_edg.CHAIN_ID as CHAIN_ID,SCHED_edg.DEST_CE as destce, cmssw.TASK_name as TASK_name, cmssw.TASK_ID as PRIMDATASET ,cmssw.ID as ID,N_EVT as NEVT,cmssw.TASK_EXIT as TASKEXIT,comment, EXEC_HOST,N_RUN as NRUN,
SUB_TIME as SUBT,START_T,STOP_T,JOB.LOG_FILE as LOGFILE,LFN as LFN ,SE_OUT as SE_OUT,SCHED_ID as SCHEDID 
/*,TASK_HEAD.TASK_NAME */
from SCHED_edg,JOB,cmssw,TASK_HEAD 
WHERE 
SCHED_edg.TASK_ID=TASK_HEAD.ID AND SCHED_edg.TASK_ID=JOB.TASK_ID AND 
SCHED_edg.TASK_ID=cmssw.TASK_ID AND SCHED_edg.ID=JOB.ID AND 
SCHED_edg.ID=cmssw.ID AND 
cmssw.PRIMDATASET='$production' $site_query_ce $job_type
				)
				union
			(
select	
SCHED_edg.SCHED_STATUS as STATUS,SCHED_edg.TASK_ID as TASKID,
SCHED_edg.CHAIN_ID as CHAIN_ID,SCHED_edg.DEST_CE as destce, cmssw.TASK_name as TASK_name, cmssw.TASK_ID as PRIMDATASET ,cmssw.ID as ID,N_EVT as NEVT,cmssw.TASK_EXIT as TASKEXIT,comment, EXEC_HOST,N_RUN as NRUN,
SUB_TIME as SUBT,START_T,STOP_T,JOB.LOG_FILE as LOGFILE,LFN as LFN ,SE_OUT as SE_OUT,SCHED_ID as SCHEDID 
/*,TASK_HEAD.TASK_NAME */
from cmssw,JOB,SCHED_edg 
WHERE 
JOB.TASK_ID=cmssw.TASK_ID AND JOB.ID=cmssw.ID AND SCHED_edg.TASK_ID=cmssw.TASK_ID AND SCHED_edg.ID=cmssw.ID
and SCHED_edg.SCHED_STATUS='Aborted'  $job_type

				)
				)
				as jam
				";

	if($job_status=="success"){		
		//$site_query_ce=str_replace ("SCHED_edg.dest_ce","destce", $site_query_ce);
		//$merge_success_job_cond=str_replace ("ENDED_cmssw.TASK_exit","TASKEXIT", $merge_success_job_cond);
		$query.=" where 
			SUBT>'$lower_limit' AND SUBT<'$upper_limit' $query_id 
			$merge_success_job_cond and TASKEXIT=0";
		//$order_by=" order by TASKID";	
	}	

	elseif($job_status=="failed"){		
		$site_query_ce=str_replace ("SCHED_edg.dest_ce","destce", $site_query_ce);
		$query.=" where SUBT>'$lower_limit' AND SUBT<'$upper_limit' $site_query_ce $query_id and TASK_name='$job_status'";
	}	

	elseif($job_status=="submitted"){		
		$site_query_ce=str_replace ("SCHED_edg.dest_ce","destce", $site_query_ce);
		$query.=" where 
			SUBT>'$lower_limit' AND SUBT<'$upper_limit' $site_query_ce $query_id ";
		//$order_by="order by TASKID";	
			//echo $query;
	}		

	elseif($job_status=="running"){		
		$site_query_ce=str_replace ("SCHED_edg.dest_ce","destce", $site_query_ce);
		$query.=" where 
			SUBT>'$lower_limit' AND SUBT<'$upper_limit' $site_query_ce $query_id and STATUS='$job_status'";
		//$order_by="order by TASKID";	
	}		

	elseif($job_status=="scheduled"){		
		$site_query_ce=str_replace ("SCHED_edg.dest_ce","destce", $site_query_ce);
		$query.=" where SUBT>'$lower_limit' AND SUBT<'$upper_limit' $site_query_ce $query_id ";
		//$order_by="order by cmssw.TASK_ID,cmssw.ID";	
	}		

	elseif($job_status=="aborted") {		
		$site_query_ce=str_replace ("SCHED_edg.dest_ce","destce", $site_query_ce);
		$query.=" where 
			SUBT>'$lower_limit' AND SUBT<'$upper_limit' $site_query_ce $query_id and STATUS='$job_status'";
		//$order_by=" order by TASKID";	
	}
	if($group_par!=''){
		$query.=" ".$group_par;
	}
	if($cnt_field!=''){
		$query=str_replace("SELECT"," SELECT $cnt_field ", $query);
	}
		
	if($order_by!='')
		$query.=" ".$order_by;
	//echo "CSA06_devel2/common/dbLib-FTS.php :<br>".$query."<hr>";//exit(0);
if($debug!=''){
?>
<textarea cols=120 rows=50 style="visibility:visible" id="query_debug"><?=$query?></textarea>
<?php
}
        $db=dbLibConnect();
        $result=&$db->CacheExecute(3600*24*30*12,$query);
        return $result;
}
function getJobDetails($production,$job_type_filter,$job_status,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by){
	//echo "<hr>: getJobDetails: $job_status";
	$debug = $GLOBALS['debug'];
	$merged_dataset = $GLOBALS['merged_dataset'];
	$stream_merge	= $GLOBALS['stream_merge'];
	//echo "common/dbLib-FTS.php getJobDetails<br>".$merged_dataset;
	if($job_type=='prod'){
		$job_type_filter=' not like \'%mergejob%\'';
		$merged_dataset="ALL";
	}
	else
		$job_type_filter='  like \'%mergejob%\'';

	$query="
		SELECT
		from (
				(
				select
				cmssw.PRIMDATASET as PRIMDATASET,N_EVT as NEVT,
				SCHED_edg.CHAIN_ID as CHAIN_ID,cmssw.ID as ID,
				cmssw.TASK_name as TASK_name,comment, EXEC_HOST, SUB_T ,START_T,STOP_T,SE_OUT,SCHED_edg.dest_ce as destce,
				JOB.LOG_FILE as LOGFILE,JOB.SUB_T as SUBT,SCHED_edg.SCHED_STATUS as STATUS,SCHED_edg.TASK_ID as TASKID,cmssw.TASK_EXIT as TASKEXIT,SCHED_ID as SCHEDID
				 from
				 JOB ,SCHED_edg,cmssw
				 WHERE
				 SCHED_edg.ID=cmssw.ID and SCHED_edg.TASK_ID=cmssw.TASK_ID and
				 JOB.TASK_ID=SCHED_edg.TASK_ID AND JOB.ID=SCHED_edg.ID and JOB.LOG_FILE $job_type_filter and
				 JOB.LOG_FILE like  '%$production%'
				)
				union
				(
				 select
				ENDED_cmssw.PRIMDATASET as PRIMDATASET,N_EVT as NEVT,
				 ENDED_SCHED_edg.CHAIN_ID as CHAIN_ID,ENDED_SCHED_edg.ID as ID,
				 ENDED_cmssw.TASK_name as TASK_name,comment, EXEC_HOST, SUB_T ,START_T,STOP_T,SE_OUT,
				 ENDED_SCHED_edg.dest_ce as destce,ENDED_JOB.LOG_FILE as LOGFILE,ENDED_JOB.SUB_T as SUBT,
				 ENDED_SCHED_edg.SCHED_STATUS as STATUS,ENDED_SCHED_edg.TASK_ID as TASKID,ENDED_cmssw.TASK_EXIT as TASKEXIT,SCHED_ID as SCHEDID
				 from
				 ENDED_JOB,ENDED_SCHED_edg,ENDED_cmssw
				 WHERE
				 ENDED_SCHED_edg.ID=ENDED_cmssw.ID and ENDED_SCHED_edg.TASK_ID=ENDED_cmssw.TASK_ID and
				 ENDED_JOB.TASK_ID=ENDED_SCHED_edg.TASK_ID AND ENDED_JOB.ID=ENDED_SCHED_edg.ID and ENDED_JOB.LOG_FILE $job_type_filter and
				 ENDED_JOB.LOG_FILE like  '%$production%'
				))
				as jam
				";

	if($job_status=="success"){		
		$site_query_ce=str_replace ("SCHED_edg.dest_ce","destce", $site_query_ce);
		$merge_success_job_cond=str_replace ("ENDED_cmssw.TASK_exit","TASKEXIT", $merge_success_job_cond);
		$query.=" where 
			SUBT>'$lower_limit' AND SUBT<'$upper_limit' $site_query_ce $query_id 
			$merge_success_job_cond and TASKEXIT=0";
		//$order_by=" order by TASKID";	
	}	

	elseif($job_status=="failed"){		
		$site_query_ce=str_replace ("SCHED_edg.dest_ce","destce", $site_query_ce);
		$query.=" where SUBT>'$lower_limit' AND SUBT<'$upper_limit' $site_query_ce $query_id and TASK_name='$job_status'";
	}	

	elseif($job_status=="submitted"){		
		$site_query_ce=str_replace ("SCHED_edg.dest_ce","destce", $site_query_ce);
		$query.=" where 
			SUBT>'$lower_limit' AND SUBT<'$upper_limit' $site_query_ce $query_id ";
		//$order_by="order by TASKID";	
			//echo $query;
	}		

	elseif($job_status=="running"){		
		$site_query_ce=str_replace ("SCHED_edg.dest_ce","destce", $site_query_ce);
		$query.=" where 
			SUBT>'$lower_limit' AND SUBT<'$upper_limit' $site_query_ce $query_id and STATUS='$job_status'";
		//$order_by="order by TASKID";	
	}		

	elseif($job_status=="scheduled"){		
		$site_query_ce=str_replace ("SCHED_edg.dest_ce","destce", $site_query_ce);
		$query.=" where STATUS='$job_status' and  SUBT>'$lower_limit' AND SUBT<'$upper_limit' $site_query_ce $query_id ";
		//$order_by="order by cmssw.TASK_ID,cmssw.ID";	
	}		

	elseif($job_status=="aborted") {		
		$site_query_ce=str_replace ("SCHED_edg.dest_ce","destce", $site_query_ce);
		$query.=" where 
			SUBT>'$lower_limit' AND SUBT<'$upper_limit' $site_query_ce $query_id and STATUS='$job_status'";
		//$order_by=" order by TASKID";	
	}
	if($merged_dataset!='' && $merged_dataset!='ALL'){
		$merged_dataset_where = rtrim($stream_merge[$merged_dataset],',');
		$merged_dataset_where = " and ( LOGFILE like '%".str_replace(",","%' OR LOGFILE like '%",$merged_dataset_where)."%')";
		//echo "<hr>merged_dataset_where:<br>".$merged_dataset_where;
		$query.=$merged_dataset_where;
	}
	if($group_par!=''){
		$query.=" ".$group_par;
	}
	if($cnt_field!=''){
		$query=str_replace("SELECT"," SELECT $cnt_field ", $query);
	}
		
	if($order_by!='')
		$query.=" ".$order_by;
	//echo "CSA06_devel2/common/dbLib-FTS.php :<br>".$query."<hr>";//exit(0);
if($debug!=''){
?>
<textarea cols=120 rows=50 style="visibility:visible" id="query_debug"><?=$query?></textarea>
<?php
}
        $db=dbLibConnect();
	$result = 	&$db->CacheExecute(600,$query); 
        return $result;
}
function getJobDetails_allStatus($production_plus,$prod_failed_job_cond,$prod_success_job_cond,$job_type,$lower_limit,$upper_limit,$ended_site_query_ce,$query_id,$cnt_field,$group_par,$order_by){
	$debug = $GLOBALS['debug'];
	$merged_dataset = $GLOBALS['merged_dataset'];
	$stream_merge	= $GLOBALS['stream_merge'];
	//echo "common/dbLib-FTS.php getJobDetails_allStatus<br>".$merged_dataset;
	$ended_site_query_ce=str_replace("ENDED_SCHED_edg.dest_ce"," destce ", $ended_site_query_ce);
	$ended_site_query_ce=str_replace("SCHED_edg.dest_ce"," destce",$ended_site_query_ce);
	$ended_site_query_ce=" where 1 ".$ended_site_query_ce;
	if($production_plus=="ALL-%")$production_plus='%%';
$query="
SELECT
from ((
\n/*job scheduled e running*/
select
SCHED_edg.dest_ce as destce,JOB.LOG_FILE as LOGFILE,JOB.SUB_T as SUBT,SCHED_edg.SCHED_STATUS as STATUS 
from 
JOB ,SCHED_edg
WHERE
JOB.TASK_ID=SCHED_edg.TASK_ID AND JOB.ID=SCHED_edg.ID AND
JOB.LOG_FILE like '$production_plus' 
\n)
union
(
\n/* cancelled and aborted*/
select
ENDED_SCHED_edg.dest_ce as destce,ENDED_JOB.LOG_FILE as LOGFILE,ENDED_JOB.SUB_T as SUBT,ENDED_SCHED_edg.SCHED_STATUS as STATUS
from ENDED_JOB,ENDED_SCHED_edg
WHERE
ENDED_JOB.TASK_ID=ENDED_SCHED_edg.TASK_ID AND ENDED_JOB.ID=ENDED_SCHED_edg.ID AND
ENDED_JOB.LOG_FILE like '$production_plus' 
\n)
union(
\n/* failed jobs*/
select
ENDED_SCHED_edg.dest_ce as destce,ENDED_JOB.LOG_FILE as LOGFILE,ENDED_JOB.SUB_T as SUBT,ENDED_cmssw.TASK_NAME as STATUS
from ENDED_JOB,ENDED_SCHED_edg,ENDED_cmssw
WHERE
ENDED_JOB.TASK_ID=ENDED_cmssw.TASK_ID AND ENDED_JOB.ID=ENDED_cmssw.ID AND ENDED_SCHED_edg.TASK_ID=ENDED_cmssw.TASK_ID AND 
ENDED_SCHED_edg.ID=ENDED_cmssw.ID $prod_failed_job_cond 
AND ENDED_JOB.LOG_FILE like 
'$production_plus' 
\n)
union(
\n/*select success job*/
select
ENDED_SCHED_edg.dest_ce as destce,ENDED_JOB.LOG_FILE as LOGFILE,ENDED_JOB.SUB_T as SUBT,ENDED_cmssw.TASK_NAME as STATUS
from ENDED_cmssw,ENDED_JOB,ENDED_SCHED_edg 
where
ENDED_JOB.TASK_ID=ENDED_cmssw.TASK_ID AND ENDED_JOB.ID=ENDED_cmssw.ID AND ENDED_SCHED_edg.TASK_ID=ENDED_cmssw.TASK_ID AND ENDED_SCHED_edg.ID=ENDED_cmssw.ID  
$prod_success_job_cond AND ENDED_JOB.LOG_FILE like '$production_plus' 
)
) 
as jam 
$ended_site_query_ce
";
	if($job_type=='prod'){
		$query.=" and LOGFILE REGEXP 'mergejob' = 0";
	}
	if($job_type=='merge'){
		$query.=" and LOGFILE REGEXP 'mergejob' = 1";
	}
	if($merged_dataset!='' && $merged_dataset!='ALL'){
		$merged_dataset_where = rtrim($stream_merge[$merged_dataset],',');
		$merged_dataset_where = " and ( LOGFILE like '%".str_replace(",","%' OR LOGFILE like '%",$merged_dataset_where)."%'";
			if(strpos($merged_dataset,"_")>0){
				$merged_dataset_where.=" or LOGFILE  not like '%mergejob%'";
			}

		$merged_dataset_where.=" )";
		$query.=$merged_dataset_where;
	}
	if($lower_limit!='' && $upper_limit!='')
		$query.=" and SUBT>'$lower_limit' AND SUBT<'$upper_limit' ";
	
	/********** **************/
	if($group_par!=''){
		$query.=" ".$group_par;
	}
	if($cnt_field!=''){
		$query=str_replace("SELECT"," SELECT $cnt_field ", $query);
	}
		
	if($order_by!='')
		$query.=" ".$order_by;
if($debug!=''){
?>
<textarea cols=120 rows=50 style="visibility:visible" id="query_debug"><?=$query?></textarea>
<script>
//document.getElementByID("query_debug").visibility="visible";
//alert(document.getElementByID("query_debug").value);
//document.getElementByID("query_debug").value="<?=$query?>";
</script>
<?php
}
        $db=dbLibConnect();
	$result = 	&$db->CacheExecute(600,$query); 
	if (!$result) {
		$result="false";
	}
	else
		return $result;
}

function selectSite_SCHED_edg($order_by,$status,$date_start_sec,$date_stop_sec,$DEST_CE_frm) {
        $db=dbLibConnect();
	$query="
	SELECT 
	SE_SIZE, (STOP_T-START_T) as exe_time,N_EVT, CPU,EXEC_HOST,SCHED_ID,SCHED_edg.CHAIN_ID,DEST_CE,SCHED_edg.ID,SCHED_edg.LAST_T,SCHED_STATUS,SCHED_edg.TASK_ID 
	from
	SCHED_edg,JOB,cmssw 
	where 
	JOB.TASK_ID=SCHED_edg.TASK_ID and SCHED_edg.TASK_ID=cmssw.TASK_ID and 
	SCHED_STATUS like '%$status%'
	and
	DEST_CE like '%$DEST_CE_frm%'
	and
	SCHED_edg.LAST_T < $date_stop_sec and SCHED_edg.LAST_T > $date_start_sec
	
	$order_by";
        $result=$db->Execute($query);
        return $result;
}

function select_DEST_CE(){
        $db=dbLibConnect();
        $result=$db->Execute("SELECT distinct DEST_CE FROM SCHED_edg");
        return $result;
}

function selectSite_dest() {
        $db=dbLibConnect();
        $result=$db->Execute("SELECT dest FROM \"destination\"");
        return $result;
}

function selectSite_source() {
        $db=dbLibConnect();
        $result=$db->Execute("SELECT host FROM \"source\"");
        return $result;
}
function getJobDetails_allStatus_old($production,$prod_failed_job_cond,$prod_success_job_cond,$job_type,$lower_limit,$upper_limit,$site_query_ce,$query_id,$cnt_field,$group_par,$order_by){
	$site_query_ce=str_replace("ENDED_SCHED_edg.dest_ce","SCHED_edg.dest_ce",$site_query_ce);
	$debug = $GLOBALS['debug'];
	$ended_site_query_ce=str_replace("AND ENDED_SCHED_edg.dest_ce"," destce ", $ended_site_query_ce);
	$ended_site_query_ce=" where 1 ".$ended_site_query_ce;
$query="
SELECT
from ((
\n/*Jobs submitted,  scheduled, running, aborted*/
select
SCHED_edg.DEST_CE as destce,JOB.LOG_FILE as LOGFILE, SCHED_edg.SCHED_STATUS as STATUS,JOB.SUB_TIME as SUBT
from cmssw,JOB,SCHED_edg 
WHERE 
JOB.TASK_ID=cmssw.TASK_ID AND JOB.ID=cmssw.ID AND SCHED_edg.TASK_ID=cmssw.TASK_ID AND SCHED_edg.ID=cmssw.ID
and SCHED_edg.SCHED_STATUS='Aborted' $site_query_ce
\n)
union
(
\n/* failed jobs*/
select  
SCHED_edg.DEST_CE as destce,
JOB.LOG_FILE  as LOGFILE, cmssw.TASK_NAME as STATUS,JOB.SUB_TIME as SUBT
from cmssw,JOB,SCHED_edg 
WHERE 
JOB.TASK_ID=cmssw.TASK_ID AND JOB.ID=cmssw.ID AND SCHED_edg.TASK_ID=cmssw.TASK_ID AND SCHED_edg.ID=cmssw.ID
$prod_failed_job_cond AND  cmssw.PRIMDATASET='$production' $site_query_ce
\n)
union(
\n/*select success job*/
select 
SCHED_edg.DEST_CE as destce,JOB.LOG_FILE as LOGFILE ,cmssw.TASK_NAME as STATUS,JOB.SUB_TIME as SUBT from cmssw,JOB,SCHED_edg
WHERE 
JOB.TASK_ID=cmssw.TASK_ID AND JOB.ID=cmssw.ID AND SCHED_edg.TASK_ID=cmssw.TASK_ID AND SCHED_edg.ID=cmssw.ID  
AND cmssw.TASK_exit=0 AND cmssw.PRIMDATASET='$production' $site_query_ce
)
) 
as jam 
$ended_site_query_ce
";
	if($job_type=='prod'){
		$query.=" and LOGFILE REGEXP 'mergejob' = 0";
	}
	if($job_type=='merge'){
		$query.=" and LOGFILE REGEXP 'mergejob' = 1";
	}

	if($group_par!=''){
		$query.=" ".$group_par;
	}
	if($cnt_field!=''){
		$query=str_replace("SELECT"," SELECT $cnt_field ", $query);
	}
		
	if($order_by!='')
		$query.=" ".$order_by;
if($debug!=''){
?>
<textarea cols=120 rows=50 style="visibility:visible" id="query_debug"><?=$query?></textarea>
<script>
//document.getElementByID("query_debug").visibility="visible";
//alert(document.getElementByID("query_debug").value);
//document.getElementByID("query_debug").value="<?=$query?>";
</script>
<?php
}
        $db=dbLibConnect();
        $result=&$db->CacheExecute(3600*24*30*12,$query);
	if (!$result) {
		$result="false";
	}
	else
		return $result;
        return $result;

}
?>
