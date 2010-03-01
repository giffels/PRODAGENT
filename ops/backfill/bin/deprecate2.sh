#!/bin/sh

site=$1

mysql -u root -DProdAgentDB -p98passwd -S $PRODAGENT_ROOT/workdir/mysql/sock -e "update we_Job set retries=3 where workflow_id regexp '.*$site*';"

wfs=`mysql -B -u root -DProdAgentDB -p98passwd -S $PRODAGENT_ROOT/workdir/mysql/sock -e "select distinct workflow_id from jq_queue where workflow_id regexp '.*site*'" | grep -v workflow_id` 
#echo $wfs;
for wf in $wfs; do 
    #new_wf=`echo $wf-deprecated`; 
    #mysql -u root -DProdAgentDB -p98passwd -S ./workdir/mysql/sock -e "update jq_queue set workflow_id='$new_wf' where workflow_id='$wf'";
    mysql -u root -DProdAgentDB -p98passwd -S $PRODAGENT_ROOT/workdir/mysql/sock -e "update jq_queue set status='released' where workflow_id='$wf'";

done

mysql  -u root -DProdAgentDB -p98passwd -S $PRODAGENT_ROOT/workdir/mysql/sock -e "select count(*),status,job_type,workflow_id from jq_queue where workflow_id regexp '.*$site*' and job_type != 'Cleanup' Group by status,job_type,workflow_id order by workflow_id;";
