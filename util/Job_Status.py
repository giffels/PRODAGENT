#!/bin/python

import os
import sys
pattern = sys.argv[1]
print "JOBQUEUE"
query1 = "select status, job_type, count(*) as total from jq_queue where workflow_id like '%s%s%s' group by status, job_type;" % ('%',pattern,'%')
os.system('mysql -u root -pcmsmcprod --socket=$PRODAGENT_WORKDIR/mysqldata/mysql.sock -D ProdAgentDB -t -e "%s"' % query1)
print "PROCESSING TABLE"
query2 = "select status, count(*) as total  from bl_runningjob where output_dir not like '%sMerge%s' and output_dir like '%s%s%s' group by status order by status desc" % ('%','%','%',pattern,'%') 
os.system('mysql -u root -pcmsmcprod --socket=$PRODAGENT_WORKDIR/mysqldata/mysql.sock -D ProdAgentDB -t -e "%s"' % query2) 
print "MERGE TABLE"
query3 = "select status, count(*) as total  from bl_runningjob where output_dir like '%sMerge%s' and output_dir like '%s%s%s' group by status order by status desc" % ('%','%','%',pattern,'%')
os.system('mysql -u root -pcmsmcprod --socket=$PRODAGENT_WORKDIR/mysqldata/mysql.sock -D ProdAgentDB -t -e "%s"' % query3)


#pattern=sys.argv[1]
#os.system('paquery () { mysql -u root -pi_like_coffe --socket=$PRODAGENT_WORKDIR/mysqldata/mysql.sock -D ProdAgentDB -t -e "$1" ; }')
#os.system('echo "JOBQUEUE"')
#os.system('paquery "select status, job_type, count(*) as total from jq_queue where workflow_id like \'%QCD_Pt170%\' group by status, job_type;"')
#os.system('echo "MERGE TABLE"')
#os.system('paquery "select status, count(*) as total  from bl_runningjob where output_dir not like \'%Merge%\' and output_dir like \'%QCD_Pt170%\'  group by status order by status desc"')
#os.system('paquery "select status, count(*) as total  from bl_runningjob where output_dir like \'%Merge%\' and output_dir like \'%QCD_Pt170%\'  group by status order by status desc"')
#
#echo "JOBQUEUE"
#paquery "select status, job_type, count(*) as total from jq_queue where workflow_id like '%QCD_Pt170%' group by status, job_type;"
#echo "MERGE TABLE"
#paquery "select status, count(*) as total  from bl_runningjob where output_dir not like '%Merge%' and output_dir like '%QCD_Pt170%'  group by status order by status desc"
#echo "UNMERGED JOBS"
#paquery "select status, count(*) as total  from bl_runningjob where output_dir like '%Merge%' and output_dir like '%QCD_Pt170%'  group by status order by status desc"
#  paquery "select destination,status_scheduler,count(*),bt.name from bl_runningjob br join bl_task bt on br.task_id=bt.id where bt.name not like '%CleanUp%' group by status_scheduler,destination"
#fi

