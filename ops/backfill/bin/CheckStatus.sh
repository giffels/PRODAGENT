#!/bin/sh

source ~/.bashrc >& /dev/null

# Importing the option parser
. ~/backfill/scripts/OptionParser.sh


# Description of the script
Abstract \${0##*/} - Check the status of all backfill workflows.


# Description of how to use the script
Usage \${0##*/} "-p [-s, -f] [workflow1] [workflow2] ..."


# Defining the parameters to be parsed
OptionWithArgument s pattern pattern 'Filter workflow id by a given pattern.'
Option f formated formated 'Formated output for monitoring.'


# Generating the option parser
GenerateParser "$@"


passwd="98passwd";
# Check for the passwd
if [ -z $passwd ]
then
    Usage
fi

# List of workflows from db or provided by the user
if (( ${#arg[@]} < 1 ))
then
    if [ -z $pattern ]
    then
        workflows=`mysql -u root -D ProdAgentDB -p${passwd} -S ${PRODAGENT_ROOT}/workdir/mysql/sock -B -e "select workflow_name from prodmon_Workflow;" | grep backfill`
    else
        workflows=`mysql -u root -D ProdAgentDB -p${passwd} -S ${PRODAGENT_ROOT}/workdir/mysql/sock -B -e "select workflow_name from prodmon_Workflow;" | grep backfill | grep ${pattern}`
    fi
else
    workflows=${arg[@]}
fi


for wf in $workflows
do
    era=`echo ${wf} | awk '{ match( $0,"([[:digit:]])-(.*)_(.*)_(.*)_backfill-(.*)",a ); print a[1]}'`;
    site=`echo ${wf} | awk '{ match( $0,"([[:digit:]])-(.*)_(.*)_(.*)_backfill-(.*)",a ); print a[3]}'`;
    version=`echo ${wf} | awk '{ match( $0,"([[:digit:]])-(.*)_(.*)_(.*)_backfill-(.*)",a ); print a[5]}'`;

    successes=0;
    for tb in ${PRODAGENT_ROOT}/archive/success/${site}/${era}/${version}/*.tar.gz; do 
        if [ -e ${tb} ]; then  
            (( successes++ ));
        fi
    done;

    failures=0;
    for tb in ${PRODAGENT_ROOT}/archive/failure/${site}/${era}/${version}/*.tar.gz; do 
        if [ -e ${tb} ]; then  
            (( failures++ ));
        fi
    done;

    running=`condorqrunning | grep -c ${wf}`;
    idle=`condorq | grep -c ${wf}`;
    (( idle -= ${running} ))

    new=`mysql  -u root -D ProdAgentDB -p${passwd} -S ${PRODAGENT_ROOT}/workdir/mysql/sock -B -e "select count(*) from jq_queue where workflow_id='${wf}' and status='new';" | tail -1`
    released=`mysql  -u root -D ProdAgentDB -p${passwd} -S ${PRODAGENT_ROOT}/workdir/mysql/sock -B -e "select count(*) from jq_queue where workflow_id='${wf}' and status='released';" | tail -1`

    new_p=`mysql  -u root -D ProdAgentDB -p${passwd} -S ${PRODAGENT_ROOT}/workdir/mysql/sock -B -e "select count(*) from jq_queue where workflow_id='${wf}' and status='new' and job_type='Processing';" | tail -1`
    released_p=`mysql  -u root -D ProdAgentDB -p${passwd} -S ${PRODAGENT_ROOT}/workdir/mysql/sock -B -e "select count(*) from jq_queue where workflow_id='${wf}' and status='released' and job_type='Processing';" | tail -1`

    new_m=`mysql  -u root -D ProdAgentDB -p${passwd} -S ${PRODAGENT_ROOT}/workdir/mysql/sock -B -e "select count(*) from jq_queue where workflow_id='${wf}' and status='new' and job_type='Merge';" | tail -1`
    released_m=`mysql  -u root -D ProdAgentDB -p${passwd} -S ${PRODAGENT_ROOT}/workdir/mysql/sock -B -e "select count(*) from jq_queue where workflow_id='${wf}' and status='released' and job_type='Merge';" | tail -1`

    declare -i total_p=0
    let "total_p = new_p + released_p"

    declare -i total_m=0
    let "total_m = new_m + released_m"

    if [ -n "$formated" ]
    then
        date=`date`
        echo "$date, $wf, $new_p, $new_m, $released_p, $released_m, $running, $idle, $successes, $failures"
    else
        echo 
        echo "WF: ${wf}"
        echo "====================================================="
        echo "processing: ${total_p}"
        echo "merge: ${total_m}"
        echo "queued: ${new} (${new_p} processing, ${new_m} merge)"
        echo "released: ${released} (${released_p} processing, ${released_m} merge)"
        echo "-----------------------------------------------------"
        echo "running: ${running}" 
        echo "idle: ${idle}" 
        echo "-----------------------------------------------------"
        echo "successes: ${successes}"
        echo "failures: ${failures}"
        echo "====================================================="
        echo 
    fi

done
