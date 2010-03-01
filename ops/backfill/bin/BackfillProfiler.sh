#!/bin/sh

source ~/.bashrc >& /dev/null

# Importing the option parser
. ~/backfill/scripts/OptionParser.sh


# Description of the script
Abstract \${0##*/} - Check the status of all backfill workflows.


# Description of how to use the script
Usage \${0##*/} "-p, -w, -s [-u, -m, -f] [workflow1] [workflow2] ..."


# Defining the parameters to be parsed
OptionWithArgument p passwd passwd 'Password to the local sql PA instance.'
OptionWithArgument w workflow workflow 'Workflow id.'
OptionWithArgument s site site 'Site fo the workflow is executed.'
OptionWithArgument i interval interval 'Interval of time in seconds.'
OptionWithArgument u udataset udataset 'Output unmerge dataset produced by the workflow.'
OptionWithArgument m mdataset mdataset 'Output merge dataset produced by the workflow.'
Option f formated formated 'Formated output for monitoring.'

# Generating the option parser
GenerateParser "$@"


# Check for the passwd
if [ -z $passwd ] || [ -z $workflow ]  || [ -z $site ]
then
    Usage
fi

if [ -z $interval ]
then
    interval=1800
fi

while true
do

    ~/backfill/bin/RelocateArchives.sh $site >& /dev/null
    info=`~/backfill/bin/CheckStatus.sh --passwd=$passwd $workflow --formated`

    if [ -n "$udataset" ] && [ -n "$mdataset"  ]
    then
        . ~/backfill/etc/BackfillDBSConfiguration.sh
        localdb_unmerge=`python $DBSCMD_HOME/dbsCommandLine.py --command=search --url=$LocalDBS --query="find sum(file.numevents) where dataset=$udataset"`
        localdb_merge=`python $DBSCMD_HOME/dbsCommandLine.py --command=search --url=$LocalDBS --query="find sum(file.numevents) where dataset=$mdataset"`
    fi

    declare -i lunmerge=${localdb_unmerge##*_}
    declare -i lmerge=${localdb_merge##*_}

    echo $info, $lunmerge, $lmerge

    sleep $interval

done


