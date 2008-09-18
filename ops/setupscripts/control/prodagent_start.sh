#!/usr/local/bin/bash

mkdir -p $PRODAGENT_WORKDIR;

cd $PRODAGENT_WORKDIR;

#aklog
prodAgentd --restart

prodAgentd --status
reauth 43200 $PRODAGENT_USER --fork

#killall -e AFScanary.sh
#/data/cmsprod/PAProd/prod/control/AFScanary.sh &
