#!/usr/local/bin/bash

mkdir -p $PRODAGENT_WORKDIR;

cd $PRODAGENT_WORKDIR;

/afs/usr/local/bin/k5reauth -f -- "prodAgentd --restart"

prodAgentd --status
