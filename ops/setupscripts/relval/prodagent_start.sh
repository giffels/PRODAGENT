#!/usr/local/bin/bash

mkdir -p $PRODAGENT_WORKDIR;

cd $PRODAGENT_WORKDIR;

prodAgentd --restart

prodAgentd --status
reauth 43200 $PRODAGENT_USER --fork

