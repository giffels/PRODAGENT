#!/bin/bash

source $PBIN/env_mytestarea.sh;

export PRODAGENT_ROOT=$MYTESTAREA/PRODAGENT
export PATH=$PRODAGENT_ROOT/bin:$PATH
export PYTHONPATH=$PRODAGENT_ROOT/lib
export PUTIL=$PRODAGENT_ROOT/util


. $MYTESTAREA/slc3_ia32_gcc323/cms/prodagent/PRODAGENT_$PAVERSION/etc/profile.d/dependencies-setup.sh

prodAgentd --status

