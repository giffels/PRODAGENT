#!/usr/local/bin/bash

source /afs/cern.ch/cms/LCG/LCG-2/UI/cms_ui_env.sh
voms-proxy-init -valid 96:00 -voms cms:/cms/Role=production
voms-proxy-info
