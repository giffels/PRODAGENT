#!/bin/bash

#First put the right paths into your ProdAgentConfig.xml:
more $PBIN/ProdAgentConfig.xml | sed s/_PAUSER/$PRODAGENT_USER/g | sed s/_PAVERSION/$PAVERSION/g | sed s/_PAINITIAL/$PRODAGENT_INITIAL/g > tmp && mv tmp $PBIN/ProdAgentConfig.xml

#Next put the right paths into your SubmitterPluginConfig.xml:
more $PBIN/SubmitterPluginConfig.xml | sed s/_PAUSER/$PRODAGENT_USER/g > tmp && mv tmp $PBIN/SubmitterPluginConfig.xml