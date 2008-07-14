#!/usr/bin/env python
"""
_RuntimePSetPrep_

Runtime script that reads in the python PSet file, and
writes out the {{{}}} format PSet file.

May also require some localisation of parameters, expansion
of env vars to be done here in support of chained jobs.

"""

import sys

from CMSConfigTools.CfgInterface import CfgInterface


if __name__ == '__main__':
    inputFile = sys.argv[1]
    outputFile = sys.argv[2]
    
    
    cfgInterface = CfgInterface(inputFile)
    
    
    output = open(outputFile, 'w')
    output.write(cfgInterface.cmsConfig.asConfigurationString())
    output.close()












