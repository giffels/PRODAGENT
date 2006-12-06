# fills secondary tiers field in MergeSensor database

from MergeSensor.Dataset import Dataset
from MergeSensor.MergeSensorDB import MergeSensorDB
from MergeSensor.MergeSensorError import MergeSensorError, \
                                         InvalidDataset, \
                                         NonMergeableDataset
from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
import logging
from logging.handlers import RotatingFileHandler

import MySQLdb
from ProdAgentDB.Connect import connect

import sys

# create log handler
logHandler = RotatingFileHandler('Logfile',"a", 1000000, 3)
logFormatter = logging.Formatter("%(asctime)s:%(message)s")
logHandler.setFormatter(logFormatter)
logging.getLogger().addHandler(logHandler)
logging.getLogger().setLevel(logging.INFO)
Dataset.setLogging(logging)

database = MergeSensorDB()
Dataset.setDatabase(database)

workflowFile = sys.argv[1]

print "Updating DB for workflow: ", workflowFile

# read the WorkflowSpecFile
try:
    wfile = WorkflowSpec()
    wfile.load(workflowFile)

# wrong dataset file
except Exception, msg:
    print "Error loading workflow specifications from %s: %s" \
          % (workflowFile, msg)
    sys.exit(1)

# get output modules
try:
    outputDatasetsList = wfile.outputDatasets()

    outputModules = [outDS['OutputModuleName'] \
                     for outDS in outputDatasetsList]

    # remove duplicates
    outputModulesList = {}
    for module in outputModules:
        outputModulesList[module] = module
    outputModulesList = outputModulesList.values()

except (IndexError, KeyError):
    print "wrong output dataset specification"

# create a dataset instances for each output module
for outputModule in outputModulesList:

    try:

        # get output datasets
        outputDatasetsList = wfile.outputDatasets()

        # select the ones associated to the current output module
        datasetsToProcess = [ outDS \
                          for outDS in outputDatasetsList \
                          if outDS['OutputModuleName'] == outputModule]

        # the first one
        outputDataset = datasetsToProcess[0]

        # the others
        others = datasetsToProcess[1:]
        secondaryOutputTiers = [outDS['DataTier'] for outDS in others]

    except (IndexError, KeyError):
        print "Wrong output dataset specification"

    # get primary Dataset
    try:
        primaryDataset = outputDataset['PrimaryDataset']
    except KeyError:
        print "Invalid primary dataset specification"

    # get datatier
    try:
        dataTier =  outputDataset['DataTier']
    except KeyError:
        print "DataTier not specified"

    # get processed
    try:
        processedDataset = outputDataset['ProcessedDataset']
    except KeyError:
        print "Invalid processed dataset specification"

    # build dataset name
    name = "/%s/%s/%s" % (primaryDataset, dataTier, \
                          processedDataset)

    print "datasetname: ", name

    secondary = "-".join(secondaryOutputTiers)

    print "    updating secondary tiers: ", secondaryOutputTiers

    # get name components
    (prim, tier, processed) = Dataset.getNameComponents(name)

    # get cursor
    try:
        conn = connect()
        cursor = conn.cursor()
    except MySQLdb.Error, msg:
        print "Error accessing database: %s" % msg
        sys.exit(1)

    # insert dataset information
    sqlCommand = """
                     UPDATE merge_dataset
                        SET updated=current_timestamp,
                            secondarytiers='""" + \
                        secondary + """'
                      WHERE prim='""" + prim + """'
                        AND tier='""" + tier + """'
                        AND processed='""" + processed + """'
                     """


    # execute command
    try:
        cursor.execute(sqlCommand)
        pass
    except MySQLdb.Error, msg:
        print "Error accessing database: %s" % msg

    sqlCommand = "commit"

    # execute command
    try:
        cursor.execute(sqlCommand)
        pass
    except MySQLdb.Error, msg:
        print "Error accessing database: %s" % msg

    print 'done'

