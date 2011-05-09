#!/usr/bin/env python
import os
import sys
import getopt
import popen2
import threading
#import Queue
import time

from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader
from ProdAgentCore.Configuration import loadProdAgentConfiguration
from MessageService.MessageService import MessageService

__version__ = "$Revision: 1.10 $"
__revision__ = "$Id: workflowStatus.py,v 1.10 2010/03/05 10:55:34 direyes Exp $"
__author__ = "direyes@cern.ch"

def main(argv):
    """
    workflowStatus.py

    Usage:

    Mandatory:
    --input-file,-i <FILE>:
        Path to a txt file having the dataset paths (one per line) to be watched.

    Optional:
    --url,-u <DBSURL>:
        Local DBS url.
    --actions,-a <comma,sep,list>:
        Actions to execute: merge, migrate, inject
    --print,-p:
        Print status.
    --help,-h:
        Print this.
    --debug,-d
        Print debug info.


    """
    valid = ['help', 'debug', 'input-file=', 'url=', 'actions=', 'print',
             'fast']
    valid_short = 'hdi:u:a:pf'

    # The order should be like this!!
    valid_actions = ['merge', 'migrate', 'inject']

    debug = False
    arguments = {'inputFile': None}
    print_report = False
    fast = False
    actions_list = []

    try:
        opts, args = getopt.getopt(argv, valid_short, valid)
    except getopt.GetoptError, ex:
        print main.__doc__
        print ''
        print str(ex)
        print ''
        sys.exit(1)

    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print main.__doc__
            sys.exit()
        if opt in ("-d", "--debug"):
            debug = True
        if opt in ("-i", "--input-file"):
            arguments['inputFile'] = arg
        if opt in ("-u", "--url"):
            arguments['dbsURL'] = arg
        if opt in ("-a", "--actions"):
            actions_arg = [x.strip() for x in arg.split(',') if x]
            for action in actions_arg:
                if not valid_actions.count(action):
                    print "Action not recognized: %s" % action
                    print "Valid actions: %s" % ", ".join(valid_actions)
                    sys.exit(1)
            for action in valid_actions:
                if actions_arg.count(action):
                    actions_list.append(action)
        if opt in ("-p", "--print"):
            print_report = True
        if opt in ("-f", "--fast"):
            fast = True

    if arguments['inputFile'] is None:
        print main.__doc__
        msg = "--input-file"
        print "Missing parameter: " + msg
        sys.exit(1)

    Datasets = RelValDatasets(**arguments)
    Datasets.debug = debug
    Datasets.with_threads = fast
    Datasets.readFile()
    if debug:
        print Datasets.datasets
    Datasets.queryDBS()
    if debug:    
        print Datasets.datasets
    if print_report:
        Datasets.printReport()
    for action in actions_list:
        if action == 'merge':
            Datasets.forceMerge()
        if action == 'migrate':
            Datasets.migrateToGlobal()
        if action == 'inject':
            Datasets.injectIntoPhEDEx()


def getEvents(reader, dataset):
    """getEvents

    Returns number of events and if all the datasets blocks were injected
    into PhEDEx. It relies on the fact that blocks known by PhEDEx have a
    storage element associated. If the dataset is not at the DBS instance
    -1 is returned. 
    """
    primds = dataset.split('/')[1]
    procds = dataset.split('/')[2]
    tier = dataset.split('/')[3]
    if not reader.matchProcessedDatasets(primds, tier, procds):
        #if self.debug:
            #print "MMMMM:%s" % (dataset)
        # Dataset is not in DBS instance.
        return [-1, False]
    blocks = reader.getFileBlocksInfo(dataset)
    in_PhEDEx = True
    total_events = -1
    for block in blocks:
        if total_events == -1:
            total_events = 0
        total_events += block['NumberOfEvents']
        try:
            if not block['StorageElementList'][0]['Name']:
                in_PhEDEx = False
        except:
            in_PhEDEx = False
    return [total_events, in_PhEDEx]


class Queue(list):
    """Queue

    Homemade queue for being used by threads. It's just a list with some extra
    methods.
    """
    def __init__(self):
        list.__init__(self)

    def put(self, item):
        self.insert(0, item)

    def get(self):
        try:
            return self.pop()
        except IndexError:
            return None

    def empty(self):
        return not (self.__len__() and True or False)

    def qsize(self):
        return self.__len__()


class DBSQueryThread(threading.Thread):
    """DBSQueryThread

    Thread for querying DBS.

    """
    def __init__(self, input_queue, output_queue, lock):
        self.idle = True
        self.kill_me = False
        self.lock = lock
        self.__input_queue = input_queue
        self.__output_queue = output_queue
        threading.Thread.__init__(self)

    def run(self):
        # This thread will run forever. Well, the whole time the script is 
        # running and the kill_me attribute is false.
        while not self.kill_me:
            self.idle = True
            if not self.__input_queue.empty():
                self.idle = False
                self.lock.acquire()
                item = self.__input_queue.get()
                self.lock.release()
                if item is not None:
                    result = []
                    # This will help me to identify the dataset.
                    result = [item['id']]
                    reader = DBSReader(item['url'])
                    result.append(getEvents(reader, item['dataset']))
                    self.lock.acquire()
                    self.__output_queue.put(result)
                    self.lock.release()


class RelValDatasets:
    """RelValDatasets

    Main Class for querying and display the production status of the input
    datasets.
    """
    def __init__(self, **args):
        self.ms = MessageService()
        self.ms.registerAs("Test")
        self.datasets = []
        self.debug = False
        self.parameters = {}
        self.parameters['inputFile'] = None
        self.parameters.update(args)
        self.with_threads = False
        if self.parameters.get('dbsURL', None) is not None:
            self.dbs_url = self.parameters['dbsURL']
        else:
            self.getLocalDBS()
        self.getGlobalDBS()

    def readFile(self):
        """readFile()

        Reads the provided file. It produces a list of dictionaries, each
        entry contains:
        - name
        - expectedEvents
        - unmergedName
        """
        self.max_length = 0
        file = open(self.parameters['inputFile'], 'r')
        if self.debug:
            print "Parsing file: %s" % self.parameters['inputFile']
        for line in file:
            if line.startswith('#'):                # Any comment?
                continue
            dataset = {}
            read_info = [
                x.strip() for x in line.strip("\n").strip().split() if x != ""]
            if len(read_info) == 1:                 # No expected events found
                read_info = [9000, read_info[0]]
            if not read_info:                       # Blank line?
                continue
            dataset['name'] = read_info[1]
            if len(dataset['name']) > self.max_length:
                self.max_length = len(dataset['name'])
            dataset['expectedEvents'] = int(read_info[0])
            # Producing the unmerged dataset name
            unmerged_parts = [x for x in dataset['name'].split('/') if x != ""]
            dataset['unmergedName'] = "/".join(['', unmerged_parts[0],
                "-".join([unmerged_parts[1], 'unmerged']), unmerged_parts[2]])
            self.datasets.append(dataset)
            if self.debug:
                print dataset 
        file.close()

    def startQueryingService(self, threads=4):
        """startQueryingService

        Launches threads for querying to DBS. The number of threads can be
        controlled by the threads argument. This is a pool of threads that
        pick up jobs from an input pool and store result in an output pool.
        """
        # Declares queue which will be later used by the threads
        self.input_queue = Queue()
        # Queue where the results will be stored
        self.output_queue = Queue()
        self.lock = threading.Lock()
        self.threads = {}
        for i in range(threads):
            if self.debug:
                print "Starting querying sevice number", i
            self.threads[i] = DBSQueryThread(self.input_queue,
                                             self.output_queue,
                                             self.lock)
            self.threads[i].start()

    def areThreadsRuninng(self):
        """areThreadsRuninng

        Verifies if the DBS threads are still running. It first verifies if the
        input queue is empty. A small sleep time is needed in order to let the
        threads run freely.
        """
        time.sleep(1)
        if self.input_queue.empty():
            for i in self.threads:
                if not self.threads[i].idle:
                    return True
            return False
        else:
            return True

    def killThreads(self):
        """killThreads

        Kill all the threads setting its killing flag to True. It seems like
        this method is needed otherwise I can't get the shell back after 
        executing the threads. Anyways, I don't like to have threads running
        if I am not using them.
        """
        for i in self.threads:
            self.threads[i].kill_me = True

    def waitForDrainQueue(self):
        """waitForDrainQueue

        This method loops until the thread pool is done with the input work
        queue. It will also handle some interruptions.
        In case something goes wring this method will kill the running
        threads and the raise an exception
        """
        try:
            while self.areThreadsRuninng():
                if self.debug:
                    print "Results so far:", self.output_queue.qsize()
                pass
        except KeyboardInterrupt, ex:
            self.killThreads()
            msg = "Execution cancelled by user."
            raise KeyboardInterrupt, msg
        except Exception, ex:
            self.killThreads()
            msg = "Unexpected exception:"
            msg += ex
            raise Exception, msg

    def queryDBS(self):
        """queryDBS

        Queries local and global DBS looking for dataset information. It 
        returns the number of events and PhEDEx availability. This method shows
        two ways of excuting. One using threads and a sequencial one. I am
        keeping this last one due to its reliability.
        """
        if self.debug:
            print "Querying info from Local and Global DBS"
            print "---------------------------------------"
        if self.debug:
            print "Local DBS: %s" % self.dbs_url
        reader = DBSReader(self.dbs_url)
        if self.debug:
            print "Global DBS: %s " % self.global_dbs_url
            print "---------------------------------------"
        reader_global = DBSReader(self.global_dbs_url)
        datasets_number = len(self.datasets)
        #  //
        # // Using threads
        #//
        if self.with_threads:
            if self.debug:
                print "#### Using Threads for querying DBS! ####"
            self.startQueryingService(threads=5)
            #  //
            # // Unmerged datasets
            #//
            for i in range(datasets_number):
                self.input_queue.put({'id': i, 'url': self.dbs_url,
                    'dataset': self.datasets[i]['unmergedName']})
            if self.debug:
                print "Unmerged datasets: Querying Local DBS."
                print "Expecting %s results..." % datasets_number
            # Waiting for all the threads to be idle.
            self.waitForDrainQueue()
            if self.debug:
                print "Obtained %s results." % self.output_queue.qsize()
            # Parsing results
            while not self.output_queue.empty():
                result = self.output_queue.get()
                self.datasets[result[0]]['unmergedEvents'] = result[1][0]
            if self.debug:
                print "Unmerged datasets: Done querying Local DBS."
            #  //
            # // Merged datasets in local
            #//
            for i in range(datasets_number):
                self.input_queue.put({'id': i, 'url': self.dbs_url,
                    'dataset': self.datasets[i]['name']})
            if self.debug:
                print "Merged datasets: Querying Local DBS."
                print "Expecting %s results..." % datasets_number
            # Waiting for all the threads to idle.
            self.waitForDrainQueue()
            if self.debug:
                print "Obtained %s results." % self.output_queue.qsize()
            # Parsing results
            while not self.output_queue.empty():
                result = self.output_queue.get()
                self.datasets[result[0]]['mergedEvents'] = result[1][0]
            if self.debug:
                print "Merged datasets: Done querying Local DBS."
            #  //
            # // Merged datasets in global
            #//
            for i in range(datasets_number):
                self.input_queue.put({'id': i, 'url': self.global_dbs_url,
                    'dataset': self.datasets[i]['name']})
            if self.debug:
                print "Merged datasets: Querying Global DBS."
                print "Expecting %s results..." % datasets_number
            # Waiting for all the threads to be idle.
            self.waitForDrainQueue()
            if self.debug:
                print "Obtained %s results." % self.output_queue.qsize()
            # Parsing results
            while not self.output_queue.empty():
                result = self.output_queue.get()
                [self.datasets[result[0]]['globalEvents'],
                    self.datasets[result[0]]['inPhEDEx']] = result[1]
            if self.debug:
                print "Merged datasets: Done querying Global DBS."
            self.killThreads()
            #  //
            # // Calculating some values. I could include this in the code
            #// above, it's easier to understand this way though.
            #\\
            for i in range(datasets_number):
                if self.datasets[i]['mergedEvents'] < 0:
                    self.datasets[i]['deltaMerge'] = \
                        self.datasets[i]['unmergedEvents']
                else:
                    self.datasets[i]['deltaMerge'] = \
                        self.datasets[i]['unmergedEvents'] \
                        - self.datasets[i]['mergedEvents']
        #  //
        # // Not using threads
        #//
        else:
            for i in range(datasets_number):
                if self.debug:
                    print self.datasets[i]['name']
                self.datasets[i]['mergedEvents'] = getEvents(reader,
                    self.datasets[i]['name'])[0]
                self.datasets[i]['unmergedEvents'] = getEvents(reader,
                    self.datasets[i]['unmergedName'])[0]
                if self.datasets[i]['mergedEvents'] < 0:
                    self.datasets[i]['deltaMerge'] = \
                        self.datasets[i]['unmergedEvents']
                else:
                    self.datasets[i]['deltaMerge'] = \
                        self.datasets[i]['unmergedEvents'] \
                        - self.datasets[i]['mergedEvents']
                [self.datasets[i]['globalEvents'],
                    self.datasets[i]['inPhEDEx']] = getEvents(reader_global,
                    self.datasets[i]['name'])

#    def getEvents(self, reader, dataset):
#        """getEvents
#
#        Returns number of events and if all the datasets blocks were injected
#        into PhEDEx. It relies on the fact that blocks known by PhEDEx have a
#        storage element associated. If the dataset is not at the DBS instance
#        -1 is returned. 
#        """
#        primds = dataset.split('/')[1]
#        procds = dataset.split('/')[2]
#        tier = dataset.split('/')[3]
#        if not reader.matchProcessedDatasets(primds, tier, procds):
#            if self.debug:
#                print "MMMMM:%s" % (dataset)
#            # Dataset is not in DBS instance.
#            return [-1, False]
#        blocks = reader.getFileBlocksInfo(dataset)
#        in_PhEDEx = True
#        total_events = 0
#        for block in blocks:
#            total_events += block['NumberOfEvents']
#            try:
#                if not block['StorageElementList'][0]['Name']:
#                    in_PhEDEx = False
#            except:
#                in_PhEDEx = False
#        return [total_events, in_PhEDEx]

    def getLocalDBS(self):
        """getLocalDBS

        Get Local DBS url from the ProdAgent Configuration.
        """
        try:
            config = loadProdAgentConfiguration()
        except StandardError, ex:
            msg = "Error reading configuration:\n"
            msg += str(ex)
            raise RuntimeError, msg
        try:
            dbsConfig = config.getConfig("LocalDBS")
        except StandardError, ex:
            msg = "Error reading configuration for LocalDBS:\n"
            msg += str(ex)
            raise RuntimeError, msg
        self.dbs_url = dbsConfig.get("ReadDBSURL", None)

    def getGlobalDBS(self):
        """getGlobalDBS

        Gets Global DBS url form the ProdAgent Configuration
        """
        try:
            config = loadProdAgentConfiguration()
        except StandardError, ex:
            msg = "Error reading configuration:\n"
            msg += str(ex)
            raise RuntimeError, msg
        try:
            dbsConfig = config.getConfig("GlobalDBSDLS")
        except StandardError, ex:
            msg = "Error reading configuration for LocalDBS:\n"
            msg += str(ex)
            raise RuntimeError, msg
        self.global_dbs_url = dbsConfig.get("ReadDBSURL", None)

    def printReport(self):
        """printReport
        
        This method prints all datasets produced with the appropiate format.
        It will make sure the first column has the right width.
        """
        col_a = 10
        col_b = 10
        col_c = 10
        col_d = 10
        col_e = 10
        col_f = 9
        set_format = "#-%ss|"*7 % (self.max_length, col_a, col_b, col_c,
            col_d, col_e, col_f)
        set_format = "|" + set_format.replace("#","%")
        first_row =  set_format % ('Dataset', 'Expected'.center(col_a),
            'Unmerged'.center(col_b), 'Merged'.center(col_c), 
            'DeltaMerge'.center(col_d), 'Global'.center(col_e),
            'InPhEDEx?'.center(col_f))
        print '-'*len(first_row)
        print first_row
        print '-'*len(first_row)
        for dataset in self.datasets:
            print set_format % (dataset['name'],
                str(dataset['expectedEvents']).center(col_a),
                str(dataset['unmergedEvents']).center(col_b),
                str(dataset['mergedEvents']).center(col_c),
                str(dataset['deltaMerge']).center(col_d),
                str(dataset['globalEvents']).center(col_e),
                str(dataset['inPhEDEx']).center(col_f))
        print '-'*len(first_row)

    def forceMerge(self, trigger=None):
        """forceMerge

        Issue ForceMerge message for those datasets that have more unmerged
        events than merge. If trigger (%) is provided then it will force merge
        on those datasets whose Merged events are more than trigger (%) of the
        expected events.
        """
        if trigger is not None:
            if trigger > 100:
                trigger = 100
        if self.debug:
            print "Force Merge on the following datasets:"
        for dataset in self.datasets:
            force_merge = False
            if dataset['unmergedEvents'] > -1:
                if trigger is None:
                    if int(dataset['deltaMerge']) > 0:
                        force_merge = True
                else:
                    if (dataset['expectedEvents'] - dataset['mergedEvents'] >\
                        0) and int(dataset['deltaMerge']) > 0 and (
                        (100 - trigger) * dataset['expectedEvents'] / 100. >\
                        dataset['deltaMerge']):
                        force_merge = True
                if force_merge:
                    event = "ForceMerge"
                    payload = dataset['unmergedName']
                    if self.debug:
                        print event, payload
                    self.sendMessage(event, payload)
                    #self.ms.publish(event, payload)
                    #self.ms.commit()

    def migrateToGlobal(self):
        """migrateToGlobal

        Migrates a dataset if the number of merged events in local are more the 
        number of events in global dbs.
        """
        if self.debug:
            print "Migrate to Global the following datasets:"
        for dataset in self.datasets:
            migrate = False
            if dataset['mergedEvents'] > 0 and (dataset['mergedEvents'] > \
                dataset['globalEvents']):
                migrate = True
            if migrate:
                event = "DBSInterface:MigrateDatasetToGlobal"
                payload = dataset['name']
                if payload.find('unmerged') > -1:
                    print "The input txt file must have merged datasets only."
                else:
                    if self.debug:
                        print event, payload
                    self.sendMessage(event, payload)
                    #self.ms.publish(event, payload)
                    #self.ms.commit()

    def injectIntoPhEDEx(self):
        """injectIntoPhEDEx

        Injects a dataset to PhEDEx if there are blocks in global which are
        not yet in PhEDEx
        """
        if self.debug:
            print "Inject the following datasets into PhEDEx:"
        for dataset in self.datasets:
            inject = False
            if not dataset['inPhEDEx'] and dataset['globalEvents'] > -1:
                inject = True
            if inject:
                event = "PhEDExInjectDataset"
                payload = dataset['name']
                if payload.find('unmerged') > -1:
                    print "The input txt file must have merged datasets only."
                else:
                    if self.debug:
                        print event, payload
                    self.sendMessage(event, payload)
                    #self.ms.publish(event, payload)
                    #self.ms.commit()

    def sendMessage(self, event, payload):
        """sendMessage

        Publishes a ProdAgent message in the ProdAgent DB.
        """
        ms = MessageService()
        ms.registerAs("Test")
        if payload != None:
            ms.publish(event, payload)
        else:
            ms.publish(event, "")
        ms.commit()


if __name__ == '__main__' :
    main(sys.argv[1:])
