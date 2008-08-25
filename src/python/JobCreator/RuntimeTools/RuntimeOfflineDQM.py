#!/usr/bin/env python
"""
_RuntimeOfflineDQM_

Harvester script for DQM Histograms.

Will do one/both of the following:
1. Copy the DQM Histogram file to the local SE, generating an LFN name for it
2. Post the DQM Histogram file to a Siteconf discovered DQM Server URL

"""
import os
import sys
from urllib2   import Request, build_opener, HTTPError, URLError
from mimetypes import guess_type
from gzip      import GzipFile
from cStringIO import StringIO
from md5       import md5
import traceback

from ProdCommon.FwkJobRep.TaskState import TaskState
from ProdCommon.MCPayloads.UUID import makeUUID

from StageOut.StageOutMgr import StageOutMgr
from StageOut.StageOutError import StageOutInitError
import StageOut.Impl

_DoHTTPPost = True
_DoStageOut = True
_HTTPPostURL = 'https://cmsweb.cern.ch/dqm/dev/data/put' #test instance
#_HTTPPostURL = 'https://cmsweb.cern.ch/dqm/tier-0/data/put' # prod instance





class OfflineDQMHarvester:
    """
    _OfflineDQMHarvester_

    Util to trawl through a Framework Job Report to find analysis
    files and copy them to some DQM server

    """
    def __init__(self):
        self.state = TaskState(os.getcwd())
        self.state.loadRunResDB()
        self.config = self.state.configurationDict()
        self.workflowSpecId = self.config['WorkflowSpecID'][0]
        self.jobSpecId = self.config['JobSpecID'][0]

        try:
            self.state.loadJobReport()
        except Exception, ex:
            print "Error Reading JobReport:"
            print str(ex)
            self.state._JobReport = None

        self.state.loadJobSpecNode()

        # map local to storage PFN
        self.mssNames = {}



    def __call__(self):
        """
        _operator()_

        Invoke this object to find files and do stage out

        """
        if self.state._JobReport == None:
            msg = "No Job Report available\n"
            msg += "Unable to process analysis files for offline DQM\n"
            print msg
            return 1

        jobRep = self.state._JobReport

        for aFile in jobRep.analysisFiles:
            if aFile['FileName'].startswith("./"):
                aFile['FileName'] = aFile['FileName'].replace("./", "")

            msg = "==========Handling Analysis File=========\n"
            for key, value in aFile.items():
                msg += " => %s: %s\n" % (key, value)
            print msg
            self.stageOut(aFile)
            if _DoHTTPPost:
                self.httpPost(aFile)

        return 0


    def stageOut(self, analysisFile):
        """
        _stageOut_

        stage out the DQM Histogram to local storage

        """
        filename = analysisFile['FileName']
        try:
            stager = StageOutMgr()
        except Exception, ex:
            msg = "Unable to stage out log archive:\n"
            msg += str(ex)
            print msg
            return
            
	filebasename = os.path.basename(filename)
	filebasename = filebasename.replace(".root", "")
	
        fileInfo = {
            'LFN' : "/store/unmerged/dqm/%s/%s/%s" % (self.workflowSpecId,
                                                      self.jobSpecId,
                                                      filebasename),
            'PFN' : os.path.join(os.getcwd(), filename),
            'SEName' : None,
            'GUID' : filebasename,
            }
        if _DoStageOut:
            try:
                stager(**fileInfo)
            except Exception, ex:
                msg = "Unable to stage out DQM File:\n"
                msg += str(ex)
                print msg
                return
        else:
            msg = "Stage Out is disabled"
            print msg
            
        storagePFN = stager.searchTFC(fileInfo['LFN'])
        self.mssNames[filename] = storagePFN
        analysisFile['StoragePFN'] = storagePFN
        return



    def httpPost(self, analysisFile):
        """
        _httpPost_

        perform an HTTP POST operation to a webserver

        """
        filename = analysisFile['FileName']
        
        args = {}
        args['step'] = 'Pass-1'
        args['producer'] = 'ProdSys'
        args['url'] = _HTTPPostURL

        jobSpecNode = self.state.jobSpecNode
        inputDataset = jobSpecNode._InputDatasets[0]
        args['workflow'] = inputDataset.name()
        
        args['mssname'] = self.mssNames[filename]
        msg = "HTTP Upload of file commencing with args:\n"
        msg += " => Filename: %s\n" % filename
        for key, val in args.items():
            msg += " => %s: %s\n" % (key, val)
        print msg
        
        try:
            self.upload(args, filename)
        except HTTPError, e:
            print 'Automated upload of %s failed' % filename
            print "ERROR", e
            print 'Status code: ', e.hdrs.get("Dqm-Status-Code", "None")
            print 'Message:     ', e.hdrs.get("Dqm-Status-Message", "None")
            print 'Detail:      ', e.hdrs.get("Dqm-Status-Detail", "None")
        except Exception, ex:
            print 'Automated upload of %s failed' % filename
            print 'problem unknown'
            print ex

            

            


                        
    def encode(self, args, files):
        """
        Encode form (name, value) and (name, filename, type) elements into
        multi-part/form-data. We don't actually need to know what we are
        uploading here, so just claim it's all text/plain.
        """
        boundary = '----------=_DQM_FILE_BOUNDARY_=-----------'
        (body, crlf) = ('', '\r\n')
        for (key, value) in args.items():
            body += '--' + boundary + crlf
            body += ('Content-disposition: form-data; name="%s"' % key) + crlf
            body += crlf + str(value) + crlf
        for (key, filename) in files.items():
            filetype = guess_type(filename)[0] or 'application/octet-stream'
            body += '--' + boundary + crlf
            body += ('Content-Disposition: form-data; name="%s"; filename="%s"'
                     % (key, os.path.basename(filename))) + crlf
            body += ('Content-Type: %s' % filetype) + crlf
            body += crlf + open(filename, "r").read() + crlf
        body += '--' + boundary + '--' + crlf + crlf
        return ('multipart/form-data; boundary=' + boundary, body)



    def upload(self, args, file):

        
        #preparing a checksum
        blockSize = 0x10000
        def upd(m, data):
            m.update(data)
            return m
        fd = open(file, 'rb')
        try:
            contents = iter(lambda: fd.read(blockSize), '')
            m = reduce(upd, contents,md5())
        finally:
            fd.close()

        args['checksum'] = 'md5:' + m.hexdigest()
        args['size']     = str(os.stat(file)[6])


        # open a connection and upload the file
        url = args.pop('url')
        request = Request(url)   
        (type, body) = self.encode(args, {'file': file})
        request.add_header('Accept-encoding', 'gzip')
        request.add_header('Content-type',    type)
        request.add_header('Content-length',  str(len(body)))
        request.add_data(body)
        result = build_opener().open(request)
        data   = result.read()
        if result.headers.get('Content-encoding', '') == 'gzip':
            data = GzipFile(fileobj=StringIO(data)).read()
        return (result.headers, data)      


if __name__ == '__main__':


    harvester = OfflineDQMHarvester()
    status = harvester()

    sys.exit(status)
