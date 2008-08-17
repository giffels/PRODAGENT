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

from ProdCommon.FwkJobRep.TaskState import TaskState
from ProdCommon.MCPayloads.UUID import makeUUID

from StageOut.StageOutMgr import StageOutMgr
from StageOut.StageOutError import StageOutInitError
import StageOut.Impl

_DoHTTPPost = True

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
            print "Found Analysis File: %s" % aFile
            self.stageOut(aFile['FileName'])
            if _DoHTTPPost:
                self.httpPost(aFile['FileName'])

        return 0


    def stageOut(self, filename):
        """
        _stageOut_

        stage out the DQM Histogram to local storage

        """
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
                                                      filename),
            'PFN' : os.path.join(os.getcwd(), filename),
            'SEName' : None,
            'GUID' : filebasename,
            }
        try:
            stager(**fileInfo)
        except Exception, ex:
            msg = "Unable to stage out DQM File:\n"
            msg += str(ex)
            print msg
            return




    def httpPost(self, filename):
        """
        _httpPost_

        perform an HTTP POST operation to a webserver

        """
        args = {}
        args['step'] = 'Pass-1'
        args['producer'] = 'automatic'
        #args['url'] = 'https://cmsweb.cern.ch/dqm/dev' #test instance
        args['url'] = 'https://cmsweb.cern.ch/dqm/tier-0' 

        try:
            self.upload(args, file)
        except HTTPError, e:
            print 'Automated upload of %s failed' % filename
            print "ERROR", e
            print 'Status code: ', e.hdrs.get("Dqm-Status-Code", "None")
            print 'Message:     ', e.hdrs.get("Dqm-Status-Message", "None")
            print 'Detail:      ', e.hdrs.get("Dqm-Status-Detail", "None")
        except:
            print 'Automated upload of %s failed' % filename
            print 'problem unknown'


                        
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

        #make or check workflow description
        (fdir, fname) = (os.path.dirname(file), os.path.basename(file))
        fnameSplit = fname.rstrip('.root').split('__')
        if not args.has_key('workflow'):
            args['workflow'] = '/' + '/'.join(fnameSplit[1:])

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
