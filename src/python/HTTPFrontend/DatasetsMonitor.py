#!/usr/bin/env python
"""
_DatasetsMonitor_

Local DBS Interface for PA Monitoring

"""
from ProdCommon.DataMgmt.DBS.DBSReader import DBSReader

class DatasetMonitor:
    """
    _DatasetMonitor_

    Generate a list of files for a dataset from the
    local DBS instance

    """
    def __init__(self, localDBS):
        self.localDBS = localDBS


    def index(self, dataset):
        html = """<html><body><h2>Local DBS Dataset Listing</h2>\n """
        html += "<h4>Dataset: %s<h4>\n" % dataset

        reader = DBSReader(self.localDBS)

        html += "<h4>Block Details</h4>\n"
        html += "<table>\n"
        html += "<tr><th>Block</th><th>SEName</th><th>Files</th>"
        html += "<th>Events</th></tr>\n"
        try:
            blocks = reader.getFileBlocksInfo(dataset)
        except Exception, ex:
            html += "</table>\n"
            html += "<p> Error accessing dataset information: %s</p>" % str(ex)
            html += """</body></html>"""
            return html
        
        for block in blocks:
            blockName = block['Name']
            seList = reader.listFileBlockLocation(blockName)
            seText = "<p>"
            for seName in seList:
                seText += "%s</br>" % seName
            seText += "</p>"
            html += "<tr><td>%s</td><td>%s</td>" % ( blockName, seText)
            html += "<td>%s</td><td>%s</td></tr>\n" % (
                block['NumberOfFiles'],
                block['NumberOfEvents'])
        
        html += "</table>\n"
        html += """</body></html>"""
        return html
    index.exposed = True
