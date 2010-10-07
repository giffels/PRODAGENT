#!/usr/bin/env python
"""
_CollectPayload_

Payload for publishing a Collect event that encodes dataset and run
information


"""


class CollectPayload(dict):
    """
    _CollectPayload_


    """
    def __init__(self):
        dict.__init__(self)
        self.setdefault("PrimaryDataset", None)
        self.setdefault("ProcessedDataset", None)
        self.setdefault("DataTier", None)
        self.setdefault("RunNumber", None)
        self.setdefault("Scenario", None)
        self.setdefault("GlobalTag", None)
        self.setdefault("CMSSWVersion", None)
        self.setdefault("RefHistKey", None)


    def datasetPath(self):
        """
        _datasetPath_

        get the dataset path

        """
        result = "/%s/%s/%s" % (self['PrimaryDataset'],
                                self['ProcessedDataset'],
                                self['DataTier'])


        return result


    def parse(self, payload):
        """
        _parse_

        unpack data from the string format

        """
        tokens = payload.split(";")
        for token in tokens:
            if token.startswith("tier="):
                self['DataTier'] = token.replace("tier=", "")
            if token.startswith("prim="):
                self['PrimaryDataset'] = token.replace("prim=", "")
            if token.startswith("proc="):
                self['ProcessedDataset'] = token.replace("proc=", "")
            if token.startswith("run="):
                self['RunNumber'] = token.replace("run=", "")
            if token.startswith("scenario="):
                self['Scenario'] = token.replace("scenario=", "")
            if token.startswith("tag="):
                self['GlobalTag'] = token.replace("tag=", "")
            if token.startswith("cmssw="):
                self['CMSSWVersion'] = token.replace("cmssw=", "")
            if token.startswith("refhist="):
                self['RefHistKey'] = token.replace("refhist=", "")

        return


    def __str__(self):
        """
        encode as payload string

        """
        result = ""
        if self['RunNumber'] != None:
            result += "run=%s;" % self['RunNumber']
        if self['PrimaryDataset'] != None:
            result += "prim=%s;" % self['PrimaryDataset']
        if self['ProcessedDataset'] != None:
            result += "proc=%s;" % self['ProcessedDataset']
        if self['DataTier'] != None:
            result += "tier=%s;" % self['DataTier']
        if self['Scenario'] != None:
            result += "scenario=%s;" % self['Scenario']
        if self['GlobalTag'] != None:
            result += "tag=%s;" % self['GlobalTag']
        if self['CMSSWVersion'] != None:
            result += "cmssw=%s;" % self['CMSSWVersion']
        if self['RefHistKey'] != None:
            result += "refhist=%s;" % self['RefHistKey']

        return result


if __name__ == '__main__':
    collect1 = CollectPayload()
    collect1['RunNumber'] = 11223344

    collect2 = CollectPayload()
    collect2['RunNumber'] = 11223344
    collect2['PrimaryDataset'] = "Primary"

    collect3 = CollectPayload()
    collect3['RunNumber'] = 11223344
    collect3['PrimaryDataset'] = "Primary"
    collect3['ProcessedDataset'] = "Processed"
    collect3['DataTier'] = "TIER"




    print str(collect1)

    print str(collect2)

    print str(collect3)

    str1 = str(collect1)
    str2 = str(collect2)
    str3 = str(collect3)

    collect4 = CollectPayload()
    collect4.parse(str1)
    collect5 = CollectPayload()
    collect5.parse(str2)
    collect6 = CollectPayload()
    collect6.parse(str3)

    print str(collect4)
    print str(collect5)
    print str(collect6)
