#!/usr/bin/python3
from os import path
from shutil import rmtree

class utils(object):
    def __init__(self):
        self.constants_d = {}
        self.offset = 0
        self.homedir = '/home/ubuntu'

    def setConstants(self):
        with open(self.homedir + 'constants.cfg', 'r') as f:
            self.d_constants = dict(x.rstrip().split(":", 1) for x in f)

        try:
            with open(self.homedir + 'offset.cfg', 'r') as f:
                self.offset = int(f.readline(1))
        except:
            self.offset = 0

    def updateOffset(self):
        try:
            with open(self.homedir + 'offset.cfg', 'w+') as f:
                f.write(str(self.offset+1))
        except:
            f.write(str(self.offset))

    def writeSearchParams(self, filename, SearchParams):
        try:
            with open(self.homedir+"tmp/"+filename, 'a+') as f:
                f.write(","+",".join(SearchParams ))
        except:
            pass

    def readSearchParams(self, filename):
        try:
            with open(self.homedir+"tmp/"+filename, 'r') as f:
                params = [y for x in f for y in x.split(",") if y != '' and y != None]
                params = [v for v in set(params)]
            if filename == 'patt':
                return [v.split("~") for v in params]
            else:
                return params
        except:
            return []

    def delSearchParams(self):
        try:
            rmtree(self.homedir+"tmp/")
        except:
            pass

    def logFailedSearch(self, filename, log_text):
        try:
            with open(self.homedir+"log/"+filename, 'a+') as f:
                f.write(log_text)
        except:
            pass
