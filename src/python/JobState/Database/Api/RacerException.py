#!/usr/bin/env python

class RacerException(Exception):

   def __init__(self,errorStr,errorMsg):
       self.args=(errorStr,errorMsg)

