#!/usr/bin/env python

class RetryException(Exception):

   def __init__(self,errorStr,errorMsg):
       self.args=(errorStr,errorMsg)

