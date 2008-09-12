#!/usr/bin/env python

class TransitionException(Exception):

   def __init__(self,errorStr,errorMsg):
       self.args=(errorStr,errorMsg)

