#!/usr/bin/env python


from ProdCommon.MCPayloads.WorkflowSpec import WorkflowSpec
import os, sys, getopt


def setPrestage():
    """
    _setPrestage_
    Takes workflow file path as parameter and enable prestaging on it.
    """ 
    valid = ['workflow=']
    workflow = None
    try:
       opts, args = getopt.getopt(sys.argv[1:], "", valid)
    except getopt.GetoptError, ex:
       print usage
       print str(ex)
       sys.exit(1)

    for opt,arg in opts:
          
        if opt == '--workflow':
              
           workflow = arg     

    if workflow is None:
       msg = 'Please provide workflow file path parameter i.e --workflow=file_path'
       raise RuntimeError, msg

    else: 
        try:
           spec = WorkflowSpec()
           spec.load(workflow)
           spec.parameters['PreStage'] = 'True'           
           spec.save(workflow)
        except Exception, ex:
           msg = 'Exception caught while enabling prestage\n'
           msg += str(ex)
           raise RuntimeError, msg  

    return


if __name__ == "__main__":

   setPrestage()








