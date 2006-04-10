#!/usr/bin/env python
"""
_ControlPoint_

ControlPoint container class that represents a
ControlPoint comprised of Conditionals and Actions
"""
__revision__ = "$Id: ControlPoint.py,v 1.1 2005/12/30 18:54:26 evansde Exp $"
__version__ = "$Revision: 1.1 $"
__author__ = "evansde@fnal.gov"

from ShREEK.ShREEKException import ShREEKException
from ShREEK.ControlPoints.Conditional import Conditional
from ShREEK.ControlPoints.Action import Action
from IMProv.IMProvNode import IMProvNode

class ControlPoint:
    """
    _ControlPoint_

    ControlPoint Object that is instantiated and populated
    with Conditionals and Actions. These Can then be validated
    and saved as an XML file. It can be reloaded from the xml
    and evaluate its contents.

    Utility Methods to interact with the ShREEK Execution components
    can be used to control the flow of the job.

    Instantiation can be done with or without a reference to the
    ShREEK Exe thread. This is so a ControlPoint can be built for insertion
    into a ScriptObject without the ShREEK Executor. At Runtime, construction
    with the ExeThread Ref is used to allow the Runtime Utility methods
    of this class.
    
    """
    def __init__(self, exeThreadRef = None):
        self.executionMgr = exeThreadRef
        self._Parameters = {}
        self._Conditionals = []
        self._Actions = []
        

    def __call__(self):
        """
        Operator ()

        Evaluate all of the Conditionals in the Control Point
        and allow them to trigger the Actions that they require
        
        """
        for conditional in self._Conditionals:
            conditional(self)
        return
            
    def addConditional(self, conditional):
        """
        _addConditional_

        Add An instance of a Conditional to this ControlPoint

        Args --

        - *conditional* : Instance of Conditional Object to be added.
        
        """
        
        if not issubclass(conditional.__class__, Conditional):
            msg = "Object Not a Conditional Instance\n"
            msg += "addConditional Argument must be a "
            msg += "Conditional Object Instance\n"
            raise ShREEKException(msg, ClassInstance = self)
            
        self._Conditionals.append(conditional)
        return

    def addAction(self, action):
        """
        _addAction_

        Add an Action Instance to this ControlPoint

        Args --

        - *action* : Instance of Action to be added

        """
        if not issubclass(action.__class__, Action):
            msg = "Object Not an Action Instance\n"
            msg += "addAction Argument must be an "
            msg += "Action Object Instance\n"
            raise ShREEKException(msg, ClassInstance = self)
            
        self._Actions.append(action)
        return

    def addParameter(self, paramName, paramValue):
        """
        _addParameter_

        Add a parameter name/value pair to this control Point
        Parameter values are stored as strings
        
        Args --

        - *paramName* : Parameter Name

        - *paramValue* : Paramter Value
        
        """
        self._Parameters[paramName] = str(paramValue)
        return
    

    def getParameters(self):
        """
        return the Parameters dictionary
        """
        return self._Parameters

    def triggerAction(self, actionName, conditionalRef):
        """
        _triggerAction_

        Execute the Action with the name actionName.

        Args --

        - *actionName* : Name Attribute of the Action to be Triggered

        - *conditionalRef* : Reference to the Conditional 
        that triggered the Action

        Returns --

        - *None*
        
        """
        for action in self._Actions:
            if action.name == actionName:
                try:
                    action(self)
                except ShREEKException, ex:
                    ex.message += "Error Triggering Action: %s\n" % actionName
                    ex.message += "Triggered from Conditional:\n"
                    ex.message += str(conditionalRef.name)
                    ex.message += '\n'
                    ex.addInfo(ActionInstance = action,
                               ConditionalInstance = conditionalRef,
                               ControlPoint = self)
                    raise ex
                return
        #  //
        # // Unmatched Action Name
        #//
        msg = "Warning: No Action was found with the name: %s\n" % actionName
        msg += "This Action was invoked from Conditional:\n"
        msg += str(conditionalRef.name)
        msg += '\n'
        raise ShREEKException(msg, ClassInstance = self,
                              ActionName = actionName,
                              ConditionalInstance = conditionalRef,
                              ControlPoint = self)
    
                
    

    def makeIMProv(self):
        """
        _makeIMProv_

        Produce and IMProv Node XML representation of this object

        Returns --

        - *IMProvNode* : Improv Node Tree containing ControlPoint
        
        """
        result = IMProvNode("ControlPoint")
        params = IMProvNode("Parameters")
        for param, value in self._Parameters.items():
            params.addNode(
                IMProvNode(param, None,  Value = value)
                )
        result.addNode(params)
        conds = IMProvNode("Conditionals")
        for cond in self._Conditionals:
            conds.addNode(cond.makeIMProv())
        actions = IMProvNode("Actions")
        for action in self._Actions:
            actions.addNode(action.makeIMProv())
        result.addNode(conds)
        result.addNode(actions)
        return result

    def save(self, filename):
        """
        _save_

        Save this ControlPoint as an XML ControlPoint File
        """
        improv = self.makeIMProv()
        handle = open(filename, 'w')
        handle.write( improv.makeDOMElement().toprettyxml() )
        handle.close()
        return


    #  //============================================
    # // Runtime Methods, only work if ExeThread Ref
    #//  is set
    def _CheckExeMgrRef(self, methodName):
        """
        _CheckExeMgrRef_

        Validates that the ExeThread Reference is set for methods that
        are runtime only
        """
        if self.executionMgr == None:
            msg = "Illegal use of Runtime Method: %s\n" % methodName
            msg += "This method can only be used after a\n"
            msg += "Reference to the ExecutionThread is provided\n"
            raise ShREEKException(msg, MethodCalled = methodName,
                                  ClassInstance = self)
        return


    def setNextTask(self, nextTask):
        """
        _setNextTask_

        Set the next task for the ExecutionManager instance
        """
        self.executionMgr.setNextTask(nextTask)
        
 
    def killjob(self):
        """
        _killjob_

        Kill the entire Job
        """
        self._CheckExeMgrRef("killjob")
        self.executionMgr.killjob()
        return
        
    def killtask(self):
        """
        _killtask_

        Kill the current task and proceed to the next task
        """
        self._CheckExeMgrRef("killtask")
        self.executionMgr.killtask()
        return

    
