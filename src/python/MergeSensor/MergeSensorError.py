
class MergeSensorError(Exception):
    """ Exception object for errors in MergeSensor component"""
    
    def __init__(self, value):
        self.value = value
        
    def __str__(self):
        return repr(self.value)
    
    