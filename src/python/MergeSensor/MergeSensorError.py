
class MergeSensorError(Exception):
    """ Exception object for general errors in MergeSensor component"""
    
    def __init__(self, value):
        self.value = value
        
    def __str__(self):
        return repr(self.value)

class InvalidDataTier(Exception):
    """ Exception object for invalid data tier"""
    
    def __init__(self, value):
        self.value = value
        
    def __str__(self):
        return repr(self.value)
        
