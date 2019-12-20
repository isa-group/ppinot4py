class DataObjectState():
    """Basic state of a condition.
    
    Args:
    
            - name: String representation of a condition
    """
    
    def __init__(self, name):
        self.name = name
        
    def getName(self):
        return self.__name
        
    def setName(self):
        self.name = name

    def __repr__(self):
        return "%s" % (self.name)