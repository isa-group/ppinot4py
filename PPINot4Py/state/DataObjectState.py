class DataObjectState():
    
    def __init__(self, name):
        #super(DataObjectState, self).__init__()
        self.name = name
        
    def getName(self):
        return self.__name
        
    def setName(self):
        self.name = name

    def __repr__(self):
        return "%s" % (self.name)