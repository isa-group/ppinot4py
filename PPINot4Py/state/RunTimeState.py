from PPINot4Py.measures import base
#class RunTimeState:
       
#    def __init__(self):
#        super().__init__()

class DataObjectState():
    
    def __init__(self, name):

        name = str(name)
        self.name = name

    def getName(self):
        return self.name

    def __repr__(self):
        return "%s" % (self.name)   

class ComplexState():
    
    def __init__(self, first, last, type):

        first = base.TimeInstantCondition(first)
        last = base.TimeInstantCondition(last)
        
        self.first = first
        self.last = last
        self.type = type
        

    def getFirst(self):
        return self.first


    def getLast(self):
        return self.last
        

    def getType(self):
        return self.type    

    def __repr__(self):
        return "%s, %s, %s" % (self.first, self.last, self.type)   