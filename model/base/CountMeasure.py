from state import RunTimeState 

class CountMeasure():

    #def __init__(self):
    #   self.when = None

    def __init__(self, when):
        when = TimeInstantCondition(when)
        self.when = when

    def getWhen(self):
        return self.when

    def setWhen(self):
        self.when = when

    def __repr__(self):
        return "%s" % (self.when)
    
    #def valid(self):
     #   return MeasureDefinition.valid() and self.__when != None


class TimeInstantCondition():
    
    def __init__(self, changesToState):
        self.changesToState = RunTimeState.DataObjectState(changesToState)
        #self.precondition = ProcessInstanceCondition()
    
    def __repr__(self):
        return "%s" % (self.changesToState)

    #def getPrecondition(self):
    #    return self.__precondition

    #def setPrecondition(self):
    #    self.__precondition = precondition
    #    return self


