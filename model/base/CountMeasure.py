from state import RunTimeState 

class CountMeasure(object):

    def __init__(self, when):
        when = TimeInstantCondition(when)
        self.when = when

    def getWhen(self):
        return self.when

    def setWhen(self):
        self.when = when

    def __repr__(self):
        return "%s" % (self.when)


class TimeInstantCondition(object):
    
    def __init__(self, changesToState):
        self.changesToState = changesToState
    
    def __repr__(self):
        return "%s" % (self.changesToState)

    def getChangesToState(self):
        return self.changesToState


