#Possibly not needed
class DataCondition():

    def __init__(self, appliesTo):
        self.appliesTo = appliesTo

    def getAppliesTo(self):
        return self.appliesTo

    def __repr__(self):
        return "%s" % (self.appliesTo)

class TimeInstantCondition(object):
    
    def __init__(self, changesToState):
        self.changesToState = changesToState
    
    def __repr__(self):
        return "%s" % (self.changesToState)

    def getChangesToState(self):
        return self.changesToState

class SeriesCondition():

    def __init__(self, series):
        self.series = series
    
    def __repr__(self):
        return "%s" % (self.series)