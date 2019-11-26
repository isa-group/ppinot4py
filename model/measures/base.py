from state import RunTimeState 
from condition.Condition import TimeInstantCondition

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

class DataMeasure():
    
    def __init__(self, dataContentSelection, precondition, first):
       
        self.dataContentSelection = dataContentSelection
        self.precondition = precondition
        self.first = first

    def isFirst(self):
        return self.isFirst

    def setFirst(self, newFirst):
        self.isFirst = newFirst

    def getDataContentSelection(self):
        return self.dataContentSelection

    def setDataContentSelection(self, newData):
        self.dataContentSelection = newData

    def getPrecondition(self):
        return self.precondition

    def setPrecondition(self, newPrecondition):
        self.precondition = newPrecondition


class TimeMeasure():

    def __init__(self, fromCondition, toCondition, 
                    timeMeasureType, singleInstanceAggFunction, 
                        considerOnly, precondition, computeUnfinished, firstTo):
  
        self.fromCondition = fromCondition
        self.toCondition = toCondition
        self.timeMeasureType = timeMeasureType
        self.singleInstanceAggFunction = singleInstanceAggFunction
        self.considerOnly = considerOnly
        self.precondition = precondition
        self.computeUnfinished = computeUnfinished
        self.firstTo = firstTo


    def getFromCondition(self):
        return self.fromCondition

    def setFromCondition(self, newFrom):
        self.fromCondition = newFrom


    def getToCondition(self):
        return self.toCondition

    def setToCondition(self, newTo):
        self.toCondition = newTo


    def getTimeMeasureType(self):
        return self.isFirst

    def setTimeMeasureType(self, newTimeMeasure):
        self.timeMeasureType = newTimeMeasure


    def getSingleInstanceAggFunction(self):
        return self.singleInstanceAggFunction

    def setSingleInstanceAggFunction(self, newAggFunction):
        self.singleInstanceAggFunction = newAggFunction


    def getConsiderOnly(self):
        return self.considerOnly

    def setConsiderOnly(self, newConsider):
        self.considerOnly = newConsider


    def getPrecondition(self):
        return self.precondition

    def setPrecondition(self, newPrecondition):
        self.precondition = newConsider


    def getcomputeUnfinished(self):
        return self.computeUnfinished

    def setcomputeUnfinished(self, newComputeUnfinish):
        self.computeUnfinished = newComputeUnfinish
    

    def isFirst(self):
        return self.firstTo

    def setFirst(self, newFirst):
        self.firstTo = newFirst


class aggregatedMeasure():

    def __init__(self, baseMeasure, filterToApply,  singleInstanceAggFunction, grouper):
  
        self.baseMeasure = baseMeasure
        self.filterToApply = filterToApply
        self.singleInstanceAggFunction = singleInstanceAggFunction
        self.grouper = grouper

        # relative = true -> desde la fecha
        # relative = false -> mes completo

 