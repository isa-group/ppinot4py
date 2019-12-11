class CountMeasure():

    def __init__(self, when):
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
                    timeMeasureType = 'LINEAR', singleInstanceAggFunction = 'SUM', 
                         firstTo = 'False', precondition = ''):
  
        self.fromCondition = fromCondition
        self.toCondition = toCondition
        self.timeMeasureType = timeMeasureType
        self.singleInstanceAggFunction = singleInstanceAggFunction
        self.precondition = precondition
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
        

    def getPrecondition(self):
        return self.precondition

    def setPrecondition(self, newPrecondition):
        self.precondition = newConsider
           

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

 
class derivedMeasure():
    
    def __init__(self, functionExpression, measureMap):
      
        self.functionExpression = functionExpression
        self.measureMap = measureMap
