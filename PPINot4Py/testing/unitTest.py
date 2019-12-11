import json
import os
import sys
import time
import unittest
from PPINot4Py import timeGrouper, dataframeImporter
from PPINot4Py.computer import measureComputer
from PPINot4Py import computer
from PPINot4Py.state import DataObjectState
from PPINot4Py.condition import Condition
from PPINot4Py.measures import base


dataframe = dataframeImporter.importer('bpi_challenge_2013_incidents.xes')

# Need to change ":" for "_" because ".query" put nervous with ":"
dataframe.columns = [column.replace(":", "_") for column in dataframe.columns]

# Data measure used
countState = DataObjectState.DataObjectState("concept_name == 'Queued'")
countCondition = Condition.TimeInstantCondition(countState)
countMeasure = base.CountMeasure(countCondition)
dataMeasure = base.DataMeasure("lifecycle_transition", countMeasure, False)

# Time measure used 
#(fromCondition, toCondition,  timeMeasureType = 'Linear', singleInstanceAggFunction = 'SUM', firstTo = 'False', precondition = '')

countStateTimeA = DataObjectState.DataObjectState('lifecycle_transition == "In Progress"')
countConditionTimeA = Condition.TimeInstantCondition(countStateTimeA)
countMeasureTimeA = base.CountMeasure(countConditionTimeA)

countStateTimeB = DataObjectState.DataObjectState('lifecycle_transition == "Closed"')
countConditionTimeB = Condition.TimeInstantCondition(countStateTimeB)
countMeasureTimeB = base.CountMeasure(countConditionTimeB)

countStateTimeC = DataObjectState.DataObjectState('lifecycle_transition == "Awaiting Assignment"')
countConditionTimeC = Condition.TimeInstantCondition(countStateTimeC)
countMeasureTimeC = base.CountMeasure(countConditionTimeC)


timeMeasureLinearA = base.TimeMeasure(countMeasureTimeA, countMeasureTimeB)
timeMeasureLinearB = base.TimeMeasure(countMeasureTimeB, countMeasureTimeA)
timeMeasureLinearC = base.TimeMeasure(countMeasureTimeA, countMeasureTimeC)

timeMeasureCyclic = base.TimeMeasure(countMeasureTimeA, countMeasureTimeB, 'CYCLIC', 'SUM')
timeMeasureCyclicMax = base.TimeMeasure(countMeasureTimeA, countMeasureTimeB, 'CYCLIC', 'MAX')
timeMeasureCyclicMin = base.TimeMeasure(countMeasureTimeA, countMeasureTimeB, 'CYCLIC', 'MIN')
timeMeasureCyclicAvg = base.TimeMeasure(countMeasureTimeA, countMeasureTimeB, 'CYCLIC', 'AVG')

#baseMeasure = measureComputer(timeMeasureLinear, dataframe)
timeGrouper60s = timeGrouper.grouper('60s')
aggregatedMeasure60s = base.aggregatedMeasure(timeMeasureLinearA, '', 'SUM', timeGrouper60s)

timeGrouper2W = timeGrouper.grouper('2W')
aggregatedMeasure2W = base.aggregatedMeasure(timeMeasureLinearA, '', 'SUM', timeGrouper2W)

measure_dictionary = {'ProgressCount': timeMeasureLinearA, 'ClosedCount': timeMeasureLinearB, 'AwwaitingCount': timeMeasureLinearC}

derivedMeasure = base.derivedMeasure('(ProgressCount + ClosedCount) / sqrt(AwwaitingCount)', measure_dictionary)


class MyTest(unittest.TestCase):
    def testTimeMeasureCyclicSum(self):
        self.assertEqual(measureComputer(timeMeasureCyclic, dataframe).size, 4904)
    def testTimeMeasureCyclicMax(self):
        self.assertEqual(measureComputer(timeMeasureCyclicMax, dataframe).size, 4904)
    def testTimeMeasureCyclicMin(self):
        self.assertEqual(measureComputer(timeMeasureCyclicMin, dataframe).size, 4904)
    def testTimeMeasureCyclicAvg(self):
        self.assertEqual(measureComputer(timeMeasureCyclicAvg, dataframe).size, 4904)
        
    def testTimeMeasureLinearA_B(self):
        self.assertEqual(measureComputer(timeMeasureLinearA, dataframe).size, 4904)
    def testTimeMeasureLinearB_A(self):
        self.assertEqual(measureComputer(timeMeasureLinearB, dataframe).size, 6)
    def testTimeMeasureLinearA_C(self):
        self.assertEqual(measureComputer(timeMeasureLinearC, dataframe).size, 3619)
        
    def testDataMeasureFirstFalse(self):
        self.assertEqual(measureComputer(dataMeasure, dataframe).size, 4511)  
        
    def testCountMeasure(self):
        self.assertEqual(measureComputer(countMeasureTimeA, dataframe).size, 7554)
        
    def testAggregatedMesure2W(self):
        self.assertEqual(measureComputer(aggregatedMeasure2W, dataframe).size, 3)
    def testAggregatedMesure60s(self):
        self.assertEqual(measureComputer(aggregatedMeasure60s, dataframe).size, 31285)

    def testDerivatedMeasure(self):
        self.assertEqual(measureComputer(derivedMeasure, dataframe).size, 4904)

if __name__ == "__main__":
    unittest.main()

