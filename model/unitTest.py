import json
import os
import sys
import time
import unittest
from pm4py.objects.log.adapters.pandas import csv_import_adapter
from state import RunTimeState
from measures import base
from measureComputer import measureComputer

# Creating the .csv in case is the first time we run the program
if(os.path.exists("log_in_csv.csv") == False):
    log = xes_import_factory.apply('bpi_challenge_2013_incidents.xes')
    csv_exporter.export(log, "log_in_csv.csv")

# Loading .csv in dataframe
dataframe = csv_import_adapter.import_dataframe_from_path(os.path.join("log_in_csv.csv"))

# Need to change ":" for "_" because ".query" put nervous with ":"
dataframe.columns = [column.replace(":", "_") for column in dataframe.columns]

# Definition of case id in our table
id_case = 'case_concept_name'   
time_column = 'time_timestamp'
# Count measure used 
countState = RunTimeState.DataObjectState("lifecycle_transition == 'In Progress'")
countCondition = base.TimeInstantCondition(countState)
countMeasure = base.CountMeasure(countCondition)

# Data measure used
dataMeasure = base.DataMeasure("lifecycle_transition", "impact == 'Medium'", False)

# Time measure used
timeMeasureCyclicSum = base.TimeMeasure('lifecycle_transition', 'lifecycle_transition == "In Progress"',
                    'lifecycle_transition == "Awaiting Assignment"', 'CYCLIC', 'SUM', '', '', '', False)
timeMeasureCyclicMax = base.TimeMeasure('lifecycle_transition', 'lifecycle_transition == "In Progress"',
                    'lifecycle_transition == "Awaiting Assignment"', 'CYCLIC', 'MAX', '', '', '', False)
timeMeasureCyclicMin = base.TimeMeasure('lifecycle_transition', 'lifecycle_transition == "In Progress"',
                    'lifecycle_transition == "Awaiting Assignment"', 'CYCLIC', 'MIN', '', '', '', False)
timeMeasureCyclicAvg = base.TimeMeasure('lifecycle_transition', 'lifecycle_transition == "In Progress"',
                    'lifecycle_transition == "Awaiting Assignment"', 'CYCLIC', 'AVG', '', '', '', False)

timeMeasureLinear = base.TimeMeasure('lifecycle_transition', 'lifecycle_transition == "In Progress"',
                    'lifecycle_transition == "Closed"', 'LINEAR', 'AVG', '', '', '', False)


class MyTest(unittest.TestCase):
    def testTimeMeasureCyclicSum(self):
        self.assertEqual(measureComputer(timeMeasureCyclicSum, dataframe, id_case, time_column).size, 7371)
    def testTimeMeasureCyclicMax(self):
        self.assertEqual(measureComputer(timeMeasureCyclicMax, dataframe, id_case, time_column).size, 7371)
    def testTimeMeasureCyclicMin(self):
        self.assertEqual(measureComputer(timeMeasureCyclicMin, dataframe, id_case, time_column).size, 7371)
    def testTimeMeasureCyclicAvg(self):
        self.assertEqual(measureComputer(timeMeasureCyclicAvg, dataframe, id_case, time_column).size, 7371)
    def testTimeMeasureLinearFirstFalse(self):
        self.assertEqual(measureComputer(timeMeasureLinear, dataframe, id_case, time_column).size, 7537)
    def testDataMeasureFirstFalse(self):
        self.assertEqual(measureComputer(dataMeasure, dataframe, id_case, time_column).size, 4045)
    def testCountMeasure(self):
        self.assertEqual(measureComputer(countMeasure, dataframe, id_case, time_column).size, 7554)

if __name__ == "__main__":
    unittest.main()

