import datetime
import pandas as pd
import unittest
from PPINot4Py.computer import measureComputer
from PPINot4Py.functionsFrame import baseMeasures
from PPINot4Py import timeGrouper, importBase, computer
from PPINot4Py.measures import base

class MyTest(unittest.TestCase):
    
    def testComputeInstances(self):
        
        timeMeasureLinearA = baseMeasures.linearTimeMeasure('lifecycle_transition == "In Progress"',
                                                                              'lifecycle_transition == "Awaiting Assignment"',
                                                                               True)
        
        timeMeasureLinearB = baseMeasures.linearTimeMeasure('lifecycle_transition == "In Progress"',
                                                                              'lifecycle_transition == "Closed"',
                                                                               True)
        
        timeMeasureLinearC = baseMeasures.linearTimeMeasure('lifecycle_transition == "In Progress"',
                                                                              'lifecycle_transition == "Pending"',
                                                                               True)
        
        measure_dictionary = {'ProgressTime': timeMeasureLinearA, 'ClosedTime': timeMeasureLinearB, 'PendingTime': timeMeasureLinearC}
        derivedMeasure = base.derivedMeasure('(ProgressTime + ClosedTime) + PendingTime', measure_dictionary)
        
        IdCase1 = '1-364285768'
        
        timeResult = datetime.timedelta(days= 3360, minutes=14, seconds=56) 
         
        time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
        time2 = datetime.datetime(2011, 3, 31, 17, 45, 48)
        time3 = datetime.datetime(2012, 4, 6, 16, 44, 7)
        time4 = datetime.datetime(2013, 4, 6, 16, 44, 7)
        time5 = datetime.datetime(2014, 5, 6, 16, 44, 7)
        time6 = datetime.datetime(2015, 6, 6, 16, 44, 7)
         
        data = {'case:concept:name':[IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1], 
                'time:timestamp': [time1, time2, time3, time4, time5, time6],
                'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'Closed', 'In Progress', 'Pending']}
        
        dataframeLinear = pd.DataFrame(data)
        var = measureComputer(derivedMeasure, dataframeLinear)
        
        self.assertEqual(var[0], timeResult)
    


if __name__ == "__main__":
    unittest.main()