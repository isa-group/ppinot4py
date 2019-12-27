import datetime
import pandas as pd
import unittest
from PPINot4Py.computer import measureComputer
from PPINot4Py.functionsFrame import baseMeasures
from PPINot4Py import timeGrouper, importBase, computer
from PPINot4Py.measures import base

class MyTest(unittest.TestCase):

    def testComputeAggregatedTimeGrouped(self):
        
        timeMeasureLinearA = baseMeasures.linearTimeMeasure('lifecycle_transition == "In Progress"',
                                                                              'lifecycle_transition == "Awaiting Assignment"',
                                                                               True)
        
        aggregatedMeasure = baseMeasures.aggregatedMeasure(timeMeasureLinearA, '1Y', 'SUM')
        
        IdCase1 = '1-364285768'
        IdCase2 = '2-364285768'
        IdCase3 = '3-364285768'
         
        time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
        time2 = datetime.datetime(2011, 3, 31, 17, 45, 48)
        time3 = datetime.datetime(2012, 4, 6, 16, 44, 7)
        time4 = datetime.datetime(2013, 4, 6, 16, 44, 7)
        time5 = datetime.datetime(2014, 5, 6, 16, 44, 7)
        time6 = datetime.datetime(2015, 6, 6, 16, 44, 7)
        
            
        data = {'case:concept:name':[IdCase1, IdCase1, IdCase2, IdCase2, IdCase3, IdCase3], 
                'time:timestamp': [time1, time2, time3, time4, time5, time6],
                'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment', 'In Progress', 'Awaiting Assignment' ]}
        
        timeResult1 = datetime.timedelta(days=365, minutes=46, seconds=6)
        timeResult2 = datetime.timedelta(seconds=0)
        
        dataframeLinear = pd.DataFrame(data)
        var = measureComputer(aggregatedMeasure, dataframeLinear)
        
        self.assertEqual(var.size, 5)
        self.assertEqual(var.iloc[0] , timeResult1)
        self.assertEqual(var.iloc[1] , timeResult2)
           
    
if __name__ == "__main__":
    unittest.main()