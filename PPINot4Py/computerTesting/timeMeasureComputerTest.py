import datetime
import pandas as pd
import unittest
from PPINot4Py.computer import measureComputer
from PPINot4Py.functionsFrame import baseMeasures

class MyTest(unittest.TestCase):

    def testComputeLinearInstances(self):
        
        timeMeasureLinearA = baseMeasures.linearTimeMeasure('lifecycle_transition == "In Progress"', 'lifecycle_transition == "Awaiting Assignment"', True)
        
        time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
        time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
        time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
        
        IdCase1 = '1-364285768'
        
        timeResult = datetime.timedelta(minutes=46, seconds=6) 
        
        data = {'case:concept:name':[IdCase1, IdCase1, IdCase1], 
                'time:timestamp': [time1, time2, time3],
                'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress']}
                
        dataframeLinear = pd.DataFrame(data)
        var = measureComputer(timeMeasureLinearA, dataframeLinear).iloc[0]
        self.assertEqual(var[0], timeResult)

    def testComputeLinearInstancesWithSeveralFrom(self):
         
        timeMeasureLinearA = baseMeasures.linearTimeMeasure('lifecycle_transition == "In Progress"', 'lifecycle_transition == "Awaiting Assignment"', False)
        
        IdCase1 = '1-364285768'
        
        time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
        time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
        time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
        time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
        time5 = datetime.datetime(2012, 5, 6, 16, 44, 7)
        time6 = datetime.datetime(2012, 6, 6, 16, 44, 7)
    
        timeResult = datetime.timedelta(minutes=46, seconds=6) 
        
        data = {'case:concept:name':[IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1], 
                'time:timestamp': [time1, time2, time3, time4, time5, time6],
                'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'In Progress', 'In Progress', 'In Progress' ]}
        
        dataframeLinear = pd.DataFrame(data)
        var = measureComputer(timeMeasureLinearA, dataframeLinear).iloc[0]
        self.assertEqual(var[0], timeResult)
        
    def testComputeLinearInstancesWithSeveralToAndFirstTo(self):
         
        timeMeasureLinearA = baseMeasures.linearTimeMeasure('lifecycle_transition == "In Progress"', 'lifecycle_transition == "Awaiting Assignment"', True)
        
        IdCase1 = '1-364285768'
        
        time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
        time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
        time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
        time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
        time5 = datetime.datetime(2012, 5, 6, 16, 44, 7)
        time6 = datetime.datetime(2012, 6, 6, 16, 44, 7)
    
        timeResult = datetime.timedelta(minutes=46, seconds=6) 
        
        data = {'case:concept:name':[IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1], 
                'time:timestamp': [time1, time2, time3, time4, time5, time6],
                'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'In Progress', 'In Progress', 'In Progress' ]}
        
        dataframeLinear = pd.DataFrame(data)
        var = measureComputer(timeMeasureLinearA, dataframeLinear).iloc[0]
        self.assertEqual(var[0], timeResult)
        
    
    def testComputeLinearInstancesWithSeveralToAndNotFirstTo(self):
        
        timeMeasureLinearA = baseMeasures.linearTimeMeasure('lifecycle_transition == "In Progress"', 'lifecycle_transition == "Awaiting Assignment"', False)
        
        IdCase1 = '1-364285768'
        
        time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
        time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
        time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
        time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
        time5 = datetime.datetime(2012, 5, 6, 16, 44, 7)
        time6 = datetime.datetime(2012, 6, 6, 16, 44, 7)
    
        timeResult = datetime.timedelta(days= 797, hours=23, minutes=44, seconds=25) 
        
        data = {'case:concept:name':[IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1], 
                'time:timestamp': [time1, time2, time3, time4, time5, time6],
                'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment', 'In Progress', 'Awaiting Assignment' ]}
        
        dataframeLinear = pd.DataFrame(data)
        var = measureComputer(timeMeasureLinearA, dataframeLinear).iloc[0]
        self.assertEqual(var[0], timeResult)
        
        
    def testComputeLinearInstancesWithSeveralFromAndToAndFirstTo2(self):
        
        timeMeasureLinearA = baseMeasures.linearTimeMeasure('lifecycle_transition == "In Progress"', 'lifecycle_transition == "Awaiting Assignment"', False)
        
        IdCase1 = '1-364285768'
        
        time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
        time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
        time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
        time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
        time5 = datetime.datetime(2012, 5, 1, 16, 44, 7)
        time6 = datetime.datetime(2012, 5, 2, 16, 44, 7)
        time7 = datetime.datetime(2012, 5, 3, 16, 44, 7)
        time8 = datetime.datetime(2012, 6, 6, 16, 44, 7)
        
        timeResult = datetime.timedelta(days= 797, hours=23, minutes=44, seconds=25) 
        
        data = {'case:concept:name':[IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1], 
                'time:timestamp': [time1, time2, time3, time4, time5, time6, time7, time8],
                'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment', 'In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment']}
        
        dataframeLinear = pd.DataFrame(data)
        var = measureComputer(timeMeasureLinearA, dataframeLinear).iloc[0]
        self.assertEqual(var[0], timeResult)
        
    def testComputeCyclicSumInstances(self):
        
        
        timeMeasureCyclic = baseMeasures.cyclicTimeMeasure('lifecycle_transition == "In Progress"', 'lifecycle_transition == "Awaiting Assignment"', 'SUM')
        
        IdCase1 = '1-364285768'
        IdCase2 = '2-364285768'
        
        time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
        time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
        time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
        time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
        time5 = datetime.datetime(2012, 5, 1, 16, 44, 7)
        time6 = datetime.datetime(2012, 5, 2, 16, 44, 7)
        time7 = datetime.datetime(2012, 5, 3, 16, 44, 7)
        time8 = datetime.datetime(2012, 6, 6, 16, 44, 7)
        
        timeResult = datetime.timedelta(days= 732, minutes=46, seconds=6) 
        
        data = {'case:concept:name':[IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase2, IdCase1], 
                'time:timestamp': [time1, time2, time3, time4, time5, time6, time7, time8],
                'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment', 'In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment']}
        
        dataframeLinear = pd.DataFrame(data)
        var = measureComputer(timeMeasureCyclic, dataframeLinear).iloc[0]
        
        self.assertEqual(var[0], timeResult)
        
    def testComputeCyclicMaxInstances(self):
     
        timeMeasureCyclic = baseMeasures.cyclicTimeMeasure('lifecycle_transition == "In Progress"', 'lifecycle_transition == "Awaiting Assignment"', 'MAX')
        
        IdCase1 = '1-364285768'
        IdCase2 = '2-364285768'
        
        time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
        time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
        time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
        time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
        time5 = datetime.datetime(2012, 5, 1, 16, 44, 7)
        time6 = datetime.datetime(2012, 5, 2, 16, 44, 7)
        time7 = datetime.datetime(2012, 5, 3, 16, 44, 7)
        time8 = datetime.datetime(2012, 6, 6, 16, 44, 7)
        
        timeResult = datetime.timedelta(days= 731) 
        
        data = {'case:concept:name':[IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase2, IdCase1], 
                'time:timestamp': [time1, time2, time3, time4, time5, time6, time7, time8],
                'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment', 'In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment']}
        
        dataframeLinear = pd.DataFrame(data)
        var = measureComputer(timeMeasureCyclic, dataframeLinear).iloc[0] 
        
        self.assertEqual(var[0], timeResult)
        
    def testComputeCyclicAvgInstances(self):
        
        timeMeasureCyclic = baseMeasures.cyclicTimeMeasure('lifecycle_transition == "In Progress"', 'lifecycle_transition == "Awaiting Assignment"', 'AVG')
        
        IdCase1 = '1-364285768'
     
        
        time1 = datetime.datetime(2010, 3, 31, 16, 59, 42)
        time2 = datetime.datetime(2010, 3, 31, 17, 45, 48)
        time3 = datetime.datetime(2010, 4, 6, 16, 44, 7)
        time4 = datetime.datetime(2012, 4, 6, 16, 44, 7)
        time5 = datetime.datetime(2012, 5, 1, 16, 44, 7)
        time6 = datetime.datetime(2012, 5, 2, 16, 44, 7)
        time7 = datetime.datetime(2012, 5, 3, 16, 44, 7)
        time8 = datetime.datetime(2012, 6, 6, 16, 44, 7)
        
        timeResult = datetime.timedelta(days= 153, hours=4, minutes=57, seconds=13.200000) 
        
        data = {'case:concept:name':[IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1, IdCase1], 
                'time:timestamp': [time1, time2, time3, time4, time5, time6, time7, time8],
                'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment', 'In Progress', 'Awaiting Assignment','In Progress', 'Awaiting Assignment']}
        
        dataframeLinear = pd.DataFrame(data)
        var = measureComputer(timeMeasureCyclic, dataframeLinear).iloc[0]
     
        self.assertEqual(var[0], timeResult)
        
    
if __name__ == "__main__":
    unittest.main()
