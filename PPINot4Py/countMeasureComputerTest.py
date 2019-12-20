import pandas as pd
import unittest
from PPINot4Py.computer import measureComputer
from PPINot4Py.state import DataObjectState
from PPINot4Py.condition import Condition
from PPINot4Py.measures import base
from PPINot4Py.functionsFrame import measuresDefinitionAux

class MyTest(unittest.TestCase):
    
    def testCompute(self):
        
        countMeasure = auxiliarCountMeasure('lifecycle_transition == "In Progress"')
    
        IdCase1 = '1-364285768'
        
        data = {'case:concept:name':[IdCase1, IdCase1, IdCase1], 
                'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress']}
                
        dataframeLinear = pd.DataFrame(data)
        var = measureComputer(countMeasure, dataframeLinear).iloc[0]

        self.assertEqual(var, 2)
        
    def testComputeNotAppear(self):
        
        countMeasure = auxiliarCountMeasure('lifecycle_transition == "Closed"')
        
        IdCase1 = '1-364285768'
        
        data = {'case:concept:name':[IdCase1, IdCase1, IdCase1], 
                'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress']}
                
        dataframeLinear = pd.DataFrame(data)
        var = measureComputer(countMeasure, dataframeLinear).iloc[0]

        self.assertEqual(var, 0)
        
    def testComputeInstances(self):
        
        countMeasure = auxiliarCountMeasure('lifecycle_transition == "In Progress"')
        
        IdCase1 = '1-364285768'
        idCase2 = '2-364285768'
        
        data = {'case:concept:name':[IdCase1, IdCase1, IdCase1, idCase2, idCase2, idCase2], 
                'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress', 'In Progress', 'In Progress', 'In Progress']}
                
        dataframeLinear = pd.DataFrame(data)
        var = measureComputer(countMeasure, dataframeLinear)

        self.assertEqual(var.iloc[0], 2)
        self.assertEqual(var.iloc[1], 0)
        
        
if __name__ == "__main__":
    unittest.main()
