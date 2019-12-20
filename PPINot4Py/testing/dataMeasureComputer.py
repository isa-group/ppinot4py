import pandas as pd
import unittest
from PPINot4Py.computer import measureComputer
from PPINot4Py.state import DataObjectState
from PPINot4Py.condition import Condition
from PPINot4Py.measures import base
from PPINot4Py.functionsFrame import measuresDefinitionAux

class MyTest(unittest.TestCase):
    
    def testDataComputer(self):
        
        dataMeasure = auxiliarDataMeasure("concept_name == 'Queued'", "lifecycle_transition", False)
        
        auxiliarDataMeasure
    
        IdCase1 = '1-364285768'
        
        data = {'case:concept:name': [IdCase1, IdCase1, IdCase1], 
                'concept:name': ['Queued', 'Queued', 'Not queued'],
                'lifecycle:transition': ['In Progress', 'Awaiting Assignment','In Progress']}
                
        dataframeLinear = pd.DataFrame(data)
        result = measureComputer(countMeasure, dataframeLinear)
        
        self.assertEqual(result.iloc[0], True)
        
        
if __name__ == "__main__":
    unittest.main()
