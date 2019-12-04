from condition.conditionChooser import conditionChooser
import pandas as pd
import measureComputer
import datetime
import numpy as np

def derivedCompute(dataframe, condition, id_case, time_column):
    
    function = condition.functionExpression
    measureMap = condition.measureMap
    dataFrameComputer = pd.DataFrame()
    istime = False
    
    for key in measureMap:  
        dataFrameComputer[key] = measureComputer.measureComputer(measureMap[key], dataframe)
        
        if(dataFrameComputer[key].dtype == 'timedelta64[ns]'):
            dataFrameComputer[key] = dataFrameComputer[key].dt.total_seconds()
            dataFrameComputer[key] = dataFrameComputer[key].fillna(0).astype(float)
            istime = True
  
    result = dataFrameComputer.eval(function)
    result = result.replace([np.inf, -np.inf], 0)
    finalResult = result.fillna(0).astype(float)
    
    if(istime == True):
        finalResult = finalResult.apply(lambda x: datetime.timedelta(seconds = x))
    
    return finalResult