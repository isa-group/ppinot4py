from functionsFrame import auxFunctions
import pandas as pd
import datetime as dt
import numpy as np
import datetime
import measureComputer

def aggregatedCompute(dataframe, measure, id_case, time_column):

    baseMeasure = measure.baseMeasure
    filterToApply = measure.filterToApply
    operation = measure.singleInstanceAggFunction
    timeGrouper = measure.grouper
    istime = False
    
    filteredDataframe = measureComputer.measureComputer(baseMeasure, dataframe)
    
    time_column = auxFunctions.timeCalculator(dataframe, False, id_case, time_column)
    
    if(filterToApply != ""):
        filteredCondition = measureComputer.measureComputer(filterToApply, dataframe)
        filteredDataframe = filteredDataframe[filteredCondition]
        time_column = time_column[filteredCondition]
    
    filteredDataframe['time_to_calculate'] = time_column
    
    if(filteredDataframe['data'].dtype == 'timedelta64[ns]'):
        filteredDataframe['data_seconds'] = filteredDataframe['data'].dt.total_seconds()
        filteredDataframe['data_seconds'] = filteredDataframe['data_seconds'].fillna(0).astype(float)
        istime = True

    if(operation == 'SUM'):
        finalResult = filteredDataframe.groupby(pd.Grouper(key='time_to_calculate', freq = timeGrouper.freq)).sum()
        if(istime == True):
            finalResult = finalResult['data_seconds'].apply(lambda x: datetime.timedelta(seconds = x))

    elif(operation == "MIN"):
        finalResult = filteredDataframe.groupby(pd.Grouper(key='time_to_calculate', freq = timeGrouper.freq)).min()
        if(istime == True):
            finalResult = finalResult['data_seconds'].apply(lambda x: datetime.timedelta(seconds = x))

    elif(operation == "MAX"):
        finalResult = filteredDataframe.groupby(pd.Grouper(key='time_to_calculate', freq = timeGrouper.freq)).max()
        if(istime == True):
            finalResult = finalResult['data_seconds'].apply(lambda x: datetime.timedelta(seconds = x))

    elif(operation == "AVG"):
        finalResult = filteredDataframe.groupby(pd.Grouper(key='time_to_calculate', freq = timeGrouper.freq)).mean()
        if(istime == True):
            finalResult = finalResult['data_seconds'].apply(lambda x: datetime.timedelta(seconds = x))

    elif(operation == "GROUPBY"):
        finalResult = filteredDataframe.groupby(pd.Grouper(key='time_to_calculate', freq = timeGrouper.freq))

    return finalResult