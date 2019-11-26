from functionsFrame import auxFunctions
import pandas as pd
import datetime as dt
import numpy as np
import datetime

def aggregatedCompute(dataframe, measure, id_case, time_column):

    baseMeasure = measure.baseMeasure.to_frame("data") #Filtro base que habra que hacer tiempo
    filterToApply = measure.filterToApply
    operation = measure.singleInstanceAggFunction
    timeGrouper = measure.grouper
    
    if(filterToApply != ""):
        baseMeasure = baseMeasure[filteredCondition]
    
    time = auxFunctions.timeCalculator(dataframe, False, id_case, time_column)

    baseMeasure['time_to_calculate'] = time

    if(baseMeasure['data'].dtype == 'timedelta64[ns]'):
        baseMeasure['data_seconds'] = baseMeasure['data'].dt.total_seconds()
        baseMeasure['data_seconds'] = baseMeasure['data_seconds'].fillna(0).astype(float)
        istime = True

    if(operation == 'SUM'):
        result = baseMeasure.groupby(pd.Grouper(key='time_to_calculate', freq = timeGrouper.freq)).sum()
        if(istime == True):
            finalResult = result['data_seconds'].apply(lambda x: datetime.timedelta(seconds = x))

    elif(operation == "MIN"):
        result = baseMeasure.groupby(pd.Grouper(key='time_to_calculate', freq = timeGrouper.freq)).min()
        if(istime == True):
            finalResult = result['data_seconds'].apply(lambda x: datetime.timedelta(seconds = x))

    elif(operation == "MAX"):
        result = baseMeasure.groupby(pd.Grouper(key='time_to_calculate', freq = timeGrouper.freq)).max()
        if(istime == True):
            finalResult = result['data_seconds'].apply(lambda x: datetime.timedelta(seconds = x))

    elif(operation == "AVG"):
        result = baseMeasure.groupby(pd.Grouper(key='time_to_calculate', freq = timeGrouper.freq)).mean()
        if(istime == True):
            finalResult = result['data_seconds'].apply(lambda x: datetime.timedelta(seconds = x))

    elif(operation == "GROUPBY"):
        finalResult = baseMeasure.groupby(pd.Grouper(key='time_to_calculate', freq = timeGrouper.freq))

    return finalResult