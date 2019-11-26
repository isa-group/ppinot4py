import pandas as pd

def timeInstantConditionFunction(dataframe, id_case, var):

    id_next = dataframe[id_case].shift(-1)
    condition = dataframe.query(var)

    dataframeValue = dataframe.index.isin(condition.index)

    conditionInSeries = pd.Series(dataframeValue)

    partial_evaluation_next = conditionInSeries.shift(+1).fillna(False)

    final_evaluation = ((conditionInSeries == True) &
                            (partial_evaluation_next == False) & 
                                (dataframe[id_case] == id_next))

    return final_evaluation


def timeCalculator(dataframe, isFirst, id_case, time_column):

    if(isFirst == True):
        result = dataframe.groupby(id_case)[time_column].first()
        
    else:
        result = dataframe.groupby(id_case)[time_column].last()

    return result