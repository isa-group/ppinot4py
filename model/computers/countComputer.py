from condition.conditionChooser import conditionChooser

def countCompute(dataframe, condition, id_case):

    precondition = (condition.when)

    filtered_series = conditionChooser(dataframe, id_case, precondition)

    result = filtered_series.groupby(dataframe[id_case]).sum()

    return result
