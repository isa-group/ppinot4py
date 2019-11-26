from condition.conditionChooser import conditionChooser

def countCompute(dataframe, condition, id_case):

    precond = (condition.when)

    filteredSeries = conditionChooser(dataframe, id_case, precond)

    value = filteredSeries.groupby(dataframe[id_case]).sum()

    return value
