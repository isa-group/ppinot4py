def dataCompute(dataframe, condition, id_case):

    filteredDataframe = dataframe.query(condition.precondition)

    if(condition.first):
        value = filteredDataframe.groupby(id_case)[condition.dataContentSelection].head(1)
    else:
        value = filteredDataframe.groupby(id_case)[condition.dataContentSelection].tail(1)

    return value