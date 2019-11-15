def countCompute(dataframe, condition, id_case):

    dataframe['id_next'] = dataframe[id_case].shift(-1)



    var = dataframe.query(str(condition.when))

    dataframe["partial_evaluation"] = dataframe.index.isin(var.index)
    dataframe["partial_evaluation_next"] = dataframe["partial_evaluation"].shift(-1)

    dataframe['final_evaluation'] = ((dataframe["partial_evaluation"] == False) &
                                        (dataframe["partial_evaluation_next"] == True) & 
                                            (dataframe[id_case] == dataframe['id_next']))

    value = dataframe.groupby(id_case)['final_evaluation'].sum()

    return value