import json
import sys
import os 
import numpy as np
import time
import shutil 
from pm4py.objects.log.adapters.pandas import csv_import_adapter
from pm4py.objects.log.importer.xes import factory as xes_import_factory
from pm4py.objects.log.exporter.csv import factory as csv_exporter
from pm4py.algo.filtering.pandas.attributes import attributes_filter
from pm4py.util import constants

def function():
    with open('jsonTest.json') as json_file:
        testJson = json.load(json_file)
        json_file.close()

    id_case = 'case:concept:name'

    #Creating the .csv in case is the first time we run the program
    if(os.path.exists("log_in_csv.csv") == False):
        log = xes_import_factory.apply('bpi_challenge_2013_incidents.xes')
        csv_exporter.export(log, "log_in_csv.csv")

    #Taking the condition value from Json
    condition = testJson["when"]["changesToState"]["dataObjectState"]["name"]

    #Splitting condition to take the KEY and the VALUE
    condition = condition.split("==")
    condition[0] = condition[0].strip()
    condition[1] = condition[1].strip()

    #Loading .csv in dataframe
    dataframe = csv_import_adapter.import_dataframe_from_path(os.path.join("log_in_csv.csv"))

    dataframe = attributes_filter.apply(dataframe, [condition[1]], 
                    parameters={constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY: condition[0], "positive": True})

    dataframe['case_next'] = dataframe[condition[0]].shift(-1)
    dataframe['id_next'] = dataframe[id_case].shift(-1)

    dataframe['final_evaluation'] = ((dataframe[condition[0]] != condition[1]) &
                                        (dataframe['case_next'] == condition[1]) & 
                                            (dataframe[id_case] == dataframe['id_next']))

    value = dataframe.groupby(id_case)['final_evaluation'].sum()

    return value


start_time = time.process_time()
print("Number of changes:\n %s\n" % function())
print("--- %s seconds ---" % (time.process_time() - start_time))


