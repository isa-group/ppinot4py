import os
import json
import sys
import time
from pm4py.objects.log.importer.xes import factory as xes_importer
from pm4py.algo.filtering.log.ltl import ltl_checker
from pm4py.objects.log.adapters.pandas import csv_import_adapter
from pm4py.algo.filtering.pandas.attributes import attributes_filter
from pm4py.util import constants


with open(sys.argv[1]) as json_file:
    testJson = json.load(json_file)
    json_file.close()

start_time = time.process_time()

#log = xes_importer.apply(os.path.join("bpi_challenge_2013_incidents.xes"))

dataframe = csv_import_adapter.import_dataframe_from_path(os.path.join("outputFile2.csv"))

#dataframe = attributes_filter.apply(dataframe, ["1-364285768"], 
#                parameters={constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY: "case:concept:name", "positive": True})


#filt_foureyes_neg = ltl_checker.four_eyes_principle(dataframe, "Accepted", "Queued", parameters={"positive": False})

filt_A_ev_B_pos = ltl_checker.A_eventually_B(dataframe, "Accepted", "Accepted",  parameters={"positive": False})

#attr_value_different_persons_pos = ltl_checker.attr_value_different_persons(dataframe, "1-364285768", parameters={"positive": True})
                                                                        
#filt_A_next_B_next_C_pos = ltl_checker.A_next_B_next_C(dataframe, "V30", "V30", "V5 3rd", parameters={"positive": True})
#print(dataframe)

#print(len(filt_foureyes_neg[0]))
print(filt_A_ev_B_pos[0])
#print(attr_value_different_persons_pos)
#print(filt_A_next_B_next_C_pos)


print("--- %s seconds ---" % (time.process_time() - start_time))