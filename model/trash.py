class CountMeasure():
    
    def __init__(self, when):
        when = TimeInstantCondition(when)
        self.when = when

    def getWhen(self):
        return self.when

    def setWhen(self):
        self.when = when

    def __repr__(self):
        return "%s" % (self.when)



x = accionesDeResolucionPorSPU.getWhen()
y = accionesDeResolucionPorSPU.when.changesToState
z = accionesDeResolucionPorSPU.when.changesToState.name
w = testJson["when"]["changesToState"]["dataObjectState"]["name"]

print()

print(x)
print(y)
print(z)
print(w)
print()

print(type(accionesDeResolucionPorSPU.when.changesToState.name))
print(type(accionesDeResolucionPorSPU.when))
print(type(accionesDeResolucionPorSPU.when.changesToState))
print()


import Type
from base import CountMeasure
from state import RunTimeState
import json
from pm4py.objects.log.importer.xes import factory as xes_import_factory
from pm4py.algo.filtering.log.attributes import attributes_filter
from pm4py.algo.filtering.log.timestamp import timestamp_filter
from pm4py.algo.filtering.log.cases import case_filter

#---Definition of a CountMeasure with a complexState-----------------------
level2 = RunTimeState.DataObjectState("firsto")
level21 = RunTimeState.DataObjectState("secondto")

varComplex = RunTimeState.ComplexState(level2, level21, Type.Type.FOLLOWS)
variable2 = CountMeasure.CountMeasure(varComplex)

print(variable2.when.changesToState.first)
#--------------------------------------------------------------------------

#---Definition of a CountMeasure with a DataObjectState--------------------
varData = RunTimeState.DataObjectState("ESTADO == 'Fijada' && PRIORIDAD == '${priority}' && (RESOLUTOR == 'SAS OFIMATICA-LAN' || RESOLUTOR == 'RESOLUTOR-PUESTO-USUARIO') && TIPOLOGIA.startsWith('Incidencia')")
var = CountMeasure.TimeInstantCondition(varData)
variable = CountMeasure.CountMeasure(var)
#--------------------------------------------------------------------------

#------------------Load raw Json-------------------------------------------
with open('jsonTest.json') as json_file:
    #json_string = json.dump(json_file)
    testJson = json.load(json_file)
    #accionesDeResolucionPorSPU = json.load(json_file, object_hook= CountMeasure.CountMeasure)
    json_file.close()
#---------------------------------------------------------------------------


#--------------Load Json to class-------------------------------------------
with open('jsonTest.json') as json_file:
    #json_string = json.dump(json_file)
    #accionesDeResolucionPorSPU = json.load(json_file)
    accionesDeResolucionPorSPU = json.load(json_file, object_hook= CountMeasure.CountMeasure)
    json_file.close()
#---------------------------------------------------------------------------


print()


#"name": "ESTADO == 'Fijada' && PRIORIDAD == '${priority}' && (RESOLUTOR == 'SAS OFIMATICA-LAN' || RESOLUTOR == 'RESOLUTOR-PUESTO-USUARIO') && TIPOLOGIA.startsWith('Incidencia')"

# -----------------------------LOG TEST-----------------------------
#log = xes_import_factory.apply("bpi_challenge_2013_incidents.xes")
#first_trace_concept_name = log.attributes[testJson["when"]["changesToState"]["dataObjectState"]["name"]]
#print(first_trace_concept_name)

#print(resources)
#-------------------------------------------------------------------        

log = xes_import_factory.apply("bpi_challenge_2013_incidents.xes")
activities = attributes_filter.get_attribute_values(log, testJson["when"]["changesToState"]["dataObjectState"]["name"])
resources = attributes_filter.get_attribute_values(log, "org:resource")
filtered_log = timestamp_filter.filter_traces_contained(log, "2011-03-09 00:00:00", "2013-01-18 23:59:59")
filtered_log_intersection = timestamp_filter.filter_traces_intersecting(log, "2011-03-09 00:00:00", "2012-01-18 23:59:59")
filtered_log_events = timestamp_filter.apply_events(log, "2011-03-09 00:00:00", "2012-01-18 23:59:59")
filtered_log_performance = case_filter.filter_on_case_performance(log, 86400, 864000)

print("Logs by type: " + str(activities))
print("logs contained in timestamp: %s" % len(filtered_log))
print("Logs in the intersection: %s" % len(filtered_log_intersection))
print("logs in the time: %s" % len(filtered_log_events))
print("Logs with performance between 1 and 10 days: %s" % len(filtered_log_performance))



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

dataframe = csv_import_adapter.import_dataframe_from_path(os.path.join("log_in_csv.csv"))

#dataframe = attributes_filter.apply(dataframe, ["1-364285768"], 
#                parameters={constants.PARAMETER_CONSTANT_ATTRIBUTE_KEY: "case:concept:name", "positive": True})


#filt_foureyes_neg = ltl_checker.four_eyes_principle(dataframe, "Accepted", "Queued", parameters={"positive": False})

filt_A_ev_B_pos = ltl_checker.A_eventually_B(dataframe, "Accepted", "Accepted",  parameters={"positive": False})

#attr_value_different_persons_pos = ltl_checker.attr_value_different_persons(dataframe, "1-364285768", parameters={"positive": True})
                                                                        
#filt_A_next_B_next_C_pos = ltl_checker.A_next_B_next_C(dataframe, "V30", "V30", "V5 3rd", parameters={"positive": True})
#print(dataframe)

#print(len(filt_foureyes_neg[0]))
print(filt_A_ev_B_pos)
#print(attr_value_different_persons_pos)
#print(filt_A_next_B_next_C_pos)


print("--- %s seconds ---" % (time.process_time() - start_time))










