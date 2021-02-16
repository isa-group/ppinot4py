from pm4py.objects.log.adapters.pandas import csv_import_adapter
from pm4py.objects.log.importer.xes import factory as xes_import_factory
from pm4py.objects.log.exporter.csv import factory as csv_exporter
import os

def dataframeImport(XESFile, csv = 'log_in_csv.csv'):
    
    if(os.path.exists(csv) == False):
        log = xes_import_factory.apply(XESFile)
        csv_exporter.export(log, "log_in_csv.csv")

    # Loading .csv in dataframe
    dataframe = csv_import_adapter.import_dataframe_from_path(os.path.join("log_in_csv.csv"))
    
    return dataframe