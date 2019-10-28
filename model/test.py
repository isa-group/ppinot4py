import Type
from base import CountMeasure
from state import RunTimeState
import json

#---Definition of a CountMeasure with a complexState-----------------------
level2 = RunTimeState.DataObjectState("fafafa")
level21 = RunTimeState.DataObjectState("fafafa")

varComplex = RunTimeState.ComplexState(level2, level21, Type.Type.FOLLOWS)

varComplex1 = CountMeasure.TimeInstantCondition(varComplex)

variable2 = CountMeasure.CountMeasure(varComplex1)


#---Definition of a CountMeasure with a DataObjectState--------------------

varData = RunTimeState.DataObjectState("ESTADO == 'Fijada' && PRIORIDAD == '${priority}' && (RESOLUTOR == 'SAS OFIMATICA-LAN' || RESOLUTOR == 'RESOLUTOR-PUESTO-USUARIO') && TIPOLOGIA.startsWith('Incidencia')")

var = CountMeasure.TimeInstantCondition(varData)

variable = CountMeasure.CountMeasure(var)

#-----------------------------------------------------------------------
#data='{"channel":{"lastBuild":"2013-11-12", "component":["test1", "test2"]}}'
with open('jsonTest.json') as json_file:
    #json_string = json.dump(json_file)
    accionesDeResolucionPorSPU = json.load(json_file, object_hook= CountMeasure.CountMeasure)

#jsonobject = json.loads( data, object_hook= CountMeasure.CountMeasure)

#------------------------------------------------------------------------

#vartorara = accionesDeResolucionPorSPU.when.changesToState.getName()

#print(accionesDeResolucionPorSPU.when.changesToState.getName())
print(accionesDeResolucionPorSPU.when.changesToState.name.values())
#print(accionesDeResolucionPorSPU)


#"name": "ESTADO == 'Fijada' && PRIORIDAD == '${priority}' && (RESOLUTOR == 'SAS OFIMATICA-LAN' || RESOLUTOR == 'RESOLUTOR-PUESTO-USUARIO') && TIPOLOGIA.startsWith('Incidencia')"
         