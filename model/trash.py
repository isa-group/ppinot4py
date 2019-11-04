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