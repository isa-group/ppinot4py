#Possibly not needed
class MeasureDefinition:

    def __init__(self, id, name, description, scale, unitOfMeasure):
        self.id = id
        self.name = name
        self.description = description
        self.scale = scale
        self.unitOfMeasure = unitOfMeasure


    def getId(self):
        return self.id
    
    def setId(self):
        if(self.id == None):
            self.id = ""
        else:
            self.id = id


    def getName(self):
        return self.name
    
    def setName(self):
         if(self.name == None):
                self.name = ""
         else:
            self.name = name
    

    def getDescription(self):
        return self.description

    def setDescription(self):
        self.description = description

    
    def getScale(self):
        return self.scale
    
    def setScale(self):
        self.scale = scale
    
    
    def getUnitOfMeasure(self):
        return self.unitOfMeasure
    
    def setUnitOfMeasure(self):
        self.unitOfMeasure = unitOfMeasure
    

    def valid(self):
        return self.id != None and (self.id is not "")

    
    def getAllIds(self):
        
        if(not self.id):
            allIds = [(self.id, self)]
            
        return allIds
