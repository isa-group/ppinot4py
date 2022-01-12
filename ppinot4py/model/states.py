import enum

class DataObjectState():
    """Basic state of a condition.
    
    Parameters:  
    ----------  
    
    name: String representation of a condition
    
    """
    
    def __init__(self, name):
        self.name = name
        
    def __repr__(self):
        return "%s" % (self.name)

class ComplexState():    
    """
    DEFINIR LA DESCRIPCION

    Parameters
    ----------
    first: Primera condicion que debe de cumplirse, de tipo TimeInstantCondition

    last: Segunda condicion que debe de cumplirse, de tipo TimeInstantCondition
    
    type:

    """  

    def __init__(self, first, last, state_type):
        self.first = first #TIMEINSTANTCONDITION
        self.last = last #TIMEINSTANTCONDITION Puede ser recursivo porque el last puede ser otro Complex
        self.type = state_type # Es de tipo State-Type

class RuntimeState():

    def __init__(self, state):
        self.state = state
  
RuntimeState.START = RuntimeState('START')
RuntimeState.END = RuntimeState('END')

class Type(enum.Enum):
    FOLLOWS = 1
    LEADSTO = 2
    LEADSTOCYCLIC = 3
