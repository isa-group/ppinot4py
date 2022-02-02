import enum

class DataObjectState():
    """Basic state of a condition.
    
    Args:    
            - name: String representation of a condition
    """
    
    def __init__(self, name):
        self.name = name
        
    def __repr__(self):
        return "%s" % (self.name)

class ComplexState():    
        def __init__(self, first, last, state_type):
            self.first = first
            self.last = last
            self.type = state_type

class RuntimeState():

    def __init__(self, state):
        self.state = state
    
    def __repr__(self) -> str:
        if(self.state == 'START'):
            return f"when starts"
        else:
            return "when  ends"
        
  
RuntimeState.START = RuntimeState('START')
RuntimeState.END = RuntimeState('END')

class Type(enum.Enum):
    FOLLOWS = 1
    LEADSTO = 2
    LEADSTOCYCLIC = 3


