from __future__ import annotations
import enum
import re
from typing import ClassVar

class DataObjectState():
    """Basic state of a condition.
    
    Args:    
            - name: String representation of a condition
    """
    
    def __init__(self, name):
        self.name = name
        
    def __repr__(self):
        return "%s" % (_format_condition_human_readable(self.name))

    def __str__(self):
        return self.__repr__()

def _format_condition_human_readable(condition: str):
    if not isinstance(condition, str):
        return str(condition)

    match = re.fullmatch(r"\s*`?([^`]+)`?\s*==\s*(['\"])(.*?)\2\s*", condition)
    if not match:
        return condition

    attribute = match.group(1)
    value = match.group(3)

    if attribute == "concept:name":
        return f'activity "{value}" occurs'
    if attribute == "lifecycle:transition":
        return f'lifecycle transition "{value}" occurs'

    return condition

class ComplexState():    
    def __init__(self, first, last, state_type):
        if isinstance(first, str):
            first = DataObjectState(first)
        if isinstance(last, str):
            last = DataObjectState(last)
        if state_type not in (Type.FOLLOWS, Type.LEADSTO):
            raise ValueError("ComplexState type must be Type.FOLLOWS or Type.LEADSTO")

        self.first = first
        self.last = last
        self.type = state_type

    def __repr__(self):
        if self.type == Type.FOLLOWS:
            return f"{self.last} immediately follows {self.first}"
        if self.type == Type.LEADSTO:
            return f"{self.first} leads to {self.last} (one match per {self.first})"
        return f"{self.first} -> {self.last}"

    def __str__(self):
        return self.__repr__()


class RuntimeState:
    START: ClassVar["RuntimeState"]
    END: ClassVar["RuntimeState"]

    def __init__(self, state: str):
        if not isinstance(state, str) or not state.strip():
            raise ValueError("state must be a non-empty string")
        self.state = state.strip()

    def __repr__(self) -> str:
        return self.state

    def __str__(self) -> str:
        return self.state

    def __eq__(self, other: object) -> bool:
        return isinstance(other, RuntimeState) and self.state == other.state

    def __hash__(self) -> int:
        return hash(self.state)

RuntimeState.START = RuntimeState("START")
RuntimeState.END = RuntimeState("END")


class Type(enum.Enum):
    FOLLOWS = 1
    LEADSTO = 2
    LEADSTOCYCLIC = 3
