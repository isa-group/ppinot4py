class grouper():   
   """ Time grouper for aggregated Measure.
    
   Args:
    
         - freq: Condition that you want to count.
         - axis: Number/name of the axis, defaults to '0'.
         - sort: Whether to sort the resulting labels, default to 'False'.
         - closed: Closed end of interval. Only when freq parameter is passed. default to 'right'.
         - label: Interval boundary to use for labeling. Only when freq parameter is passed. Default to 'right'.
         
   """
   def __init__(self, freq, axis = 0, sort = False, closed = 'right', label = 'right'):
         
      self.freq = freq
      self.axis = axis
      self.sort = sort
      self.closed = closed
      self.label = label
