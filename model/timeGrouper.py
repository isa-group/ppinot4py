class grouper():

     def __init__(self, freq, axis = 0, sort = False, closed = 'right', label = 'right'):
         
        self.freq = freq
        self.axis = axis
        self.sort = sort
        self.closed = closed
        self.label = label
