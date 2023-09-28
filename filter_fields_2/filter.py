from protocol import Serializer

class FilterDistance:
    def __init__(self):
        self.serializer = Serializer()

    def run(self):
        self.serializer.run(self.filter)
    
    def filter(self):
        pass
    
