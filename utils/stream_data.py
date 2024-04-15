import sys
sys.path.append('/users/ngoyal')

from src.streams.gbif_stream import GbifStream
from src.streams.idigbio_stream import IdigbioStream
from src.streams.obis_stream import ObisStream

class StreamDataStrategy:
    
    def __init__(self, topic_name):
        self.topic = topic_name

    def select_stream(self):
        if self.topic == "gbif":
            return GbifStream()
        elif self.topic == "idigbio":
            return IdigbioStream()
        elif self.topic == "obis":
            return ObisStream()
        else:
            raise ValueError("Invalid criteria")
