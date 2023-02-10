from abc import abstractmethod


class TransportAVLData:
    @classmethod
    @abstractmethod
    def from_response_row(cls, response_row: dict) -> 'TransportAVLData':
        pass

    @abstractmethod
    def __eq__(self, other: 'TransportAVLData') -> bool:
        pass

    @abstractmethod
    def __hash__(self):
        pass
