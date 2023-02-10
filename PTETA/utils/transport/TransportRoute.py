from abc import abstractmethod


class TransportRoute:
    @classmethod
    @abstractmethod
    def from_response_row(cls, response_row: dict) -> 'TransportRoute':
        pass
