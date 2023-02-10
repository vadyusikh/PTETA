from abc import abstractmethod


class TransportOperator:
    @classmethod
    @abstractmethod
    def from_response_row(cls, response_row: dict) -> 'TransportOperator':
        pass
