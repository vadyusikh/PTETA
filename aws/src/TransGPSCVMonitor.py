import time

from PTETA.utils.transport.BaseDBAccessDataclass import BaseDBAccessDataclass
from PTETA.utils.transport.TransportOperator import TransportOperator
from PTETA.utils.transport.TransportRoute import TransportRoute
from PTETA.utils.transport.TransportVehicle import TransportVehicle
from psycopg2.extensions import connection as Connection
import psycopg2

from typing import List, Union


class TransGPSCVMonitor:
    db_connection: Connection = None

    operator_to_id: dict = dict()
    route_to_id: dict = dict()
    vehicle_to_id: dict = dict()

    objects_unique = None

    def __init__(self, connection_config: dict, data_model: str, **kwarg: dict) -> None:
        self.connection_config = connection_config
        self.db_connection = psycopg2.connect(**connection_config)

        self.define_data_model(data_model)

        self.reload_operators()
        self.reload_routes()
        self.reload_vehicles()

        self.datetime_format = '%Y-%m-%d %H:%M:%S'

    def reconnect(self):
        self.db_connection = psycopg2.connect(**self.connection_config)

    def define_data_model(self, data_model: str):
        # TODO : define data model in configs
        if data_model not in ["chernivtsi", "kharkiv"]:
            raise ValueError(f"Wrong data model!")

        self.data_model = data_model

        if data_model == "chernivtsi":
            from PTETA.utils.transport.chernivtsi.ChernivtsiTransportOperator import ChernivtsiTransportOperator
            from PTETA.utils.transport.chernivtsi.ChernivtsiTransportRoute import ChernivtsiTransportRoute
            from PTETA.utils.transport.chernivtsi.ChernivtsiTransportVehicle import ChernivtsiTransportVehicle
            from PTETA.utils.transport.chernivtsi.ChernivtsiTransportAVLData import ChernivtsiTransportAVLData
            self.operator_cls = ChernivtsiTransportOperator
            self.route_cls = ChernivtsiTransportRoute
            self.vehicle_cls = ChernivtsiTransportVehicle
            self.avl_data_cls = ChernivtsiTransportAVLData
        elif data_model == "kharkiv":
            from PTETA.utils.transport.kharkiv.KharkivTransportOperator import KharkivTransportOperator
            from PTETA.utils.transport.kharkiv.KharkivTransportRoute import KharkivTransportRoute
            from PTETA.utils.transport.kharkiv.KharkivTransportVehicle import KharkivTransportVehicle
            from PTETA.utils.transport.kharkiv.KharkivTransportAVLData import KharkivTransportAVLData
            self.operator_cls = KharkivTransportOperator
            self.route_cls = KharkivTransportRoute
            self.vehicle_cls = KharkivTransportVehicle
            self.avl_data_cls = KharkivTransportAVLData

        self.objects_unique = {
            self.operator_cls: set(),
            self.route_cls: set(),
            self.vehicle_cls: set()
        }

    def reload_operators(self):
        operator_list: List[TransportOperator] = self.operator_cls.get_table(self.db_connection)
        self.objects_unique[self.operator_cls] = set(operator_list)
        self.operator_to_id = dict({operator: operator.id for operator in operator_list})

    def reload_routes(self):
        route_list: List[TransportRoute] = self.route_cls.get_table(self.db_connection)
        self.objects_unique[self.route_cls] = set(route_list)
        self.route_to_id = dict({route: route.id for route in route_list})

    def reload_vehicles(self):
        vehicle_list: List[TransportVehicle] = self.vehicle_cls.get_table(self.db_connection)
        self.objects_unique[self.vehicle_cls] = set(vehicle_list)
        self.vehicle_to_id = dict({vehicle: vehicle.id for vehicle in vehicle_list})

    def get_new_objs(self, obj_list: List[BaseDBAccessDataclass]) -> List[BaseDBAccessDataclass]:
        seen_obj = set()
        seen_add = seen_obj.add
        unique_obj_list = [x for x in obj_list if not (x in seen_obj or seen_add(x))]

        return [obj for obj in unique_obj_list
                if obj not in self.objects_unique[obj.__class__]]

    def update_db(self, obj_list: List[BaseDBAccessDataclass]):
        current_class = obj_list[0].__class__
        are_in_db_list = current_class.are_in_table(self.db_connection, obj_list)

        obj_to_insert = [obj
                         for obj, is_in in zip(obj_list, are_in_db_list)
                         if not is_in]

        for obj in obj_to_insert:
            obj.insert_many_in_table(self.db_connection, [obj])
            if isinstance(obj, self.operator_cls):
                self.reload_operators()
            elif isinstance(obj, self.route_cls):
                self.reload_routes()
            elif isinstance(obj, self.vehicle_cls):
                self.reload_vehicles()

    @classmethod
    def row_validation(cls, row: dict) -> bool:
        if (row['lat'] is None) or (row['lng'] is None):
            return False
        return True

    def decompose_response(self, response: List[dict], valid_fn=None) -> Union:
        if valid_fn is None:
            valid_fn = self.row_validation

        operator_list, route_list = list(), list()
        vehicle_list, avl_data_list = list(), list()

        for row in response:
            if not valid_fn(row):
                continue
            operator_list.append(self.operator_cls.from_response_row(row))
            route_list.append(self.route_cls.from_response_row(row))
            vehicle_list.append(self.vehicle_cls.from_response_row(row))
            avl_data_list.append(self.avl_data_cls.from_response_row(row))

        return operator_list, route_list, vehicle_list, avl_data_list

    def write_to_db(self, response):
        operator_list, route_list, vehicle_list, avl_data_list = self.decompose_response(response)

        for obj_list in [operator_list, route_list, vehicle_list]:
            new_obj = self.get_new_objs(obj_list)
            if new_obj:
                print(f"There are {len(new_obj)} new {new_obj[0].__class__} to inserted in DB")
                self.update_db(new_obj)

        for i, (vehicle, route) in enumerate(zip(vehicle_list, route_list)):
            avl_data_list[i].vehicle_id = self.vehicle_to_id[vehicle]
            avl_data_list[i].route_id = self.route_to_id[route]

        try:
            self.avl_data_cls.insert_many_in_table(self.db_connection, avl_data_list)
        except self.db_connection.Error:
            self.reconnect()
            time.sleep(1)
            self.avl_data_cls.insert_many_in_table(self.db_connection, avl_data_list)
