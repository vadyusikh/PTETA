from datetime import datetime, timedelta
from typing import List, Union

import psycopg2
from psycopg2.extensions import connection as Connection

from transport.BaseDBAccessDataclass import BaseDBAccessDataclass
from PTETA.utils.transport.TransportOperator import TransportOperator
from PTETA.utils.transport.TransportRoute import TransportRoute
from PTETA.utils.transport.TransportVehicle import TransportVehicle
from PTETA.utils.transport.TransportAVLData import TransportAVLData


class RequestProcessor:
    db_connection: Connection = None

    operator_to_id: dict = dict()
    route_to_id: dict = dict()
    vehicle_to_id: dict = dict()

    objects_unique = {
        TransportOperator: set(),
        TransportRoute: set(),
        TransportVehicle: set()
    }

    def __init__(self, connection_config: dict, **kwarg: dict) -> None:
        self.db_connection = psycopg2.connect(**connection_config)

        self.reload_operators()
        self.reload_routes()
        self.reload_vehicles()

    def reload_(self, obj):
        if isinstance(obj, TransportOperator):
            self.reload_operators()
        elif isinstance(obj, TransportRoute):
            self.reload_routes()
        elif isinstance(obj, TransportVehicle):
            self.reload_vehicles()
        else:
            raise ValueError(f"Reload operation for class '{obj.__class__}' not defined!")

    def reload_operators(self):
        operator_list = TransportOperator.get_table(self.db_connection)
        self.objects_unique[TransportOperator] = set(operator_list)
        self.operator_to_id = dict({operator: operator.id for operator in operator_list})

    def reload_routes(self):
        route_list = TransportRoute.get_table(self.db_connection)
        self.objects_unique[TransportRoute] = set(route_list)
        self.route_to_id = dict({route: route.id for route in route_list})

    def reload_vehicles(self):
        vehicle_list = TransportVehicle.get_table(self.db_connection)
        self.objects_unique[TransportVehicle] = set(vehicle_list)
        self.vehicle_to_id = dict({vehicle: vehicle.id for vehicle in vehicle_list})

    def get_new_objs(self, obj_list: List[BaseDBAccessDataclass]) -> List[BaseDBAccessDataclass]:
        seen_obj = set()
        seen_add = seen_obj.add
        unique_obj_list = [x for x in obj_list if not (x in seen_obj or seen_add(x))]

        return [obj for obj in unique_obj_list
                if obj not in self.objects_unique[obj.__class__]]

    def update_db(self, obj_list: List[BaseDBAccessDataclass]):
        are_in_db_list = [obj.is_in_table(self.db_connection) for obj in obj_list]

        obj_to_insert = [obj for obj, is_in in zip(obj_list, are_in_db_list)
                         if not is_in]

        for obj in obj_to_insert:
            obj.insert_in_table(self.db_connection)
            self.reload_(obj)

    @classmethod
    def decompose_response(cls, response: List[dict]) -> Union:
        operator_list, route_list = list(), list()
        vehicle_list, avl_data_list = list(), list()

        for row in response:
            operator_list.append(TransportOperator.from_response_row(row))
            route_list.append(TransportRoute.from_response_row(row))
            vehicle_list.append(TransportVehicle.from_response_row(row))
            avl_data_list.append(TransportAVLData.from_response_row(row))

        return operator_list, route_list, vehicle_list, avl_data_list

    def write_to_db(self, response):
        operator_list, route_list, vehicle_list, avl_data_list = self.decompose_response(response)

        for obj_list in [operator_list, route_list, vehicle_list]:
            for i, obj in enumerate(obj_list):
                if not (obj in self.objects_unique[obj.__class__]):
                    print(f"There is new {obj.__class__.__name__}({obj}) to inserted in DB")
                    self.update_db([obj])
                    self.reload_(obj)

                if isinstance(obj, TransportOperator):
                    vehicle_list[i].perev_id = self.operator_to_id[obj]
                elif isinstance(obj, TransportRoute):
                    route_list[i].id = self.route_to_id[obj]
                    avl_data_list[i].route_id = self.route_to_id[obj]
                elif isinstance(obj, TransportVehicle):
                    vehicle_list[i].id = self.vehicle_to_id[obj]
                    avl_data_list[i].vehicle_id = self.vehicle_to_id[obj]

        for i, (vehicle, route) in enumerate(zip(vehicle_list, route_list)):
            avl_data_list[i].vehicle_id = self.vehicle_to_id[vehicle]
            avl_data_list[i].route_id = self.route_to_id[route]

        TransportAVLData.insert_many_in_table(self.db_connection,
                                              [obj for obj in avl_data_list
                                               if obj.lat and obj.lng])
