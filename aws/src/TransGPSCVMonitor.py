from datetime import datetime, timedelta
import pytz

import json
import requests
import time

from PTETA.utils.transport.TransportOperator import TransportOperator
from PTETA.utils.transport.TransportRoute import TransportRoute
from PTETA.utils.transport.TransportVehicle import TransportVehicle
from PTETA.utils.transport.TransportAVLData import TransportAVLData
from psycopg2.extensions import connection as Connection
import psycopg2

from apscheduler.schedulers.background import BackgroundScheduler
from typing import List, Union

from transport.BaseDBAccessDataclass import BaseDBAccessDataclass

response_prev = dict()


class TransGPSCVMonitor:
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

        self.datetime_format = '%Y-%m-%d %H:%M:%S'

        self.REQUEST_URI = \
            kwarg.get('REQUEST_URI', 'http://www.trans-gps.cv.ua/map/tracker/?selectedRoutesStr=')
        self.START_DATE = \
            kwarg.get('START_DATE', (datetime.now() - timedelta(days=1)).strftime(self.datetime_format))
        self.END_DATE = \
            kwarg.get('END_DATE', (datetime.now() + timedelta(days=30)).strftime(self.datetime_format))
        self.REQ_TIME_DELTA = kwarg.get('REQ_TIME_DELTA', 1.1)

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

    @classmethod
    def request_data(cls, request_uri='http://www.trans-gps.cv.ua/map/tracker/?selectedRoutesStr='):
        dt_now = datetime.now()
        dt_tz_now = datetime.utcnow().replace(tzinfo=pytz.utc)

        try:
            request = requests.get(request_uri)
            if (request is None) or (request.text is None):
                return
            response_cur = json.loads(request.text)

            global response_prev

            keys_prev = set(response_prev.keys())
            keys_cur = set(response_cur.keys())

            optimized_data_list = list()
            for imei in keys_prev.intersection(keys_cur):
                if response_prev[imei]['gpstime'] != response_cur[imei]['gpstime']:
                    response_cur[imei]['response_datetime'] = dt_tz_now
                    optimized_data_list += [response_cur[imei]]

            for imei in keys_cur.difference(keys_prev):
                response_cur[imei]['response_datetime'] = dt_tz_now
                optimized_data_list += [response_cur[imei]]

            response_prev = response_cur
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as err:
            print(f"{dt_now.strftime('%Y-%m-%d %H;%M;%S')} : error while trying to GET data\n"
                  f"\t{err}\n")

        #         print(dt_now.strftime('%Y-%m-%d %H;%M;%S'), len(optimized_data_list))
        return optimized_data_list

    def get_new_objs(self, obj_list: List[BaseDBAccessDataclass]) -> List[BaseDBAccessDataclass]:
        unique_obj_list = list(set(obj_list))
        return [obj for obj in unique_obj_list
                if obj not in self.objects_unique[obj.__class__]]

    def update_db(self, obj_list: List[BaseDBAccessDataclass]):
        current_class = obj_list[0].__class__
        are_in_db_list = current_class.are_in_table(self.db_connection, obj_list)

        obj_to_insert = [obj
                         for obj, is_in in zip(obj_list, are_in_db_list)
                         if not is_in]

        current_class.insert_many_in_table(self.db_connection, obj_to_insert)

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
            new_obj = self.get_new_objs(obj_list)
            if new_obj:
                print(f"There are {len(new_obj)} new {new_obj[0].__class__} to inserted in DB")
                self.update_db(new_obj)
                if isinstance(new_obj[0], TransportOperator):
                    self.reload_operators()
                elif isinstance(new_obj[0], TransportRoute):
                    self.reload_routes()
                elif isinstance(new_obj[0], TransportVehicle):
                    self.reload_vehicles()

        for i, vehicle in enumerate(vehicle_list):
            avl_data_list[i].vehicleId = self.vehicle_to_id[vehicle]

        TransportAVLData.insert_many_in_table(self.db_connection, avl_data_list)

    def run(self):
        scheduler = BackgroundScheduler(job_defaults={'max_instances': 8})
        scheduler.add_job(
            self.request_data,
            'interval',
            seconds=self.REQ_TIME_DELTA,
            end_date=self.END_DATE,
            id='listener')

        scheduler.start()

        try:
            print('Scheduler started!')
            while 1:
                time.sleep(10)
                print(datetime.now())
        except KeyboardInterrupt:
            if scheduler.state:
                scheduler.shutdown()