from dataclasses import dataclass
from psycopg2.extensions import connection as Connection
from psycopg2.errors import InFailedSqlTransaction

from PTETA.utils.transport.TransportVehicle import TransportVehicle

# vehicle_list = TransportVehicle.get_table(conn)
# len(vehicle_list)

# [r.is_in_table(conn) for r in vehicle_list]
# TransportVehicle.are_in_table(conn, vehicle_list)

# obj.insert_in_table(conn)
# obj.is_in_table(conn)