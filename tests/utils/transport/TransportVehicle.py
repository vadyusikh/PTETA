from PTETA.utils.transport.TransportVehicle import TransportVehicle

# vehicle_list = TransportVehicle.get_table(conn)
# len(vehicle_list)

# [r.is_in_table(conn) for r in vehicle_list]
# TransportVehicle.are_in_table(conn, vehicle_list)

# obj.insert_in_table(conn)
# obj.is_in_table(conn)

## TEST INTERT IN TABLE
# obj = vehicle_list[0]
# obj.imei += "-"
# obj.is_in_table(conn)
# obj.insert_in_table(conn)

## TEST INTERT MANY IN TABLE
# for v in vehicle_list[:10]:
#     v.imei += '_'
# TransportVehicle.insert_many_in_table(conn, vehicle_list[:10])