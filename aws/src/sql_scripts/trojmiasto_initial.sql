DROP SCHEMA IF EXISTS trojmiasto CASCADE;
DROP TABLE IF EXISTS "trojmiasto.gpsdata" CASCADE;
DROP TABLE IF EXISTS "trojmiasto.vehicle" CASCADE;
DROP TABLE IF EXISTS "trojmiasto.route" CASCADE;
DROP TABLE IF EXISTS "trojmiasto.owner" CASCADE;


CREATE SCHEMA trojmiasto;

CREATE TYPE headsign_enum AS ENUM ('');

CREATE TABLE trojmiasto.gpsdata (
  id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  lat float4 NOT NULL,
  lng float4 NOT NULL,
  speed smallint,
  direction smallint,
  delay integer, 
  gpsQuality smallint,
  tripId smallint NOT NULL,
  headsign headsign_enum NOT NULL,
  
  scheduledTripStartTime  TIMESTAMP with TIME ZONE ,
  lastUpdate              TIMESTAMP with TIME ZONE NOT NULL,
  response_datetime       TIMESTAMP with TIME ZONE NOT NULL, 
  
  vehicle_id int NOT NULL,
  route_id   int NOT NULL,
  
  UNIQUE(lat, lng, speed, direction, lastUpdate, vehicle_id, route_id)
);

CREATE INDEX trojmiasto_gpsdata_vehicle_id_index ON trojmiasto.gpsdata (vehicle_id);
CREATE INDEX trojmiasto_gpsdata_route_id_index ON trojmiasto.gpsdata (route_id);
CREATE INDEX trojmiasto_gpsdata_last_update_index ON trojmiasto.gpsdata (lastUpdate);
CREATE INDEX trojmiasto_gpsdata_response_datetime_index ON trojmiasto.gpsdata (response_datetime);

CREATE TABLE trojmiasto.vehicle(
  id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, -- vehicleId
  vehicleCode INTEGER NOT NULL,
  owner_id int NOT NULL,
  edited TIMESTAMP with TIME ZONE DEFAULT now(),
  UNIQUE(imei, name, owner_id)
);

CREATE TABLE trojmiasto.owner(
  id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  vehicleService varchar UNIQUE NOT NULL,
  edited TIMESTAMP with TIME ZONE DEFAULT now()
);

CREATE TABLE trojmiasto.route(
  id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  routeShortName varchar UNIQUE NOT NULL,
  edited TIMESTAMP with TIME ZONE DEFAULT now()
);

ALTER TABLE trojmiasto.gpsdata ADD FOREIGN KEY (vehicle_id) REFERENCES trojmiasto.vehicle (id);

ALTER TABLE trojmiasto.gpsdata ADD FOREIGN KEY (route_id) REFERENCES trojmiasto.route (id);

ALTER TABLE trojmiasto.vehicle ADD FOREIGN KEY (owner_id) REFERENCES trojmiasto.owner (id);

INSERT INTO trojmiasto.owner(id, name) VALUES (-1, 'UNKNOWN') ON CONFLICT DO NOTHING;

INSERT INTO trojmiasto.route(id, name, type) VALUES (-1, 'UNKNOWN', -1) ON CONFLICT DO NOTHING;
