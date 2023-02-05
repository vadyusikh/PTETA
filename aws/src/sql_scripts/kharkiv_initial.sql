DROP SCHEMA IF EXISTS kharkiv CASCADE;
DROP TABLE IF EXISTS "vehicle" CASCADE;
DROP TABLE IF EXISTS "gpsdata" CASCADE;
DROP TABLE IF EXISTS "route" CASCADE;
DROP TABLE IF EXISTS "owner" CASCADE;

CREATE SCHEMA kharkiv;

CREATE TABLE kharkiv.gpsdata (
  id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  lat float NOT NULL,
  lng float NOT NULL,
  speed float,
  orientation float,
  gps_datetime_origin INT,
  gpstime TIMESTAMP with TIME ZONE NOT NULL,
  dd INT,
  vehicle_id int NOT NULL,
  route_id int NOT NULL,
  response_datetime TIMESTAMP with TIME ZONE,
  edited TIMESTAMP with TIME ZONE  DEFAULT now(),
  UNIQUE(lat, lng, speed, orientation, gpstime, vehicle_id, route_id)
);

CREATE INDEX kharkiv_gpsdata_vehicle_id_index ON kharkiv.gpsdata (vehicle_id);
CREATE INDEX kharkiv_gpsdata_route_id_index ON kharkiv.gpsdata (route_id);
CREATE INDEX kharkiv_gpsdata_gpstime_index ON kharkiv.gpsdata (gpstime);
CREATE INDEX kharkiv_gpsdata_response_datetime_index ON kharkiv.gpsdata (response_datetime);
CREATE INDEX kharkiv_gpsdata_edited_index ON kharkiv.gpsdata (edited);

CREATE TABLE kharkiv.vehicle(
  id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  imei varchar NOT NULL,
  name varchar NOT NULL,
  type INT,
  owner_id int NOT NULL,
  edited TIMESTAMP with TIME ZONE DEFAULT now(),
  UNIQUE(imei, name, type, owner_id)
);

CREATE TABLE kharkiv.owner (
  id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  owner_name varchar NOT NULL,
  edited TIMESTAMP with TIME ZONE DEFAULT now()
);

CREATE TABLE kharkiv.route (
  id int GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  route_name varchar NOT NULL,
  edited TIMESTAMP with TIME ZONE DEFAULT now(),
  UNIQUE(id, route_name)
);

ALTER TABLE kharkiv.gpsdata ADD FOREIGN KEY (vehicle_id) REFERENCES kharkiv.vehicle (id);

ALTER TABLE kharkiv.gpsdata ADD FOREIGN KEY (route_id) REFERENCES kharkiv.route (id);

ALTER TABLE kharkiv.vehicle ADD FOREIGN KEY (owner_id) REFERENCES kharkiv.owner (id);

INSERT INTO kharkiv.owner(id, owner_name) VALUES (-1, 'UNKNOWN') ON CONFLICT DO NOTHING;

INSERT INTO kharkiv.route(id, route_name) VALUES (-1, 'UNKNOWN') ON CONFLICT DO NOTHING;
