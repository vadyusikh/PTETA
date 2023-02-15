LISTENER_TAG=v1.1.0

build_listener_base:
	docker build --network=host -t listener_base:$(LISTENER_TAG) --target listener_base .

build_listener_kharkiv:
	docker build --network=host -t listener_kharkiv:$(LISTENER_TAG) --target listener_kharkiv .

build_listener_chernivtsi:
	docker build --network=host -t listener_chernivtsi:$(LISTENER_TAG) --target listener_chernivtsi .

build_base_no_cache:
	docker build --no-cache --network=host -t listener_base:$(LISTENER_TAG) .

run_base_foreground : build_listener
	docker run \
	-it --rm --network=host -p 8080:8080 \
	-e RDS_HOSTNAME=${RDS_HOSTNAME} \
	-e RDS_PTETA_DB_PASSWORD=${RDS_PTETA_DB_PASSWORD} \
	listener_base:$(LISTENER_TAG) /bin/bash

run_base_background : build_listener
	docker run \
	-dit --rm --network=host -p 8080:8080 \
	-e RDS_HOSTNAME=${RDS_HOSTNAME} \
	-e RDS_PTETA_DB_PASSWORD=${RDS_PTETA_DB_PASSWORD} \
	listener_base:$(LISTENER_TAG) /bin/bash

run_listener_chernivtsi_foreground : build_listener_chernivtsi
	docker run \
	-it --rm --network=host -p 8080:8080 \
	-e RDS_HOSTNAME=${RDS_HOSTNAME} \
	-e RDS_PTETA_DB_PASSWORD=${RDS_PTETA_DB_PASSWORD} \
	listener_chernivtsi:$(LISTENER_TAG)

#docker run -dit --rm --network=host -e RDS_HOSTNAME=${RDS_HOSTNAME} -e RDS_PTETA_DB_PASSWORD=${RDS_PTETA_DB_PASSWORD} listener_chernivtsi:v1.1.0
run_listener_chernivtsi_background : build_listener_chernivtsi
	docker run \
	-dit --rm --network=host -p 8080:8080 \
	-e RDS_HOSTNAME=${RDS_HOSTNAME} \
	-e RDS_PTETA_DB_PASSWORD=${RDS_PTETA_DB_PASSWORD} \
	listener_chernivtsi:$(LISTENER_TAG)

run_listener_kharkiv_foreground : build_listener_kharkiv
	docker run \
	-it --rm --network=host -p 8080:8080 \
	-e RDS_HOSTNAME=${RDS_HOSTNAME} \
	-e RDS_PTETA_DB_PASSWORD=${RDS_PTETA_DB_PASSWORD} \
	listener_kharkiv:$(LISTENER_TAG)

#docker run -dit --rm --network=host -e RDS_HOSTNAME=${RDS_HOSTNAME} -e RDS_PTETA_DB_PASSWORD=${RDS_PTETA_DB_PASSWORD} listener_kharkiv:v1.1.0
run_listener_kharkiv_background : build_listener_kharkiv
	docker run \
	-dit --rm --network=host -p 8080:8080 \
	-e RDS_HOSTNAME=${RDS_HOSTNAME} \
	-e RDS_PTETA_DB_PASSWORD=${RDS_PTETA_DB_PASSWORD} \
	listener_kharkiv:$(LISTENER_TAG)
