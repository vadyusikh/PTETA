build:
	docker build --network=host -t listener:latest .

build-no-cache:
	docker build --no-cache --network=host -t listener:latest .

run_base:
	docker run \
	-it --rm \
	--network=host -p 8080:8080 \
	-e RDS_HOSTNAME=${RDS_HOSTNAME} \
	-e RDS_PTETA_DB_PASSWORD=${RDS_PTETA_DB_PASSWORD} \
	listener:latest /bin/bash

#run_mounted:
#	docker run -it --rm --network=host -v $PWD:/app -p 8080:8080 listener:latest /bin/bash
#
#run_bash:
#     run_base \
#     /bin/bash
#
#python_listener_chernivtsi:
#    python3 PTETA/listener/listener_chernivtsi.py
#
#run_listener_chernivtsi:
#    run_base python_listener_chernivtsi

#run_dev: build
#	docker run -it --rm --network=host -v ${PWD}:/main ml_in_prod:week3_latest /bin/bash
#
#test:
#	pytest -ra -s ./tests/
#
#test_data:
#	pytest -ra -s ./tests/test_data.py
#
#test_code:
#	pytest -ra -s ./tests/test_code.py
#
#test_model:
#	pytest -ra -s ./tests/test_model.py
#
#test_all:
#	pytest --cov=image_classification -ra -s tests/
