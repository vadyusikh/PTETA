FROM python:3.11.1-slim as listener_base
LABEL mainteiner="Vadym Honcharenko"

RUN apt-get update && apt-get install make && apt-get install nano

WORKDIR /home/app

COPY PTETA/requirements.txt requirements.txt
RUN pip install --user -r requirements.txt

RUN ln -s /usr/bin/python3 /usr/bin/python

ENV PYTHONPATH /app

CMD [ "bash" ]


# listener_chernivtsi
FROM listener_base as listener_chernivtsi

COPY . .

CMD [ "python3", "PTETA/listener/listener_chernivtsi.py" ]


# listener_kharkiv
FROM listener_base as listener_kharkiv

COPY . .

CMD [ "python3", "PTETA/listener/listener_kharkiv_to_db.py" ]
