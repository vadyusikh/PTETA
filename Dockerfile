FROM python:3.11.1-slim as listener_base

LABEL mainteiner="Vadym Honcharenko"

RUN apt-get update && apt-get install make && apt-get install nano

WORKDIR /app

COPY PTETA/requirements.txt requirements.txt
RUN pip install -r requirements.txt

RUN ln -s /usr/bin/python3 /usr/bin/python

ENV PYTHONPATH /app

COPY . .

CMD [ "bash" ]


# listener_kharkiv
FROM listener_base as listener_kharkiv

CMD [ "bash" ]


# listener_chernivtsi
FROM listener_base as listener_chernivtsi

CMD [ "bash" ]
