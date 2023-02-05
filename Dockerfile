FROM python:3.11.1-slim

LABEL mainteiner="Vadym Honcharenko"

RUN apt-get update && apt-get install make && apt-get install nano

WORKDIR /app

COPY PTETA/requirements.txt requirements.txt
RUN pip install -r requirements.txt

RUN ln -s /usr/bin/python3 /usr/bin/python

ENV PYTHONPATH /app

COPY . .

CMD [ "bash" ]