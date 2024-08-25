FROM ubuntu:24.04

WORKDIR /workspace

RUN apt-get update -y && apt-get upgrade -y
RUN apt-get install -y python3 python3-pip

COPY ./requirements.txt /tmp/requirements.txt

RUN python3 -m pip install --no-cache-dir --break-system-packages --upgrade -r /tmp/requirements.txt

COPY ./src /workspace/src
COPY ./scripts /workspace/scripts

RUN chmod +x /workspace/scripts/*
