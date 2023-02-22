#FROM python:3.7-slim as cloudmapper
#FROM alpine:3.17.2
#FROM python:3.7-alpine3.17
FROM python:3.9-slim as cloudmapper

LABEL maintainer="https://github.com/0xdabbad00/"
LABEL Project="https://github.com/duo-labs/cloudmapper"

EXPOSE 8000
WORKDIR /opt/cloudmapper
ENV AWS_DEFAULT_REGION=us-east-1 

RUN apt-get update -y
RUN apt-get install -y build-essential autoconf automake libtool python3-tk jq awscli
RUN apt-get install -y bash
# RUN apk update && \
#     apk upgrade && \
#     apk add --virtual build-dependencies \
#     build-base \
#     gcc \
#     wget \
#     git 
# RUN apk add --no-cache \
#     alpine-sdk \
#     autoconf \
#     automake \
#     bash \
#     jq \
#     libtool \
#     python3-tkinter \
#     python3-dev \
#     py-pip && \
#     pip install --upgrade pip && \
#     pip install awscli

COPY . /opt/cloudmapper
RUN pip install -r requirements.txt

#RUN bash
ENTRYPOINT "./entrypoint"
